#%%
import pandas as pd
import streamlit as st
import plotly.express as px
import pyarrow.dataset as ds
from snowflake.connector import connect
import requests
import pyarrow as pa
import json

st.set_page_config(
    page_title="SBA Vendor Count",
    page_icon="https://www.sba.gov/brand/assets/sba/img/pages/logo/logo.svg",
    layout="wide",
    initial_sidebar_state="expanded",
)

hide_streamlit_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True) 
con = connect(**st.secrets.snowflake_credentials)
cursor = con.cursor()
#%%
@st.cache_resource
def get_data (query, params=None):
    if params:
        cursor.execute(query, params)
    else: 
        cursor.execute(query)
    results = cursor.fetch_pandas_all()
    return results

#%%
#Get needed tables

@st.cache_data
def naics_list ():
    '''returns Pandas Series of NAICS names'''
    links = ['https://www.census.gov/naics/reference_files_tools/2007/naics07.xls',
             'https://www.census.gov/naics/2012NAICS/2-digit_2012_Codes.xls',
             'https://www.census.gov/naics/2017NAICS/2-6%20digit_2017_Codes.xlsx']
    
    NAICS_sr = (pd.concat([(pd.read_excel(link)
               .drop('Seq. No.', axis = 1)
               .iloc[:,[0,1]]
               .set_axis(['Code','Title'], axis=1)
               .pipe(lambda _df: _df.assign(Code = _df.Code.astype(str)))
               .set_index('Code')
                )
                  for link in links], axis=1)
                 .pipe(lambda _df: _df.assign(industry = _df.apply(
                    lambda row: next((val for val in row if pd.notnull(val)), pd.NA), axis=1)))
                 .pipe(lambda _df:_df.drop(_df.columns[0:1],axis=1))
                 .dropna(axis=0)
                 .squeeze()
                )
    return NAICS_sr

@st.cache_data
def get_PSC_names(): 
    '''returns Pandas Series of PSC names'''
    PSClink = "https://www.acquisition.gov/sites/default/files/manual/PSC%20April%202022.xlsx"
    PSCnames=(pd.read_excel(PSClink)
            .drop_duplicates("PSC CODE",keep="first")
            .assign(PSC_CODE=lambda _df: _df["PSC CODE"].astype(str))
            .drop('PSC CODE', axis=1)
            .set_index("PSC_CODE")
            .filter(regex="NAME")
            .pipe(lambda _df:_df.assign(Title = _df.iloc[:,1].combine_first(_df.iloc[:,0])))
            .sort_index()
            .loc[:,"Title"]
            .squeeze())
    return PSCnames

@st.cache_data
def dept_agency_choices():
    dept_agency = get_data("SELECT DISTINCT FUNDING_DEPARTMENT_NAME, FUNDING_AGENCY_NAME FROM SMALL_BUSINESS_GOALING")
    dict = {key: list(group['FUNDING_AGENCY_NAME']) for key, group in dept_agency.group_by('FUNDING_DEPARTMENT_NAME')}
    return dict

@st.cache_data
def state_county_CD_zip():
    token = st.secrets.HUD.HUDkey

    #Get Counties from HUD
    state_county_zip = get_data("SELECT ZIP_CODE, FIPS, STATE_FIPS_CODE, STATE_NAME, STATE, COUNTY FROM SBA_DO_ZIP")

    #Get Congressional Districts from HUD
    url = "https://www.huduser.gov/hudapi/public/usps?type=5&query=all"
    headers = {"Authorization": "Bearer {0}".format(token)}
    response = requests.get(url, headers = headers)
    
    zip_data = (pd.DataFrame(response.json()["data"]["results"])	
            .sort_values('bus_ratio',ascending=False)
            .drop_duplicates(subset="zip",keep="first")
            .assign(CD=lambda _df:_df.geoid.astype(str).str.slice(2,4))
            .set_index('zip')
            .join(state_county_zip.drop_duplicates().set_index('ZIP_CODE')
                ,how="outer")
            .fillna(method='ffill')
            .filter(['CD', 'FIPS' ,'STATE_FIPS_CODE', 'STATE_NAME', 'STATE', 'COUNTY'])
        )
    return zip_data

@st.cache_data
def years_choices():
    years = get_data("SELECT DISTINCT FISCAL_YEAR FROM SMALL_BUSINESS_GOALING")
    years = years.squeeze().astype(int).sort_values().tolist()
    return years

#%%
def reset_session_state ():
    for x in st.session_state:
        del st.session_state[x]
    st.experimental_rerun()

#%%
def get_year():
    years = years_choices()
    if 'year' not in st.session_state:
        st.session_state.year=years[-1]

    year = st.sidebar.selectbox(label='Fiscal Year', options = years,
                                key='year')
    return {'FISCAL_YEAR': year}

def department():
    da_choices = dept_agency_choices()
    choices = sorted(list(da_choices.keys()))
    
    if 'dept' not in st.session_state:
        st.session_state.dept=[]
    if 'agency_name' not in st.session_state:
        st.session_state.agency_name=[]

    funding_department_name = st.sidebar.multiselect(label="Department"
                                        , options=choices, key = 'dept')
    
    agency_choices = []
    if len(funding_department_name) == 1:
        agency_choices = sorted(da_choices[funding_department_name[0]])
    agency_name = st.sidebar.multiselect(label="Agency", options = agency_choices, key = 'agency_name'
                                         , disabled = (len(funding_department_name) != 1))   
    return {'FUNDING_DEPARTMENT_NAME': funding_department_name, 'FUNDING_AGENCY_NAME': agency_name}

def state_zip ():
    #set session states
    if 'state' not in st.session_state:
        st.session_state.state=[]
    if 'counties' not in st.session_state:
        st.session_state.counties=[]
    if 'CDs' not in st.session_state:
        st.session_state.CDs=[]

    #state selection
    state_df = state_county_CD_zip()
    state = st.sidebar.multiselect(label='State (pick multi)'
                                   , options = state_df.STATE_NAME.drop_duplicates().sort_values().to_list(),
                                   key = 'state')
 
    #county and CD selection
    counties = []
    CDs = []

    if len(state)==1:
        county_choice = state_df[state_df['STATE_NAME']==state[0]].COUNTY.drop_duplicates().sort_values().to_list()
        CD_choice = [x for x in       
                        state_df[state_df['STATE_NAME']==state[0]].CD.drop_duplicates().sort_values().to_list()
                    if '00' not in x]
    else:
        county_choice = []
        CD_choice = []

    counties = st.sidebar.multiselect(label='Counties (pick multi)',
                                   options = county_choice,
                                   key = 'counties',
                                   disabled = ((len(state) != 1) | (len(CDs)>0))
                                   )   

    CDs = st.sidebar.multiselect(label='Congressional Districts (pick multi)'
                                   , options = CD_choice,
                                   key = 'CDs',
                                   disabled = ((len(state) != 1) | (len(counties)>0))
                                   )  

    #convert to ZIP codes
    if ((len(CDs) > 0) | (len(counties) > 0)):
        if (len(CDs) > 0):
            zip = state_df[(state_df['STATE_NAME']==state[0]) & (state_df['CD'].isin(CDs))].index.to_list()
        else:
            zip = state_df[(state_df['STATE_NAME']==state[0]) & (state_df['COUNTY'].isin(counties))].index.to_list()
    else:
        zip = []

    return {'VENDOR_ADDRESS_STATE_NAME': [x.upper() for x in state], 'VENDOR_ADDRESS_ZIP_CODE': zip}

def get_NAICS ():
    naicslst = naics_list ()
    options = [f"{index}: {value}" for index, value in naicslst.items()]
    if 'naics' not in st.session_state:
        st.session_state.naics=[]
    NAICS_pick = st.sidebar.multiselect (label="NAICS (pick multi)", options = options
                                         , key = 'naics')
    
    NAICS_pick_long = [m 
        for l in NAICS_pick 
        for m in (list(range(
            int(l.split(':')[0].split('-')[0]), 
            int(l.split(':')[0].split('-')[1])+1)) 
        if '-' in l 
        else [int(l.split(':')[0])])
        ]

    NAICS6_pick = [int(num) for num in naicslst.index.to_list() 
                   if len(num) == 6 and 
                   any(num.startswith(str(prefix)) for prefix in NAICS_pick_long)]
    return {'PRINCIPAL_NAICS_CODE': NAICS6_pick}
    
def get_PSC ():
    PSC_list = get_PSC_names()
    options = [f"{index}: {value}" for index, value in PSC_list.items()]
    if 'psc' not in st.session_state:
        st.session_state.psc=[]
    psc_pick = st.sidebar.multiselect (label="PSCs (pick multi)", options = options, key = 'psc')
    psc_pick = [x.split(':')[0] for x in psc_pick]

    psc_pick_long = [pick for pick in PSC_list.index.to_list() 
                   if len(pick) == 4 and 
                   any(pick.startswith(prefix) for prefix in psc_pick)]
    return {'PRODUCT_OR_SERVICE_CODE': psc_pick_long}    
#%%
def dollars_table (**kwargs):
    '''Returns a pyarrow table with consolidated and filtered results from a single FY'''
    filter = []

    if ('VENDOR_ADDRESS_ZIP_CODE' in kwargs):
        filter.append (f"SUBSTRING(VENDOR_ADDRESS_ZIP_CODE, 1, 5) in (%(VENDOR_ADDRESS_ZIP_CODE)s))")
    
    for x in kwargs:
        if x not in ['VENDOR_ADDRESS_ZIP_CODE', 'FISCAL_YEAR']:
            filter.append(f"{x} in (%({x})s)")

    filter_all = ' AND '.join(filter)

    if filter_all == '':
        filter_all = '1=1'
   
    groupcols = ["FUNDING_DEPARTMENT_NAME",
            "FUNDING_AGENCY_NAME",
            "FUNDING_OFFICE_NAME",
            'PRODUCT_OR_SERVICE_CODE',
            'PRODUCT_OR_SERVICE_DESCRIPTION',
            'PRINCIPAL_NAICS_CODE',
            'PRINCIPAL_NAICS_DESCRIPTION',
        ]
    
    dolcols=["TOTAL_SB_ACT_ELIGIBLE_DOLLARS",
            "SMALL_BUSINESS_DOLLARS",
            "SDB_DOLLARS",
            "WOSB_DOLLARS",
            "CER_HUBZONE_SB_DOLLARS",
            "SRDVOB_DOLLARS",
            "EIGHT_A_PROCEDURE_DOLLARS"]

    dollars = cursor.execute(f'''
            SELECT 
                    {", ".join(groupcols)},
                    SUBSTRING(VENDOR_ADDRESS_ZIP_CODE, 1, 5) VENDOR_ADDRESS_ZIP_CODE,
                    COALESCE (VENDOR_UEI, VENDOR_DUNS_NUMBER) UEI_OR_DUNS,
                    COALESCE (UEI_NAME, VENDOR_NAME) VENDOR_NAME,
                    {", ".join([f'sum({col}) {col}' for col in dolcols])}
                FROM SMALL_BUSINESS_GOALING
                WHERE FISCAL_YEAR = (%(FISCAL_YEAR)s) AND TOTAL_SB_ACT_ELIGIBLE_DOLLARS > 0 AND {filter_all}
                GROUP BY SUBSTRING(VENDOR_ADDRESS_ZIP_CODE, 1, 5),
                    COALESCE (VENDOR_UEI, VENDOR_DUNS_NUMBER),
                    COALESCE (UEI_NAME, VENDOR_NAME),
                    {", ".join(groupcols)}
            ''', kwargs).fetch_arrow_all()
    
    return dollars

#%%
def dollars_display(dollars_tb):
    '''Displays the dollars table, sorted by metrics provided by the user. Allows for download.'''
    dollars_dict={
        "TOTAL_SB_ACT_ELIGIBLE_DOLLARS":"Total Dollars",
        "SMALL_BUSINESS_DOLLARS":"Small Business Dollars",
        "SDB_DOLLARS":"SDB Dollars",
        "WOSB_DOLLARS":"WOSB Dollars",
        "CER_HUBZONE_SB_DOLLARS":"HUBZone Dollars",
        "SRDVOB_DOLLARS":"SDVOSB Dollars",
        "EIGHT_A_PROCEDURE_DOLLARS":"8(a) Dollars",
    }
    dollars_dict_rev = {v:k for k,v in dollars_dict.items()}

    metric=st.radio("Select metric to graph",options=list(dollars_dict.values()),index=1)
    st.caption ("8(a) dollars are dollars spent on 8(a) contracts and are a subset of SDB Dollars. SDB Dollars includes 8(a) dollars and other awards to SDBs.")

    #Funding Offices
    st.header("Top Funding Offices")
    gb_cols = ['FUNDING_OFFICE_NAME', 'FUNDING_AGENCY_NAME', 'FUNDING_DEPARTMENT_NAME']
    top_offices = ((dollars_tb
                   .group_by(gb_cols)
                   .aggregate([(x, 'sum') for x in list(dollars_dict.keys())])
                   .sort_by([(f'{dollars_dict_rev[metric]}_sum', 'descending')])
                   .slice(length=100)
                  ).to_pandas()
                  .rename({f'{k}_sum':v for k,v in dollars_dict.items()}, axis=1)
                  .loc[:,gb_cols + list(dollars_dict.values())]
                  .pipe(lambda _df:_df.assign(FUNDING_OFFICE_NAME = _df.FUNDING_OFFICE_NAME.fillna(_df.FUNDING_AGENCY_NAME)))
                  .rename({x:x.replace('_',' ').title() for x in gb_cols}, axis=1)
                  .round(0)
                  )

    fig_offices = px.bar(top_offices.sort_values(metric, ascending=False).head(10).sort_values(metric)
         , x=metric, y="Funding Office Name", orientation='h')
    st.plotly_chart(fig_offices)
    st.dataframe(top_offices,use_container_width=True, hide_index=True)
    st.download_button('Download this table',top_offices.to_csv(), 'top_offices.csv')

    #Top Vendors
    st.header("Top Vendors")
    top_vendors = ((dollars_tb
                    .group_by(['UEI_OR_DUNS', 'VENDOR_NAME'])
                    .aggregate([(x, 'sum') for x in list(dollars_dict.keys())])
                    .sort_by([(f'{dollars_dict_rev[metric]}_sum', 'descending')])
                    ).to_pandas()
                    .rename({f'{k}_sum':v for k,v in dollars_dict.items()}, axis=1)
                    .group_by('UEI_OR_DUNS', sort=True)
                    .agg({**{'VENDOR_NAME': 'first'},**{x:'sum' for x in list(dollars_dict.values())}})
                    .sort_values(metric, ascending=False)
                    .head(100)
                    .reset_index()
                    .round(0)
    )
    
    fig_vendors = px.bar(top_vendors.sort_values(metric, ascending=False).head(10).sort_values(metric)
         , x=metric, y=top_vendors.columns[1], orientation='h',labels={top_vendors.columns[1]:"Vendor Name"})
    st.plotly_chart(fig_vendors)
    st.dataframe(top_vendors,use_container_width=True, hide_index=True)
    st.download_button('Download this table',top_vendors.to_csv(), 'top_vendors.csv')

    
    #Top NAICS and PSCs
    for x in ('PRINCIPAL_NAICS_CODE', 'PRODUCT_OR_SERVICE_CODE'):
        st.header (f'Top Industries by {x.replace("_"," ").title().replace("Naics","NAICS")}')

        top_industries = ((dollars_tb
                        .group_by([x, x.replace('CODE', 'DESCRIPTION')])
                        .aggregate([(x, 'sum') for x in list(dollars_dict.keys())])
                        .sort_by([(f'{dollars_dict_rev[metric]}_sum', 'descending')])
                        .slice(length=100)
                        ).to_pandas()
                        .rename({f'{k}_sum':v for k,v in dollars_dict.items()}, axis=1)
                        .loc[:,[x, x.replace('CODE', 'DESCRIPTION')] + list(dollars_dict.values())]
                        .rename({y:y.replace('_',' ').title() for y in gb_cols}, axis=1)
                        .round(0)
                        )

        fig_industries = px.bar(top_industries.head(10).sort_values(metric)
            , x=metric, y=x.replace('CODE', 'DESCRIPTION'), orientation='h',
            labels={x.replace('CODE', 'DESCRIPTION'):"Industry"},
           )
        st.plotly_chart(fig_industries)
        st.dataframe(top_industries,use_container_width=True, hide_index=True)
        st.download_button('Download this table',top_industries.to_csv(), f'top_{x}.csv')

    # Show_county_graph
    ZIP_FIPS = state_county_CD_zip().FIPS.to_dict()

    fips_totals=((dollars_tb
                        .group_by('VENDOR_ADDRESS_ZIP_CODE')
                        .aggregate([(dollars_dict_rev[metric], 'sum')])
                        ).to_pandas()
                        .pipe(lambda _df:_df.assign(FIPS = _df.VENDOR_ADDRESS_ZIP_CODE.map(ZIP_FIPS)))
                        .rename({f'{dollars_dict_rev[metric]}_sum':metric}, axis=1)
                        .group_by(["FIPS"],as_index=False)[metric].sum()
                        )
    
    #get county names
    ZIP_county=state_county_CD_zip().filter(['FIPS','COUNTY']).drop_duplicates().set_index('FIPS').squeeze().to_dict()
    fips_totals["County"] = fips_totals["FIPS"].map(ZIP_county)

    @st.cache_data
    def get_counties_fips():
        from urllib.request import urlopen
        counties=json.load(urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json'))
        return counties

    counties=get_counties_fips()
    midpoint=fips_totals[fips_totals[metric]>0][metric].quantile(0.99)

    fig=px.choropleth_mapbox(fips_totals[fips_totals[metric]>0],geojson=counties
                             ,locations='FIPS',color=metric,
                             color_continuous_scale='Portland',
                             #hover_data={'FIPS':'County'},
                             hover_name='County',
                             #color_continuous_midpoint=midpoint,
                             range_color=(0,midpoint),
                             mapbox_style="carto-positron",
                             center={"lat":37.0902,"lon":-95.7129},
                             zoom=3
    )
    st.plotly_chart(fig,use_container_width = True)


#%%
if __name__ == "__main__":
    st.title("Top Offices and Vendors")
    st.caption('This report shows the top offices, vendors, and PSC industries based on filter selections to the left and the metric selected below. A county-by-county map of spending by vendor location appears at the bottom.')

    d={} #filter dictionary
    d.update(get_year())
    d.update(department())
    d.update(state_zip())
    d.update(get_NAICS())
    d.update(get_PSC())

    #prepare the filter dictionary for processing
    for x in d.copy():
        if d[x] == 'All' or d[x] == []:
            del d[x]

    #get the dollars tables based on filters
    dollars_tb = dollars_table(**d)

    dollars_display (dollars_tb)