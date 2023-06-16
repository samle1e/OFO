#%%
import pandas as pd
import duckdb
import streamlit as st
from snowflake.connector import connect
import plotly.express as px
import pyarrow as pa
import re
import plotly.graph_objects as go
from itertools import chain
import pyarrow.compute as pc

# %%
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

def collist():
    x = dict(cols = [ 'COALESCE (VENDOR_DUNS_NUMBER, VENDOR_UEI) VENDOR_ID',
            'ADDRESS_STATE',
            'TRY_TO_NUMBER(SUBSTRING(VENDOR_ADDRESS_ZIP_CODE, 1, 5)) VENDOR_ADDRESS_ZIP_CODE',
            'TRY_TO_NUMBER(PRINCIPAL_NAICS_CODE) PRINCIPAL_NAICS_CODE',
            'FUNDING_AGENCY_NAME',
            'FUNDING_DEPARTMENT_NAME',
            'PRODUCT_OR_SERVICE_CODE',
            'TYPE_OF_SET_ASIDE',
            'IDV_TYPE_OF_SET_ASIDE',
            'EVALUATED_PREFERENCE',
            'TRY_TO_NUMBER(FISCAL_YEAR) FY'],
    dolcols = ["SMALL_BUSINESS_DOLLARS",
            "SDB_DOLLARS",
            "WOSB_DOLLARS",
            "CER_HUBZONE_SB_DOLLARS",
            "SRDVOB_DOLLARS",
            'EIGHT_A_PROCEDURE_DOLLARS',
            'VOSB_DOLLARS'],
    flagcols = ['MINORITY_OWNED_BUSINESS_FLAG', #6
            'APAOB_FLAG', #4
            'BAOB_FLAG', #1
            'HAOB_FLAG', #2
            'NAOB_FLAG', #3
            'SAAOB_FLAG',
            'OTHER_MINORITY_OWNED', #5
            'ALASKAN_NATIVE_CORPORATION',
            'NATIVE_HAWAIIAN_ORGANIZATION',],
    tribal = "TO_BOOLEAN(CASE WHEN INDIAN_TRIBE = \'YES\' OR TRIBALLY_OWNED = \'YES\' OR AIOB_FLAG = \'YES\' THEN 1 ELSE 0 END) TRIBAL"
    )
    return x

def shrink_table (tb):
    #convert large numeric types to smaller types
    type_dict = {'VENDOR_ADDRESS_ZIP_CODE':pa.uint32(),
            'PRINCIPAL_NAICS_CODE':pa.uint32(),
            'FY':pa.uint16()}
    newsch = tb.schema
    for x in type_dict:
            newsch = newsch.set(newsch.get_field_index(x),
            pa.field(x, type_dict[x]))
    tb = tb.cast(newsch)

    #convert columns with common entries to dictionaries
    from pyarrow.compute import dictionary_encode

    dict_cols = ['FUNDING_AGENCY_NAME',
            'FUNDING_DEPARTMENT_NAME',
            'PRODUCT_OR_SERVICE_CODE',
            'TYPE_OF_SET_ASIDE',
            'IDV_TYPE_OF_SET_ASIDE']
    for x in dict_cols:
            i = tb.schema.get_field_index(x)
            if tb.schema.types[i] != pa.dictionary(pa.int32(), pa.utf8()):
                    tb = tb.set_column(i,x,dictionary_encode(tb[x]))

    return tb

# @st.cache_data
# def get_vendor_data ():
#     cols = collist()
#     boolflagcols = [f"TO_BOOLEAN(CASE WHEN {x} = 'YES' THEN 1 ELSE 0 END) {x}" for x in cols['flagcols']] 
#     booldolcols = [f'TO_BOOLEAN(CASE WHEN {x} > 0 THEN 1 ELSE 0 END) {x}' for x in cols['dolcols']]
#     q = f'''
#             SELECT DISTINCT {', '.join(cols['cols'])}, {', '.join(booldolcols)}, {', '.join(boolflagcols)}, {cols['tribal']}
#                     FROM SMALL_BUSINESS_GOALING
#                     WHERE TOTAL_SB_ACT_ELIGIBLE_DOLLARS > 0
#             '''
#     tb = get_data (q)
#     tb = shrink_table(tb)
#     return tb
#%%
# get other tables needed

@st.cache_data
def get_DO_table ():
    '''returns Pyarrow Table of district offices, zip codes, fips codes'''
    q = 'SELECT * FROM SBA_DO_ZIP'
    SBA_DO = get_data (q)
    return SBA_DO

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



#%%
def reset_session_state ():
    for x in st.session_state:
        del st.session_state[x]
    st.experimental_rerun()
    

#%%

def address_state_vendor_address_zip_code ():
    DO_table = get_DO_table ()
    single_DO = (DO_table
                .loc[:,['SBA_DISTRICT_OFFICE','STATE']]
                .drop_duplicates()
                .set_index('STATE')
                .loc[lambda _df:_df.value_counts('STATE')
                      .loc[lambda s:s==1]
                      .index
                      .to_list()]
                .squeeze()
    )
    
    state_choices = sorted(DO_table['STATE_NAME'].unique().tolist())
    region_choices = [f'SBA Region {x}' for x in list(range(1,11))]
    DO_choices = sorted(DO_table['SBA_DISTRICT_OFFICE'].unique().tolist())

    if 'region' not in st.session_state:
        st.session_state.region='All'
    
    if 'state' not in st.session_state:
        st.session_state.state=[]
    
    if 'do' not in st.session_state:
        st.session_state.do='All'

    state_select = st.sidebar.multiselect (label="State (pick multi)", options=state_choices,
                                           key = 'state', args='state', disabled = ((st.session_state.region != 'All') | (st.session_state.do != 'All')))
    region_select = st.sidebar.selectbox (label="SBA Region", options=['All']+region_choices,
                                          key = 'region', args='region', disabled = ((len(st.session_state.state) > 0) | (st.session_state.do != 'All')))
    DO_select = st.sidebar.selectbox (label="SBA District Office", options=['All']+DO_choices,
                                          key = 'do', args='do', disabled = ((st.session_state.region != 'All') | (len(st.session_state.state) > 0)))
    
    state_abbr = DO_table.loc[:,['STATE_NAME','STATE']].drop_duplicates().set_index('STATE_NAME').squeeze().to_dict()
    
    address_state = []
    vendor_address_zip_code = []
    if len(state_select)>0:
        address_state = [state_abbr[x] for x in state_select]
        vendor_address_zip_code = []
    elif region_select != 'All':
        address_state = DO_table.loc[DO_table.SBA_REGION==region_select.split(' ')[-1],'STATE'].unique().tolist()
        vendor_address_zip_code = []
    elif DO_select != 'All':
        if DO_select in single_DO.to_list():
            address_state = single_DO.loc[single_DO==DO_select].index.to_list()
            vendor_address_zip_code = DO_table.loc[~DO_table.STATE.isin(address_state) & DO_table.SBA_DISTRICT_OFFICE==DO_select, 'ZIP_CODE'].tolist()
        else:
            vendor_address_zip_code = DO_table.loc[DO_table.SBA_DISTRICT_OFFICE==DO_select, 'ZIP_CODE'].tolist()
    return {'ADDRESS_STATE':address_state, 'VENDOR_ADDRESS_ZIP_CODE':vendor_address_zip_code}

@st.cache_data
def dept_agency_choices():
    dept_agency = get_data("SELECT DISTINCT FUNDING_DEPARTMENT_NAME, FUNDING_AGENCY_NAME FROM SMALL_BUSINESS_GOALING")
    dict = {key: list(group['FUNDING_AGENCY_NAME']) for key, group in dept_agency.groupby('FUNDING_DEPARTMENT_NAME')}
    return dict

def funding_department_name_agency_name ():
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

def get_set_aside ():
    set_aside_dict={'Small Business Set-Aside': 'ESB',
        'Partial SB Set-Aside': 'SBP',
        '8(a) Competitive': '8A',
        '8(a) Sole Source': '8AN',
        'WOSB Set-Aside': 'WOSB',
        'WOSB Sole Source': 'WOSBSS',
        'EDWOSB Set-Aside': 'EDWOSB',
        'EDWOSB Sole Source': 'EDWOSBSS',
        'SDVOSB Set-Aside': 'SDVOSBC',
        'SDVOSB Sole Source': 'SDVOSBS',
        'HUBZone Set-Aside': 'HZC',
        'HUBZone Sole Source': 'HZS',
        'HUBZone Price Evaluation Preference': 'HZE'}

    options = set_aside_dict.keys()
    if 'set_aside' not in  st.session_state:
        st.session_state.set_aside=[]
    set_aside_pick = st.sidebar.multiselect (label="Set Asides (pick multi)", options = options, key = 'set_aside')
    set_aside_pick = [set_aside_dict[pick] for pick in set_aside_pick]
    return {'SET_ASIDE': set_aside_pick}

def format_table (df):
    df.index = df.index.map(str)
    df.columns = (df.columns
        .str.replace("_DOLLARS","_VENDORS")
        .str.replace("B_FLAG|_OWNED_BUSINESS|_OWNED$","_SB_VENDORS", regex=True)
        .str.replace("PROCEDURE_","")
        .str.replace('ALASKAN_NATIVE_CORPORATION','ALASKAN_NATIVE_CORPORATION_SB_VENDORS')
        .str.replace('NATIVE_HAWAIIAN_ORGANIZATION','NATIVE_HAWAIIAN_ORGANIZATION_SB_VENDORS')
        .str.replace('TRIBAL','TRIBAL_SB_VENDORS')
        )
    df = df.set_axis(df.columns.str.replace('_',' '), axis=1)
    return df

def counts_table (all_ct=True, **kwargs): 
    #kwargs is a series of keyword:list pairs that can be used to filter the table
    filter = []
    if ('ADDRESS_STATE' in kwargs) and ('VENDOR_ADDRESS_ZIP_CODE' in kwargs):
        filter.append (f"(ADDRESS_STATE in (%(ADDRESS_STATE)s) OR SUBSTRING(VENDOR_ADDRESS_ZIP_CODE, 1, 5) in (%(VENDOR_ADDRESS_ZIP_CODE)s))")
    elif 'VENDOR_ADDRESS_ZIP_CODE' in kwargs:
        filter.append (f"SUBSTRING(VENDOR_ADDRESS_ZIP_CODE, 1, 5) in (%(VENDOR_ADDRESS_ZIP_CODE)s))")
    elif 'ADDRESS_STATE' in kwargs:
        filter.append (f"ADDRESS_STATE in (%(ADDRESS_STATE)s)")

    if 'SET_ASIDE' in kwargs:
        filter.append (f"(TYPE_OF_SET_ASIDE IN (%(SET_ASIDE)s) OR \
                       IDV_TYPE_OF_SET_ASIDE IN (%(SET_ASIDE)s) OR EVALUATED_PREFERENCE IN (%(SET_ASIDE)s))")

    for x in kwargs:
        if x not in ['ADDRESS_STATE', 'VENDOR_ADDRESS_ZIP_CODE', 'SET_ASIDE']:
            filter.append(f"{x} in (%({x})s)")

    filter_all = ' AND '.join(filter)

    if filter_all == '':
        filter_all = '1=1'
   
    cols = collist()

    sb_counts = get_data(f'''
            SELECT FISCAL_YEAR,
                {", ".join([f"COUNT(DISTINCT CASE WHEN {x} > 0 THEN COALESCE (VENDOR_DUNS_NUMBER, VENDOR_UEI) END) AS {x}" for x in cols["dolcols"]])},
                {", ".join([f"COUNT(DISTINCT CASE WHEN {x} = 'YES' THEN COALESCE (VENDOR_DUNS_NUMBER, VENDOR_UEI) END) AS {x}" for x in cols["flagcols"]])},
                COUNT(DISTINCT CASE WHEN INDIAN_TRIBE = 'YES' OR TRIBALLY_OWNED = 'YES' OR AIOB_FLAG = 'YES' THEN COALESCE (VENDOR_DUNS_NUMBER, VENDOR_UEI) END) TRIBAL
                FROM SMALL_BUSINESS_GOALING
                WHERE SMALL_BUSINESS_DOLLARS > 0 AND {filter_all}
                GROUP BY FISCAL_YEAR
                ORDER BY FISCAL_YEAR
            ''', kwargs).set_index('FISCAL_YEAR')
    if all_ct: 
        all_df = get_data(f'''
                SELECT FISCAL_YEAR, 
                COUNT (DISTINCT COALESCE (VENDOR_DUNS_NUMBER, VENDOR_UEI)) AS TOTAL_VENDORS
                    FROM SMALL_BUSINESS_GOALING
                    WHERE TOTAL_SB_ACT_ELIGIBLE_DOLLARS > 0 AND {filter_all}
                    GROUP BY FISCAL_YEAR
                    ORDER BY FISCAL_YEAR
                ''', kwargs).set_index('FISCAL_YEAR')
        counts =  all_df.join(sb_counts)
    else:
        counts = sb_counts
    counts = format_table(counts)
    return counts



if __name__ == '__main__':
    st.title("SBA Vendor Counts")
    st.caption('This report shows the number of vendors that received a positive obligation in the Small Business Goals Report, after applying scorecard exclusions. Except for Total Vendors, only small businesses are counted.')
    d={}
    d.update(address_state_vendor_address_zip_code())
    d.update(funding_department_name_agency_name())
    d.update(get_PSC())
    d.update(get_NAICS())
    d.update(get_set_aside())
    for x in d.copy():
        if d[x] == 'All' or d[x] == []:
            del d[x]
    st.table(counts_table(True,**d))
    if st.sidebar.button("Reset"):
        reset_session_state()

    st.caption('''Source: SBA Small Business Goaling Reports, FY09-FY22. A vendor is a unique DUNS or UEI that received a positive obligation in the fiscal year.\n
    Abbreviations: SDB - Small Disadvantaged Business, WOSB - Women-owned small business, HUBZone - Historically Underutilized Business Zone, SDVOSB - Service-disabled veteran-owned small business.\n
    This report consider transactions on the Small Business Goaling Report, after applying scorecard exclusions. Except for "All Vendors," the report considers only vendors that received a positive obligation in the given scorecard category (e.g., HUBZone vendors consider only vendors that received positive obligations in the HUBZone scorecard category).''')
    cursor.close()
    
#%%

# map_select=st.sidebar.selectbox(label="Map what type of vendor?",options=newdolcols,index=1)
# year=st.sidebar.slider(label="Map which Fiscal Year?",min_value=min_year, max_value=max_year,value=max_year)
# vendor_map=vendors.filter(pl.col("FY")==year)

# #%%
# def get_count_map(vendors,col,var):
#     for x in newdolcols:
#         vendors=vendors.with_columns(
#         pl.when(pl.col(x)==True)
#         .then(pl.col("VENDOR_ID"))
#         .otherwise(pl.lit("@"))
#         .alias(x)
#     )
#     counts=vendors.select([col]+[var]).groupby(var).n_unique()
#     counts_adj=counts.select([pl.col(var),pl.col(col).map(lambda x:x-1)]).collect().to_pandas().set_index(var)
#     return counts_adj
# #%%
# #prepare table and plots

# vendor_table=get_counts(vendors,"FY")
# pal = ["#002e6d", "#cc0000", "#969696", "#007dbc", "#197e4e", "#f1c400"]
# fig=px.line(vendor_table,x=vendor_table.index,y=vendor_table.columns
#             ,    color_discrete_sequence=pal,labels={"index":"FY","value":"vendors","variable":""}
# )
# # st.write(map_select)
# # st.write(year)
# map_table= get_count_map(vendor_map,map_select,"state_abbr").iloc[:,0]

# fig2 = go.Figure(data=go.Choropleth(
#     locations=map_table.index, # Spatial coordinates
#     z = map_table.array, # Data to be color-coded
#     locationmode = 'USA-states', # set of locations match entries in `locations`
#     colorscale = 'Portland',
#     colorbar_title = "Vendors",
# ))

# fig2.update_layout(
#     geo_scope='usa',
# )
# #%%    
# if fig:
#     st.plotly_chart(fig)

# st.table(vendor_table)

# if fig2:
#     st.plotly_chart(fig2)
