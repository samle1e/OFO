#%%
import pandas as pd
import streamlit as st
import plotly.express as px
import snowflake.snowpark as sp
from utils import generator

if st.secrets:
    pass
else:
    generator.create_secrets()

st.set_page_config(
    page_title="SBA Local Scorecard Dashboard",
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

#%%
#bring in the match table
@st.cache_data
def get_DOZIP ():
    session = sp.Session.builder.configs(st.secrets.snowflake_credentials).create()
    data = session.table('SBA_DO_ZIP')
    SBA_match = data.select(['ZIP_CODE','SBA_DISTRICT_OFFICE','STATE_NAME']).to_pandas().drop_duplicates("ZIP_CODE")
    SBA_match.columns = ['ZIP.Code', 'SBA.District.Office', 'State.Name']
    SBA_match['VENDOR_ADDRESS_STATE_NAME']=SBA_match["State.Name"].str.upper()
    return SBA_match
#%%
# dicts for regions and DOs
Region_dict={'Connecticut':1,'Maine':1
,'Massachusetts':1,'New Hampshire':1
,'Rhode Island':1,'Vermont':1
,'New Jersey':2,'New York':2
,'Puerto Rico':2, 'Guam':2
,'Delaware':3,'District of Columbia':3
,'Maryland':3,'Pennsylvania':3
,'Virginia':3,'West Virginia':3
,'Alabama':4,'Florida':4
,'Georgia':4,'Kentucky':4
,'Mississippi':4,'North Carolina':4
,'South Carolina':4
,'Tennessee':4,'Illinois':5
,'Indiana':5,'Michigan':5
,'Minnesota':5
,'Ohio':5,'Wisconsin':5
,'Arkansas':6,'Louisiana':6
,'New Mexico':6
,'Oklahoma':6,'Texas':6
,'Iowa':7,'Kansas':7
,'Missouri':7,'Nebraska':7
,'Colorado':8,'Montana':8
,'North Dakota':8,'South Dakota':8
,'Utah':8,'Wyoming':8
,'Arizona':9,'California':9
,'Hawaii':9,'Nevada':9
,'Northern Marianas':9
,'Alaska':10
,'Idaho':10,'Oregon':10
,'Washington':10
}
Region_dict_upper = {key.upper():Region_dict[key] for key in sorted(Region_dict.keys())}

single_DO=['Alabama'
,'Alaska'
,'Arizona'
,'Arkansas'
,'Colorado'
,'Connecticut'
,'Delaware'
#,'District of Columbia'
,'Georgia'
,'Hawaii'
,'Illinois'
,'Indiana'
,'Iowa'
,'Kentucky'
,'Louisiana'
,'Maine'
,'Massachusetts'
,'Michigan'
,'Minnesota'
,'Mississippi'
,'Montana'
,'Nebraska'
,'Nevada'
,'New Hampshire'
,'New Jersey'
,'New Mexico'
,'North Carolina'
,'North Dakota'
,'Oklahoma'
#,'Puerto Rico'
,'Rhode Island'
,'South Carolina'
,'South Dakota'
,'Tennessee'
,'Utah'
,'Vermont'
,'West Virginia'
,'Wisconsin'
,'Wyoming'
#,'American Samoa'
#,'Guam'
#,'Northern Marianas'
]
single_DO=[x.upper() for x in single_DO]
dolcols=["TOTAL_SB_ACT_ELIGIBLE_DOLLARS","SMALL_BUSINESS_DOLLARS","SDB_DOLLARS","WOSB_DOLLARS","CER_HUBZONE_SB_DOLLARS","SRDVOB_DOLLARS"]

#%%

@st.cache_data
def get_dollars():
    from snowflake.snowpark.functions import substring
    connection_parameters = st.secrets.snowflake_credentials
    global session
    session = sp.Session.builder.configs(connection_parameters).create()
    data = session.table("SMALL_BUSINESS_GOALING")

    single_DO_df = session.create_dataframe(single_DO, schema=["col1"])
    data_state = data.filter(data['VENDOR_ADDRESS_STATE_NAME'].isin(single_DO_df)).group_by(["FISCAL_YEAR",'VENDOR_ADDRESS_STATE_NAME']).sum(*dolcols).to_pandas()
    
    data_ZIP = data.filter(~data['VENDOR_ADDRESS_STATE_NAME'].isin(single_DO_df)
                           ).with_column("ZIP5", substring(data["VENDOR_ADDRESS_ZIP_CODE"],1,5)
                                         ).group_by(["FISCAL_YEAR",'VENDOR_ADDRESS_STATE_NAME','ZIP5']).sum(*dolcols).to_pandas()
    df = pd.concat([data_state, data_ZIP])    
    df.columns = df.columns.str.replace("SUM(","", regex=False).str.replace(")","", regex=False)
    return df

#%%
dollars=get_dollars()

#%%
SBA_match = get_DOZIP ()
dollarsDO=dollars.merge(SBA_match,how="left",left_on=["ZIP5","VENDOR_ADDRESS_STATE_NAME"],right_on=['ZIP.Code',"VENDOR_ADDRESS_STATE_NAME"])
#%%
#group by district office
dollarsDO.loc[dollarsDO["VENDOR_ADDRESS_STATE_NAME"].isin(single_DO),['SBA.District.Office']]=dollarsDO["VENDOR_ADDRESS_STATE_NAME"]
dollarsDO=dollarsDO.groupby(['SBA.District.Office',"FISCAL_YEAR","VENDOR_ADDRESS_STATE_NAME"],as_index=False)[dolcols].sum()
dollarsDO.loc[:,'SBA.District.Office']=dollarsDO['SBA.District.Office'].str.upper()
dollarsDO.loc[:,"SBA.Region"]=dollarsDO["VENDOR_ADDRESS_STATE_NAME"].map(Region_dict_upper)
dollarsDO.rename(columns={"VENDOR_ADDRESS_STATE_NAME":"State"},inplace=True)
#%%

#get user input on the state/DO/Region
keys = ["a","b","c"]
def select_options(var):
    select = dollarsDO[var].dropna().drop_duplicates().sort_values().to_list()
    return select

state_hide = st.sidebar.empty()
state=state_hide.selectbox(label="State",options=["No Selection"] + select_options("State"),index=1, key = keys[0])

region_hide = st.sidebar.empty()
region=region_hide.selectbox(label="SBA Region",options=["No Selection"] + ["SBA Region " + str(reg) for reg in select_options("SBA.Region")],index=0, key = keys[1])

DO_hide = st.sidebar.empty()
DO=DO_hide.selectbox(label="SBA District",options=["No Selection"] + select_options("SBA.District.Office"),index=0, key = keys[2])

if DO != "No Selection":
    var="SBA.District.Office"
    select=DO
    state_hide.empty()
    region_hide.empty()
elif region != "No Selection":
    var="SBA.Region"
    select=int(region.replace("SBA Region ",""))
    state_hide.empty()
    DO_hide.empty()
else:
    var="State"
    select=state

st.title("Local Small Business Achievements by Fiscal Year")

def reset():
    for key in keys:
        st.session_state[key] = 'No Selection'
    st.session_state["a"] = select_options("State")[0]

st.sidebar.button('Reset', on_click=reset)

#%%
select_dollars=dollarsDO[dollarsDO[var]==select].groupby('FISCAL_YEAR')[dolcols].sum()
doldict={"TOTAL_SB_ACT_ELIGIBLE_DOLLARS":"Total$","SMALL_BUSINESS_DOLLARS":"SmallBusiness$","SDB_DOLLARS":"SDB$","WOSB_DOLLARS":"WOSB$","CER_HUBZONE_SB_DOLLARS":"HUBZone$","SRDVOB_DOLLARS":"SDVOSB$"}
select_dollars=select_dollars.rename(columns=doldict)
#%%
select_pct=select_dollars.iloc[:,1:].div(select_dollars.iloc[:,0], axis=0).multiply(100).round(2)
select_pct.columns=select_pct.columns.str.replace("$","%",regex=False)

#%%
#show the graph
SP_long=select_pct.melt(ignore_index=False).rename(columns={"variable":"Category","value":"Pct"})
SP_long.index=SP_long.index.astype(str).rename("Fiscal Year")
#%%
pal = ["#002e6d", "#cc0000", "#969696", "#007dbc", "#197e4e", "#f1c400"]

fig=px.line(SP_long,x=SP_long.index,y="Pct",color='Category'
            ,    color_discrete_sequence=pal
)
st.plotly_chart(fig)
#%%
#display the table
if (var=="State"): 
    select_display = select.title() 
else: select_display=str(select)
st.write(var.replace("."," "), ":", select_display)
select_dollars.index=select_dollars.index.astype(str)
select_pct.index=select_pct.index.astype(str)

st.table(select_dollars.style.format('${:,.0f}')
	     )
st.table(select_pct.style.format('{:.2f}%')
)


#allow download of the table
if DO != "No Selection":
	filename=(DO+"_DO_achievements.csv")
elif region != "No Selection":
	filename=("Region_"+str(region)+"_achievements.csv")
else:
	filename=(state+"_achievements.csv")

st.download_button(label="Download Data"
           ,data=select_dollars.join(select_pct).to_csv()
		   ,file_name=filename
	   )

st.caption("""Source: SBA Small Business Goaling Reports, FY10-FY22. Location is based on vendor business address. This data does not apply double-credit adjustments and will not match up with the SBA small-business scorecard.\n
Abbreviations: SDB - Small Disadvantaged Business, WOSB - Women-owned small business, HUBZone - Historically Underutilized Business Zone, SDVOSB - Service-disabled veteran-owned small business.\n
Total dollars are total scorecard-eligible dollars after applying the exclusions on the [SAM.gov Small Business Goaling Report Appendix](https://sam.gov/reports/awards/standard/F65016DF4F1677AE852B4DACC7465025/view) (login required).""")

# %%
