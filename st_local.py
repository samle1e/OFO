#%%
#import polars as pl
import pandas as pd
import streamlit as st
import plotly.express as px
#import pyarrow.dataset as ds
#import os

st.set_page_config(
    page_title="SBA Local Goaling Dashboard",
    page_icon="https://www.sba.gov/brand/assets/sba/img/pages/logo/logo.svg",
    layout="wide",
    initial_sidebar_state="expanded",
)

#os.chdir("C:/Users/SQLe/U.S. Small Business Administration/Office of Policy Planning and Liaison (OPPL) - Data Lake/")
#%%
#bring in the match table
SBA_match=pd.read_csv("/SBA_DO_ZIP_matching_table.csv",  converters={ #./Mapping Files
	'ZIP.Code': str},usecols=[
	"ZIP.Code","SBA.District.Office","State.Name"]).drop_duplicates("ZIP.Code")
SBA_match['VENDOR_ADDRESS_STATE_NAME']=SBA_match["State.Name"].str.upper()
ZIP_list=SBA_match['ZIP.Code']

#%%
# SBGRdir="./SBGR_parquet"
# list=sorted([file for file in os.listdir(SBGRdir) if "SBGR" in file])
# max_year=list[-1].replace("SBGR_FY","")
# arrowds=ds.dataset(SBGRdir,format="parquet")
# plds=pl.scan_ds(arrowds)

#%%
max_year=2022 

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
    dollars=pd.read_parquet("dollars.parquet")
    # plsingle_Do=pl.Series(single_DO)
    # plziplist=pl.Series(ZIP_list)

    # plds_zip=plds.filter(
    #     (pl.col("FISCAL_YEAR")<=int(max_year))).with_columns(
    #     pl.col("VENDOR_ADDRESS_ZIP_CODE").str.slice(0,5).alias("ZIP5")
    # ).with_columns(
    #     pl.when(pl.col('VENDOR_ADDRESS_STATE_NAME').is_in(plsingle_Do))
    #     .then(pl.col('VENDOR_ADDRESS_STATE_NAME'))
    #     .when(pl.col('ZIP5').is_in(plziplist))
    #     .then(pl.col('ZIP5'))
    #     .otherwise("NA")
    #     .alias("group"))
    # dollars=plds_zip.filter((pl.col('group') != "NA") & (pl.col('VENDOR_ADDRESS_STATE_NAME') !="")).groupby(
	#     "group","FISCAL_YEAR","VENDOR_ADDRESS_STATE_NAME").agg(
	#     pl.col(dolcols).sum()).collect().to_pandas()
    return dollars

#%%
dollars=get_dollars()
#dollars.to_parquet("dollars.parquet") #COMMENT OUT

#%%
dollarsDO=dollars.merge(SBA_match,how="left",left_on=["group","VENDOR_ADDRESS_STATE_NAME"],right_on=['ZIP.Code',"VENDOR_ADDRESS_STATE_NAME"])
#%%
#group by district office
dollarsDO.loc[dollarsDO["VENDOR_ADDRESS_STATE_NAME"].isin(single_DO),['SBA.District.Office']]=dollarsDO["VENDOR_ADDRESS_STATE_NAME"]
dollarsDO=dollarsDO.groupby(['SBA.District.Office',"FISCAL_YEAR","VENDOR_ADDRESS_STATE_NAME"],as_index=False)[dolcols].sum()
dollarsDO.loc[:,'SBA.District.Office']=dollarsDO['SBA.District.Office'].str.upper()
dollarsDO.loc[:,"SBA.Region"]=dollarsDO["VENDOR_ADDRESS_STATE_NAME"].map(Region_dict_upper)
dollarsDO.rename(columns={"VENDOR_ADDRESS_STATE_NAME":"State"},inplace=True)
#%%

#get user input on the state/DO/Region
def select_options(var):
	select= pd.concat([pd.Series(["No Selection"]),dollarsDO[var].dropna().drop_duplicates().sort_values()])
	return select

st.title("Local Small Business Achievements by FY")

state=st.sidebar.selectbox(label="State",options=select_options("State"),index=1)
region=st.sidebar.selectbox(label="Region",options=select_options("SBA.Region"),index=0)
DO=st.sidebar.selectbox(label="SBA District",options=select_options("SBA.District.Office"),index=0)

#all dollars is initial display
if DO != "No Selection":
	var="SBA.District.Office";select=DO
elif region != "No Selection":
	var="SBA.Region";select=region
else:
	var="State";select=state

#for dashboard, save to parquet
#dollars.to_parquet("dollars.parquet") #COMMENT OUT

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
SP_long.index=SP_long.index.astype(str).rename("FY")
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

st.caption("Source: SBA Small Business Goaling Reports. This data does not apply double-credit adjustments and will not match up with the SBA small-business scorecard.")

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

# %%
