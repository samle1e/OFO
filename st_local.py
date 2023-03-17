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
#define my datasets
SBA_match=pd.read_csv("SBA_DO_ZIP_matching_table.csv",  converters={'ZIP.Code': str}).drop_duplicates("ZIP.Code")
# SBA_match_pl=pl.from_pandas(SBA_match).lazy()
# SBGRdir="./SBGR_parquet"
# list=sorted([file for file in os.listdir(SBGRdir) if "SBGR" in file])
# max_year=list[-1].replace("SBGR_FY","")
max_year=2022 
# arrowds=ds.dataset(SBGRdir,format="parquet")
# plds=pl.scan_ds(arrowds)

#%%
dolcols=["TOTAL_SB_ACT_ELIGIBLE_DOLLARS","SMALL_BUSINESS_DOLLARS","SDB_DOLLARS","WOSB_DOLLARS","CER_HUBZONE_SB_DOLLARS","SRDVOB_DOLLARS"]

@st.cache_data
def get_dollars():
    dollars=pd.read_parquet("dollars.parquet")
    # plds_zip=plds.filter(
    #     pl.col("FISCAL_YEAR")<=int(max_year)).with_columns(
	#     pl.col("VENDOR_ADDRESS_ZIP_CODE").str.slice(0,5).alias("ZIP5"))
    # dollars=plds_zip.groupby("ZIP5","FISCAL_YEAR","VENDOR_ADDRESS_STATE_NAME").agg(pl.col(dolcols).sum())
    # dollars=dollars.join(SBA_match_pl,how="left",left_on="ZIP5",right_on='ZIP.Code').sort("FISCAL_YEAR").collect().to_pandas()
    return dollars
#select(["VENDOR_ADDRESS_STATE_NAME","VENDOR_ADDRESS_ZIP_CODE","FISCAL_YEAR"],dolcols).

#get user input on the state/DO/Region
state_select=pd.concat([pd.Series(["No Selection"]),SBA_match["State.Name"].dropna().drop_duplicates().sort_values()])
region_select=pd.concat([pd.Series(["No Selection"]),SBA_match["SBA.Region"].round(0).dropna().astype(int).drop_duplicates().sort_values()])
DO_select=pd.concat([pd.Series(["No Selection"]),SBA_match["SBA.District.Office"].dropna().drop_duplicates().sort_values()])

st.title("Local Small Business Achievements by FY")
state="Alabama";region="No Selection";DO="No Selection"

state=st.sidebar.selectbox(label="State",options=state_select,index=1)
region=st.sidebar.selectbox(label="Region",options=region_select,index=0)
DO=st.sidebar.selectbox(label="SBA District",options=DO_select,index=0)
#st.sidebar.write("Max year = ",max_year)

#all dollars is initial display
if DO != "No Selection":
	var="SBA.District.Office";select=DO
elif region != "No Selection":
	var="SBA.Region";select=region
else:
	var="State";select=state.upper()

#prepare the state table
dollars=get_dollars()
#for dashboard, save to parquet
#dollars.to_parquet("dollars.parquet") #COMMENT OUT


dolcols=["TOTAL_SB_ACT_ELIGIBLE_DOLLARS","SMALL_BUSINESS_DOLLARS","SDB_DOLLARS","WOSB_DOLLARS","SRDVOB_DOLLARS","CER_HUBZONE_SB_DOLLARS"]
#%%
dollars=dollars.drop(columns="State").rename(columns = {'VENDOR_ADDRESS_STATE_NAME':'State'})
#%%
select_dollars=dollars[dollars[var]==select].groupby('FISCAL_YEAR')[dolcols].sum()
doldict={"TOTAL_SB_ACT_ELIGIBLE_DOLLARS":"Total$","SMALL_BUSINESS_DOLLARS":"SmallBusiness$","SDB_DOLLARS":"SDB$","WOSB_DOLLARS":"WOSB$","CER_HUBZONE_SB_DOLLARS":"HUBZone$","SRDVOB_DOLLARS":"SDVOSB$"}
select_dollars=select_dollars.rename(columns=doldict)
#%%
#%%
select_pct=select_dollars.iloc[:,1:].div(select_dollars.iloc[:,0], axis=0).multiply(100).round(2)
select_pct.columns=select_pct.columns.str.replace("$","%",regex=False)

#.set_axis(pct_names, axis='columns', copy=False).reset_index()
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

st.table(select_dollars.style.format('$ {:,.0f}')
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
