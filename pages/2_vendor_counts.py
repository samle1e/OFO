#%%
import polars as pl
import pandas as pd
import streamlit as st
import plotly.express as px
import pyarrow.dataset as ds
import os
import pyarrow as pa
import json
import plotly.graph_objects as go

# datalake="C:\\Users\\SQLe\\U.S. Small Business Administration\\Office of Policy Planning and Liaison (OPPL) - Data Lake\\"

# arrowds=ds.dataset(f"{datalake}/SBGR_parquet",format="parquet",partitioning = ds.HivePartitioning(
#     pa.schema([("FY", pa.int16())])))
# plds=pl.scan_ds(arrowds)

# max_year = int(os.listdir(f"{datalake}/SBGR_parquet")[-1].replace("FY=",""))
# min_year = int(os.listdir(f"{datalake}/SBGR_parquet")[0].replace("FY=",""))
max_year=2022
min_year=2009
# %%
st.set_page_config(
    page_title="SBA Vendor Count",
    page_icon="https://www.sba.gov/brand/assets/sba/img/pages/logo/logo.svg",
   # layout="wide",
    initial_sidebar_state="expanded",
)

#%%
#extract vendor data
vendorcols=["VENDOR_DUNS_NUMBER","VENDOR_UEI"]
geocols=["VENDOR_ADDRESS_STATE_NAME","CONGRESSIONAL_DISTRICT"
         ,'VENDOR_ADDRESS_ZIP_CODE',]
buyercols=['FUNDING_DEPARTMENT_NAME','FUNDING_AGENCY_NAME']
contractcols=['IDV_TYPE_OF_SET_ASIDE','TYPE_OF_SET_ASIDE','PRINCIPAL_NAICS_CODE']
dolcols=["TOTAL_SB_ACT_ELIGIBLE_DOLLARS","SMALL_BUSINESS_DOLLARS","SDB_DOLLARS","WOSB_DOLLARS","CER_HUBZONE_SB_DOLLARS","SRDVOB_DOLLARS"]

vendor_dict={"TOTAL_SB_ACT_ELIGIBLE_DOLLARS":"All Vendors"
            ,"SMALL_BUSINESS_DOLLARS":"Small Business Vendors"
            ,"SDB_DOLLARS":"SDB Vendors"
            ,"WOSB_DOLLARS":"WOSB Vendors"
            ,"CER_HUBZONE_SB_DOLLARS":"HUBZone Vendors"
            ,"SRDVOB_DOLLARS":"SDVOSB Vendors"
    }
newdolcols=list(vendor_dict.values())

set_aside_dict={
        "SBA":"Small Business Set-Aside",
        "RSB":"Small Business Set-Aside",
        "ESB":"Small Business Set-Aside",
        "SBP":"Partial SB Set-Aside",
        "8A":"8(a) Competitive",
        "8AN":"8(a) Sole Source",
        "WOSB":"WOSB Set-Aside",
        "WOSBSS":"WOSB Sole Source",
        "EDWOSB":"EDWOSB Set-Aside",
        "EDWOSBSS":"EDWOSB Sole Source",
        "SDVOSBC":"SDVOSB Set-Aside",
        "SDVOSBS":"SDVOSB Sole Source",
        "HS3":"HUBZone Set-Aside",
        "HZC":"HUBZone Set-Aside",
        "HZS":"HUBZone Sole Source",
    }
department_select=['AGENCY FOR INTERNATIONAL DEVELOPMENT', 'AGRICULTURE, DEPARTMENT OF', 'COMMERCE, DEPARTMENT OF'
          ,'DEPT OF DEFENSE', 'EDUCATION, DEPARTMENT OF', 'ENERGY, DEPARTMENT OF'
          ,'ENVIRONMENTAL PROTECTION AGENCY', 'GENERAL SERVICES ADMINISTRATION', 'HEALTH AND HUMAN SERVICES, DEPARTMENT OF'
          ,'HOMELAND SECURITY, DEPARTMENT OF', 'HOUSING AND URBAN DEVELOPMENT, DEPARTMENT OF', 'INTERIOR, DEPARTMENT OF THE'
          ,'JUSTICE, DEPARTMENT OF', 'LABOR, DEPARTMENT OF', 'NATIONAL AERONAUTICS AND SPACE ADMINISTRATION'
          ,'NATIONAL SCIENCE FOUNDATION', 'NUCLEAR REGULATORY COMMISSION', 'OFFICE OF PERSONNEL MANAGEMENT'
          ,'SMALL BUSINESS ADMINISTRATION', 'SOCIAL SECURITY ADMINISTRATION', 'STATE, DEPARTMENT OF'
          ,'TRANSPORTATION, DEPARTMENT OF', 'TREASURY, DEPARTMENT OF THE', 'VETERANS AFFAIRS, DEPARTMENT OF']
state_select=['Alabama','Alaska','Arizona','Arkansas','California','Colorado','Connecticut',
    'Delaware','District of Columbia','Florida','Georgia','Guam','Hawaii','Idaho','Illinfois','Indiana',
    'Iowa','Kansas','Kentucky','Louisiana',
    'Maine','Maryland','Massachusetts','Michigan','Minnesota','Mississippi',
    'Missouri','Montana','Nebraska','Nevada',
    'New Hampshire','New Jersey','New Mexico','New York',
    'North Carolina','North Dakota','Ohio','Oklahoma',
    'Oregon','Pennsylvania','Puerto Rico','Rhode Island','South Carolina','South Dakota',
    'Tennessee','Texas','Utah','Vermont','Virginia',
    'Washington','West Virginia','Wisconsin','Wyoming']
state_select=[x.upper() for x in state_select]
#%%
def get_vendors():
    # vendors=plds.filter((pl.col("TOTAL_SB_ACT_ELIGIBLE_DOLLARS")>0) & (pl.col("FY")<max_year)).select(
    #     vendorcols+geocols+buyercols+contractcols+["FY"]+[pl.col(dolcols).map(lambda x: x>0)]).with_columns(
    #         pl.col("VENDOR_ADDRESS_ZIP_CODE").str.slice(0,5).alias("zip")
    #     )
    # vendors=vendors.unique()

    # ZIP_match=pl.read_csv("ZIP_to_FIPS_Name_20230322.csv",columns=["FIPS","zip","County","state","bus_ratio","state_names"]
    #                     ,dtypes={'zip':pl.Utf8, 'FIPS':pl.Utf8}
    #                     ).sort(by="bus_ratio",descending=True
    #                     ).drop("bus_ratio").unique(subset="zip",keep="first").lazy()
    # vendors=vendors.join(ZIP_match,how="left",on="zip").drop(["VENDOR_ADDRESS_ZIP_CODE","zip"])

    # vendors=vendors.with_columns(
    #     pl.when(pl.col("FY")<=2021)
    #     .then(pl.col("VENDOR_DUNS_NUMBER"))
    #     .otherwise(pl.col("VENDOR_UEI"))
    #     .alias("VENDOR_ID")
    #     ).drop(["VENDOR_DUNS_NUMBER","VENDOR_UEI"])

    #     #set-asides
    # SBA_set_asides=["SBA","SBP","RSB", "8AN", "SDVOSBC" ,"8A", "HZC","WOSB","SDVOSBS","HZS","EDWOSB"
    #     ,"WOSBSS","ESB","HS3","EDWOSBSS"]
    # SBA_socio_asides=SBA_set_asides[3:]

    # vendors=vendors.with_columns(
    #     pl.when(pl.col('TYPE_OF_SET_ASIDE').is_in(SBA_socio_asides))
    #         .then(pl.col('TYPE_OF_SET_ASIDE'))
    #         .when(pl.col('IDV_TYPE_OF_SET_ASIDE').is_in(SBA_set_asides))
    #         .then(pl.col('IDV_TYPE_OF_SET_ASIDE'))
    #         .otherwise(pl.col('TYPE_OF_SET_ASIDE'))
    #         .map_dict(set_aside_dict)
    #         .alias("set_aside")
    # ).drop(['IDV_TYPE_OF_SET_ASIDE','TYPE_OF_SET_ASIDE'])
    # vendors=vendors.rename({"VENDOR_ADDRESS_STATE_NAME":"State","state":"state_abbr","FUNDING_DEPARTMENT_NAME":"Department"
    #                             ,"FUNDING_AGENCY_NAME":"Agency","PRINCIPAL_NAICS_CODE":"NAICS"
    #                             ,"CONGRESSIONAL_DISTRICT":"Congressional District"})
    # vendors=vendors.rename(vendor_dict)
    # return vendors
    return pl.scan_parquet("VendorData.parquet")
#%%
# vendors=get_vendors().collect()
# vendors.write_parquet("Data",row_group_size=1000000,use_pyarrow=True,compression="zstd",compression_level=22)

#%%
def get_counts(vendors,var):
    for x in newdolcols:
        vendors=vendors.with_columns(
            pl.when(pl.col(x)==True)
            .then(pl.col("VENDOR_ID"))
            .otherwise(pl.lit("@"))
            .alias(x)
        )
    counts=vendors.select(newdolcols+[var]).groupby(var,maintain_order=True).n_unique()
    counts_adj=counts.select([pl.col(var),pl.col(newdolcols).map(lambda x:x-1)]).collect().to_pandas().set_index(var)
    return counts_adj

#%%
@st.cache_data
def get_choices():
    # vendors=get_vendors().collect().to_pandas()
    # six_digit_NAICS=vendors["NAICS"].drop_duplicates().sort_values().dropna()
    # two_digit_NAICS=six_digit_NAICS.str.slice(0,2).drop_duplicates().dropna()

    # NAICS_select=pd.concat([
    #     two_digit_NAICS,six_digit_NAICS], ignore_index=True
    #     ).drop_duplicates().sort_values().to_list()

    # agency_select={}
    # for x in department_select:
    #     agency_list=vendors[vendors['Department']==x]["Agency"].drop_duplicates().sort_values().to_list()
    #     agency_select.update({x:agency_list})
    # county_select={}
    # for x in state_select:
    #     county_list=vendors.loc[vendors['State']==x]["County"].drop_duplicates().sort_values().to_list()
    #     county_select.update({x:county_list})
    # CD_select={}
    # for x in state_select:
    #     CD_list=vendors.loc[vendors['State']==x]["Congressional District"].drop_duplicates().sort_values().to_list()
    #     CD_select.update({x:CD_list})

    # choices=[NAICS_select,agency_select,county_select,CD_select]
    # return choices
    return json.load(open ('Vendorchoices.json', 'r'))
#%%
choices=get_choices()
#json.dump(choices,open('choices.json', 'w'))

NAICS_select, agency_select, county_select, CD_select=get_choices()
department_select=list(agency_select.keys())
state_select=list(county_select.keys())
set_aside_select=list(dict.fromkeys(set_aside_dict.values()))

#%%
#initial_display
vendors=get_vendors()
#vendors.collect().write_parquet("vendors.parquet")
#%%
# user input
st.title("SBA Vendor Counts")
department=st.sidebar.selectbox(label="Department",options=["All"]+department_select,index=0)
if department != 'All':
    agency=st.sidebar.selectbox(label="Agency",options=["All"]+agency_select[department])
    vendors=vendors.filter(pl.col("Department")==department)

NAICS=st.sidebar.multiselect(label="NAICS",options=NAICS_select)
if len(NAICS)>0:
    vendors=vendors.filter(pl.col("NAICS").is_in(NAICS))

set_aside=st.sidebar.multiselect(label="Set Aside",options=set_aside_select)
if len(set_aside)>0:
    vendors=vendors.filter(pl.col("set_aside").is_in(set_aside))

state=st.sidebar.multiselect(label="State",options=state_select)
if len(state)>0:
    vendors=vendors.filter(pl.col("State").is_in(state))
if (len(state)==1):
    county=st.sidebar.multiselect(label="County",options=["All"]+county_select[state[0]],default="All")
    try:
        if (len(county)>0) & (county[0]!="All"):
            vendors=vendors.filter(pl.col("County").is_in(county))
    except:pass
    CD=st.sidebar.multiselect(label="Congressional District",options=["All"]+CD_select[state[0]],default="All")
    try:
        if (len(CD)>0) & (CD[0]!= "All"): 
            vendors=vendors.filter(pl.col("Congressional District").is_in(CD))
    except:pass

map_select=st.sidebar.selectbox(label="Map what type of vendor?",options=newdolcols,index=1)
year=st.sidebar.slider(label="Map which Fiscal Year?",min_value=min_year, max_value=max_year,value=max_year)
vendor_map=vendors.filter(pl.col("FY")==year)

#%%
def get_count_map(vendors,col,var):
    for x in newdolcols:
        vendors=vendors.with_columns(
        pl.when(pl.col(x)==True)
        .then(pl.col("VENDOR_ID"))
        .otherwise(pl.lit("@"))
        .alias(x)
    )
    counts=vendors.select([col]+[var]).groupby(var).n_unique()
    counts_adj=counts.select([pl.col(var),pl.col(col).map(lambda x:x-1)]).collect().to_pandas().set_index(var)
    return counts_adj
#%%
if (st.sidebar.button("Submit")): 
    with st.spinner("Working"):
        vendor_table=get_counts(vendors,"FY")
        pal = ["#002e6d", "#cc0000", "#969696", "#007dbc", "#197e4e", "#f1c400"]
        fig=px.line(vendor_table,x=vendor_table.index,y=vendor_table.columns
                    ,    color_discrete_sequence=pal,labels={"index":"FY","value":"vendors","variable":""}
        )
        # st.write(map_select)
        # st.write(year)
        map_table= get_count_map(vendor_map,map_select,"state_abbr").iloc[:,0]
        fig2 = go.Figure(data=go.Choropleth(
            locations=map_table.index, # Spatial coordinates
            z = map_table.array, # Data to be color-coded
            locationmode = 'USA-states', # set of locations match entries in `locations`
            colorscale = 'Portland',
            colorbar_title = "Vendors",
        ))

        fig2.update_layout(
            geo_scope='usa',
        )
else:
    vendor_table=pd.DataFrame()
    map_table=pd.DataFrame()
    fig=px.line()
    fig2=go.Figure(data=go.Choropleth(
        locationmode = 'USA-states', # set of locations match entries in `locations`
        colorscale = 'Portland',
        colorbar_title = "Vendors",
    )).update_layout(
            geo_scope='usa',
        )
#%%    
st.plotly_chart(fig)
st.table(vendor_table)
#st.table(map_table)
st.plotly_chart(fig2)

#%%

#%%
#or a state map -- THIS DOESN'T WORK
# if (st.checkbox("Show a county map")):
#     map_table=get_count_map(vendor_map,map_select,"FIPS").iloc[:,0]
#     map_table.index=map_table.index.str.zfill(5)

#     from urllib.request import urlopen
#     with urlopen('https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json') as response:
#         counties = json.load(response)

#     fig3 = px.choropleth(map_table, geojson=counties, locations=map_table.index, color=map_table.array,
#                             color_continuous_scale='Portland',
#                             scope="usa"
#                             )
#     fig3.layout.template = None
#     st.plotly_chart(fig3)   

#%%
#FY22 vendor counts
# FY22_vendors=vendors.loc[(vendors["FY"]==2022),['VENDOR_UEI','FUNDING_DEPARTMENT_NAME'
# 					      ]+dolcols].drop_duplicates()

# countsDF=pd.DataFrame(index=department_select,columns=dolcols)

# for x in countsDF.index:
#     for y in countsDF.columns:
# 	    countsDF.loc[x,y]=len(set(FY22_vendors.loc[(FY22_vendors[y]==True) & (FY22_vendors['FUNDING_DEPARTMENT_NAME']==x)
# 					,"VENDOR_UEI"]))

# totalvendors=pd.Series(index=dolcols,dtype='int64')
# for x in totalvendors.index:
#     totalvendors[x]=len(set(FY22_vendors.loc[(FY22_vendors[x]==True),"VENDOR_UEI"]))
# totalvendors=totalvendors.to_frame().transpose()

# countsDF=pd.concat([countsDF,totalvendors])
# countsDF.columns=countsDF.columns.str.replace("_DOLLARS","_Vendors")
# countsDF.sort_values(by="TOTAL_SB_ACT_ELIGIBLE_Vendors")

# countsDF.to_excel("FYvendors22.xlsx")

#%%
# Get latest ZIP to FIPS match with name
#  import requests

# url = "https://www.huduser.gov/hudapi/public/usps?type=2&query=all"
# token = open(f"{datalake}/Credentials/HUDuser.txt","r").read()
# headers = {"Authorization": "Bearer {0}".format(token)}

# response = requests.get(url, headers = headers)

# if response.status_code != 200:
# 	print ("Failure, see status code: {0}".format(response.status_code))
# else: 
# 	df = pd.DataFrame(response.json()["data"]["results"])	
# 	df.to_parquet("zip_county.parquet")

#FIPS to county name
# FIPS_to_name=pd.read_csv("https://www.ncei.noaa.gov/erddap/convert/fipscounty.csv")
# FIPS_to_name.to_csv("fipscounty.csv")

# ZIP_to_FIPS=df
# ZIP_to_FIPS["FIPS"]=ZIP_to_FIPS["geoid"].astype('int64')
# ZIP_to_FIPS.set_index("FIPS",inplace=True)
# FIPS_to_name.set_index("FIPS",inplace=True)

# ZIP_to_FIPS=ZIP_to_FIPS.join(FIPS_to_name,how="left",rsuffix="_R")

# ZIP_to_FIPS["County"]=ZIP_to_FIPS["Name"].str.partition(", ")[2]
# ZIP_to_FIPS.to_csv("ZIP_to_FIPS_Name_20230322.csv")
#%%
# ZIP_match=pd.read_csv("ZIP_to_FIPS_Name_20230322.csv",converters={'zip': str,'FIPS': str})
# state_names=pd.read_csv("statenames.csv",index_col="State Abbreviation").squeeze().to_dict()
# ZIP_match["state_names"]=ZIP_match["state"].map(state_names)
# ZIP_match.to_csv("ZIP_to_FIPS_Name_20230322.csv")

#%%
#%%
# with open('choices.json', 'w') as f:
#     json.dump(choices, f)