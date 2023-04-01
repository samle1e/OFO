#%%
import polars as pl
import pandas as pd
import streamlit as st
import plotly.express as px
import pyarrow.dataset as ds
import os
import pyarrow as pa
import json
#%%
def get_years_desktop():
    datalake="C:\\Users\\SQLe\\U.S. Small Business Administration\\Office of Policy Planning and Liaison (OPPL) - Data Lake\\"
    min_year = int(os.listdir(f"{datalake}/SBGR_parquet")[0].replace(
        "FY=",""))
    max_year = int(os.listdir(f"{datalake}/SBGR_parquet")[-1].replace(
        "FY=",""))
    return min_year, max_year
#%%
def get_data_desktop(year):
    datalake="C:\\Users\\SQLe\\U.S. Small Business Administration\\Office of Policy Planning and Liaison (OPPL) - Data Lake\\"
    arrowds=ds.dataset(f"{datalake}/SBGR_parquet/FY={year}",format="parquet")
    plds=pl.scan_ds(arrowds)
    return plds
#%%
def get_county_mapping_desktop():  

    import requests
    datalake="C:\\Users\\SQLe\\U.S. Small Business Administration\\Office of Policy Planning and Liaison (OPPL) - Data Lake\\"

    def get_HUD (url, element):
        token = open(f"{datalake}/Credentials/HUDuser.txt","r").read()
        headers = {"Authorization": "Bearer {0}".format(token)}
        response = requests.get(url, headers = headers)
        zip_data = pd.DataFrame(response.json()["data"]["results"])	
        zip_data = zip_data.rename(columns={"geoid":element}).set_index("zip")
        return zip_data

    zip_FIPS = get_HUD("https://www.huduser.gov/hudapi/public/usps?type=2&query=all","FIPS")
    zip_CD = get_HUD("https://www.huduser.gov/hudapi/public/usps?type=5&query=all","CD")

    zip_FIPS=zip_FIPS.join(zip_CD,rsuffix="_CD").reset_index()

    FIPS_to_name=pd.read_csv("https://www.ncei.noaa.gov/erddap/convert/fipscounty.csv")

    zip_FIPS.set_index("FIPS",inplace=True)
    FIPS_to_name["FIPS"]=FIPS_to_name["FIPS"].astype(str).str.zfill(5)
    FIPS_to_name.set_index("FIPS",inplace=True)

    ZIP_to_county=zip_FIPS.join(FIPS_to_name,how="left",rsuffix="_R")

    ZIP_to_county["County"]=ZIP_to_county["Name"].str.partition(", ")[2]

    state_names=pd.read_csv("https://raw.githubusercontent.com/jasonong/List-of-US-States/master/states.csv")
    state_names=state_names.set_index("Abbreviation").squeeze().to_dict()
    terr_dict={"PR":"Puerto Rico","GU":"Guam"
                                    ,"AS":"American Samoa"	
                                    ,"MP":"Northern Mariana Is."
                                    ,"VI":"Virgin Islands"}
    state_names.update(terr_dict)

    ZIP_to_county["State"]=ZIP_to_county["state"].map(state_names)

    return ZIP_to_county.sort_values(by=["zip","bus_ratio"],ascending=[True,False]).reset_index()
#%%
# ZIP_to_county=get_county_mapping_desktop()
# ZIP_to_county.to_parquet("../ZIP_to_FIPS_Name_20230327.parquet")
#%%
def get_county_mapping():
    return pd.read_parquet("ZIP_to_FIPS_Name_20230327.parquet")
ZIP_to_county=get_county_mapping()
#%%
#%%
def NAICS_PSC_names():
#get three sets of NAICS names
    NAICSnames=[None]*3
    NAICSnames[0]=pd.read_excel("https://www.census.gov/naics/2012NAICS/2-digit_2012_Codes.xls")
    NAICSnames[1]=pd.read_excel("https://www.census.gov/naics/2017NAICS/2-6%20digit_2017_Codes.xlsx")
    NAICSnames[2]=pd.read_excel("https://www.census.gov/naics/2022NAICS/2-6%20digit_2022_Codes.xlsx")

    for i in range(0,3):
        NAICSnames[i]=NAICSnames[i].filter(regex="Code|Title")
        NAICSnames[i]=NAICSnames[i].set_index(NAICSnames[i].columns[0])

    combined=NAICSnames[0].join([NAICSnames[1],NAICSnames[2]],how="outer")
    combined=combined.loc[combined.index.dropna()]
    combined.index=combined.index.astype("str")
    combined["Title"]=combined.iloc[:,0].combine_first(combined.iloc[:,1]
                                        ).combine_first(combined.iloc[:,2])
    NAICS_names=combined.sort_index().loc[:,"Title"].squeeze().to_dict()

    PSCnames=pd.read_excel(
        "https://www.acquisition.gov/sites/default/files/manual/PSC%20April%202022.xlsx")

    PSC_names=PSCnames.drop_duplicates("PSC CODE",keep="first").set_index(
        "PSC CODE").filter(regex="NAME")
    PSC_names.index=PSC_names.index.astype("str")
    PSC_names["Title"]=PSC_names.iloc[:,1].combine_first(PSC_names.iloc[:,0])
    PSC_names=PSC_names.sort_index().loc[:,"Title"].squeeze().to_dict()

    return NAICS_names,PSC_names
#%%
def get_Cong_dist_list():
    Cong_dist_list=pd.read_csv(
        "https://theunitedstates.io/congress-legislators/legislators-current.csv"
        ,usecols=["state","district"],converters=({"district":str}))
    Cong_dist_list=Cong_dist_list.loc[Cong_dist_list['district'].str.len()>0]
    Cong_dist_list['district']=Cong_dist_list['district'].astype(str).str.zfill(2)
    Cong_dist_list.sort_values(["state","district"],inplace=True)
    return Cong_dist_list
#%%
def get_choices_desktop():
    min_year, max_year=get_years_desktop()
    year_select=[min_year, max_year]

    state_select=ZIP_to_county["State"].drop_duplicates().sort_values().to_list()

    state_abbr={}
    for x in state_select:
        abbr=ZIP_to_county[ZIP_to_county["State"]==x]["state"].iloc[0]
        state_abbr.update({abbr:x})

    county_select={}
    for x in state_abbr:
        county_list=ZIP_to_county.loc[ZIP_to_county['state']==x][
            "County"].drop_duplicates().sort_values().to_list()
        county_select.update({x:county_list})

    CD_select={}
    for x in state_abbr.keys():
        CD=ZIP_to_county[(ZIP_to_county["state"]==x) &
                         (ZIP_to_county["bus_ratio_CD"]>0.5)][
            "CD"].drop_duplicates().sort_values().to_list()
        CD_select.update({x:CD})

    NAICS_select,PSC_select = NAICS_PSC_names()

    department_select=['AGENCY FOR INTERNATIONAL DEVELOPMENT', 'AGRICULTURE, DEPARTMENT OF', 'COMMERCE, DEPARTMENT OF'
            ,'DEPT OF DEFENSE', 'EDUCATION, DEPARTMENT OF', 'ENERGY, DEPARTMENT OF'
            ,'ENVIRONMENTAL PROTECTION AGENCY', 'GENERAL SERVICES ADMINISTRATION', 'HEALTH AND HUMAN SERVICES, DEPARTMENT OF'
            ,'HOMELAND SECURITY, DEPARTMENT OF', 'HOUSING AND URBAN DEVELOPMENT, DEPARTMENT OF', 'INTERIOR, DEPARTMENT OF THE'
            ,'JUSTICE, DEPARTMENT OF', 'LABOR, DEPARTMENT OF', 'NATIONAL AERONAUTICS AND SPACE ADMINISTRATION'
            ,'NATIONAL SCIENCE FOUNDATION', 'NUCLEAR REGULATORY COMMISSION', 'OFFICE OF PERSONNEL MANAGEMENT'
            ,'SMALL BUSINESS ADMINISTRATION', 'SOCIAL SECURITY ADMINISTRATION', 'STATE, DEPARTMENT OF'
            ,'TRANSPORTATION, DEPARTMENT OF', 'TREASURY, DEPARTMENT OF THE', 'VETERANS AFFAIRS, DEPARTMENT OF']

    choices=[year_select, state_select, state_abbr, county_select,CD_select
            ,NAICS_select, PSC_select, department_select]
    
    return choices
#%%
#choices=get_choices_desktop()
#json.dump(choices,open('choices_FOdash.json', 'w'))
#%%
def get_choices():
    return json.load(open ('choices_FOdash.json', 'r'))
#%%
def user_input():
    year_select, state_select, state_abbr, county_select, CD_select, NAICS_select, PSC_select, department_select = get_choices()
    inv_state_abbr={v: k for k, v in state_abbr.items()}

    year=st.sidebar.slider(label="Fiscal Year",min_value=year_select[0]
                        ,max_value=year_select[1]-1,value=year_select[1]-1)
    state=st.sidebar.multiselect(label="States (pick multi)",options=state_select)

    county_choice=[]
    CD_choice=[]

    if len(state)>0:
        state_abbr_select=[v for k,v in inv_state_abbr.items() if k in state]    

        county_list=[(i,k) for k,v in county_select.items() for i in v if k in state_abbr_select]
        county_choice=[f"{county}, {state}" for (county, state) in county_list]

        CD_list=[(i,k) for k,v in CD_select.items() for i in v if k in state_abbr_select]
        CD_choice=[f"{state} {CD[2:4]}" for (CD, state) in CD_list]

    counties=st.sidebar.multiselect(label="Counties (pick multi)",options=county_choice,disabled=(len(state)==0))

    CDs=st.sidebar.multiselect(label="Congressional Districts (pick multi)",options=CD_choice
                            ,disabled=(len(state)==0))

    NAICS_pick=st.sidebar.multiselect(label="NAICS (pick multi)"
                                ,options=[f"{k}: {v}" for k,v in NAICS_select.items()])
    NAICS_pick_short=[x.split(": ")[0] for x in NAICS_pick]

    #get all the six-digit NAICS picked
    NAICS=[x for x in NAICS_pick_short if len(x)==6]

    for i in NAICS_pick_short:
        if len(i)<6:
            NAICS.extend([x for x in NAICS_select.keys() if (len(x)==6) & (x.startswith(i))])

    PSC_pick=st.sidebar.multiselect(label="Product Service Codes (pick multi)"
                            ,options=[f"{k}: {v}" for k,v in PSC_select.items()])
    PSC_pick_short=[x.split(": ")[0] for x in PSC_pick]

    PSC = [x for x in PSC_pick_short if len(x)==4]

    for i in PSC_pick_short:
        if len(i)<4:
            PSC.extend([x for x in PSC_select.keys() if (len(x)==4) & (x.startswith(i))])
    return year, state, counties, CDs, NAICS, PSC

#%%
def filter_data (year, state, counties, CDs, NAICS, PSC):
    year_select, state_select, state_abbr, county_select, CD_select, NAICS_select, PSC_select, department_select = get_choices()

    data=get_data_desktop(year)
    if state:
        state_list=[abbr for abbr,name in state_abbr.items() if name in state]
        filter_data=data.filter(pl.col("ADDRESS_STATE").is_in(state_list))
    if counties:
        county_list=[", ".join([x.split(", ")[1],x.split(", ")[0]]) for x in counties]
        zip_county_list=ZIP_to_county.loc[(ZIP_to_county["Name"].isin(county_list)) & 
                                        (ZIP_to_county["bus_ratio"]>0.3), "zip"].to_list()
        filter_data=data.filter(pl.col("VENDOR_ADDRESS_ZIP_CODE").is_in(zip_county_list))
    if CDs:
        zip_cd_list=[]
        for x in state:
            state_list=[abbr for abbr,name in state_abbr.items() if name in state]
            CD_match=[x[3:5] for x in CDs if x[0:3]==x]
            add_list=ZIP_to_county.loc[(ZIP_to_county["State"]==x) & 
                        (ZIP_to_county["CD"].str.endswith(CD_match)), "zip"].to_list()
            zip_cd_list.extend(add_list)
        filter_data=data.filter(pl.col("VENDOR_ADDRESS_ZIP_CODE").is_in(zip_cd_list))
    if NAICS:
        filter_data=data.filter(pl.col("PRINCIPAL_NAICS_CODE").is_in(NAICS))
    if PSC:
        filter_data=data.filter(pl.col("PRODUCT_OR_SERVICE_CODE").is_in(PSC))
    if ~any([state, counties, CDs, NAICS, PSC]):
        filter_data=data
    return filter_data

#%%
def top_offices(filtered_data):
    officecols=[  "FUNDING_DEPARTMENT_NAME",
            "FUNDING_AGENCY_NAME",
            "FUNDING_OFFICE_NAME",
        ]
    dolcols=["TOTAL_SB_ACT_ELIGIBLE_DOLLARS",
            "SMALL_BUSINESS_DOLLARS",
            "WOSB_DOLLARS",
            "CER_HUBZONE_SB_DOLLARS",
            "SDB_DOLLARS",
            "SRDVOB_DOLLARS",
            "EIGHT_A_PROCEDURE_DOLLARS"]

    top_offices=filtered_data.select(officecols+dolcols).groupby(
        officecols).sum().sort("SMALL_BUSINESS_DOLLARS",descending=True).collect().to_pandas()
    return top_offices

#%%
def top_vendors(filtered_data):
    vendor_id={"VENDOR_UEI":"VENDOR_ID"
                ,"UEI_NAME":"VENDOR_NAME"
                ,"VENDOR_DUNS_NUMBER":"VENDOR_ID"
                ,"VENDOR_NAME":"VENDOR_NAME"}
    dolcols=["TOTAL_SB_ACT_ELIGIBLE_DOLLARS",
            "SMALL_BUSINESS_DOLLARS",
            "WOSB_DOLLARS",
            "CER_HUBZONE_SB_DOLLARS",
            "SDB_DOLLARS",
            "SRDVOB_DOLLARS",
            "EIGHT_A_PROCEDURE_DOLLARS"]

    if year<2022:
        vendor_cols=list(vendor_id.keys())[2:]
    else:
        vendor_cols=list(vendor_id.keys())[:2]
    top_vendors=filtered_data.select(vendor_cols+dolcols).groupby(
        vendor_cols).sum().sort("SMALL_BUSINESS_DOLLARS",descending=True).collect().to_pandas()
    return top_vendors
#%%
def top_products(filtered_data):
    productcols=['PRODUCT_OR_SERVICE_CODE',
                 'PRODUCT_OR_SERVICE_DESCRIPTION'
        ]
    dolcols=["TOTAL_SB_ACT_ELIGIBLE_DOLLARS",
            "SMALL_BUSINESS_DOLLARS",
            "SDB_DOLLARS",
            "WOSB_DOLLARS",
            "SRDVOB_DOLLARS",
            "CER_HUBZONE_SB_DOLLARS",
            "EIGHT_A_PROCEDURE_DOLLARS"]

    top_products=filtered_data.select(productcols+dolcols).groupby(
        productcols).sum().sort("SMALL_BUSINESS_DOLLARS",descending=True).collect().to_pandas()
    return top_products
#%%
def format_df (df):
    dollars_dict={
        "TOTAL_SB_ACT_ELIGIBLE_DOLLARS":"Total Dollars",
        "SMALL_BUSINESS_DOLLARS":"Small Business Dollars",
        "SDB_DOLLARS":"SDB Dollars",
        "WOSB_DOLLARS":"WOSB Dollars",
        "CER_HUBZONE_SB_DOLLARS":"HUBZone Dollars",
        "SRDVOB_DOLLARS":"SDVOSB Dollars",
        "EIGHT_A_PROCEDURE_DOLLARS":"8(a) Dollars",
    }
    df=df.rename(columns=dollars_dict) #rename columns

    indexlist=[]
    for x in list(dollars_dict.values()):
        newindex=df[df[x]>0].sort_values(x,ascending=False).head(500).index
        indexlist.extend(newindex)
    indexlist=list(set(indexlist))
    df=df.iloc[indexlist].set_index(df.columns[0])
    return df
#    indexes=
#%%
def display_data (top_offices,top_vendors,top_products):
    dollars_dict={
        "TOTAL_SB_ACT_ELIGIBLE_DOLLARS":"Total Dollars",
        "SMALL_BUSINESS_DOLLARS":"Small Business Dollars",
        "SDB_DOLLARS":"SDB Dollars",
        "WOSB_DOLLARS":"WOSB Dollars",
        "CER_HUBZONE_SB_DOLLARS":"HUBZone Dollars",
        "SRDVOB_DOLLARS":"SDVOSB Dollars",
        "EIGHT_A_PROCEDURE_DOLLARS":"8(a) Dollars",
    }
#    st.write()
    metric=st.radio("Select metric to graph",options=list(dollars_dict.values()),index=1)

    top_offices=format_df(top_offices).sort_values("Small Business Dollars")
    fig_offices = px.bar(top_offices.sort_values(metric).head(10)
        , x=metric, y="FUNDING_OFFICE_NAME", orientation='h')
    st.dataframe(top_offices,use_container_width=True)

    top_vendors=format_df(top_vendors).sort_values("Small Business Dollars")
    fig_offices = px.bar(top_vendors.sort_values(metric).head(10)
        , x=metric, y=top_vendors.columns[0], orientation='h')
    st.dataframe(top_vendors,use_container_width=True)

    top_offices=format_df(top_products).sort_values("Small Business Dollars")
    fig_offices = px.bar(top_products.sort_values(metric).head(10)
        , x=metric, y="PRODUCT_OR_SERVICE_DESCRIPTION", orientation='h')
    st.dataframe(top_products,use_container_width=True)

#%%
if __name__ == "__main__":
    st.set_page_config(
        page_title="Top Offices and Vendors",
        page_icon="https://www.sba.gov/brand/assets/sba/img/pages/logo/logo.svg",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.title("Top Offices and Vendors")
    year, state, counties, CDs, NAICS, PSC= user_input()
    filtered_data=filter_data(year, state, counties, CDs, NAICS, PSC)
    top_offices = top_offices(filtered_data)
    top_vendors = top_vendors(filtered_data)
    top_products = top_products(filtered_data)
    display_data (top_offices,top_vendors,top_products)
#%%


#%% [markdown]

# Industries
# Agencies
# What do they buy?
# States
# Congressional Districts
# Counties
# SBA Match
