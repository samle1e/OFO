#%%
import polars as pl
import pandas as pd
import streamlit as st
from datetime import datetime

date_of = "20230402" #have to change every month!

#%%
def get_data ():
    import pyarrow.dataset as ds
    import os
    import pyarrow as pa

    datalake="C:\\Users\\SQLe\\U.S. Small Business Administration\\Office of Policy Planning and Liaison (OPPL) - Data Lake\\"
    Extractsdir=f"{datalake}/SAM_entity_registration"
    files = os.listdir(Extractsdir)
    files.sort()
    most_recent=files[-1]
    date_of=most_recent[-8:]

    arrowds=ds.dataset(f"{datalake}/SAM_entity_registration/{most_recent}",format="parquet")
    data=pl.scan_ds(arrowds)
    return data, date_of

def create_data_local():
    data, date_of = get_data ()
    cols_used=["PURPOSE.OF.REGISTRATION", "BUS.TYPE.STRING", "NAICS.CODE.STRING", "NAICS.EXCEPTION.STRING"
               , "PHYSICAL.ADDRESS.PROVINCE.OR.STATE", "PHYSICAL.ADDRESS.CITY"
               ,"GOVT.BUS.POC.FIRST.NAME", "GOVT.BUS.POC.LAST.NAME",
                'NAICS.EXCEPTION.COUNTER'
                ,"LEGAL.BUSINESS.NAME",
        "UNIQUE.ENTITY.ID",
        'SAM.EXTRACT.CODE',
        "PHYSICAL.ADDRESS.LINE.1",
        "PHYSICAL.ADDRESS.LINE.2",
        "PHYSICAL.ADDRESS.ZIP/POSTAL.CODE",
        "ENTITY.URL","GOVT.BUS.POC.U.S..PHONE",
        "GOVT.BUS.POC.EMAIL.",
        "PSC.CODE.STRING",
        'PRIMARY.NAICS']
    data=data.select(pl.col(cols_used)).collect()
    data.write_parquet(f"registrants_{date_of}.parquet",row_group_size=1000000,use_pyarrow=True,compression="zstd",compression_level=22)

#create_data_local()
def get_data_local ():
    data = pl.scan_parquet(f"registrants_{date_of}.parquet")
    return data, date_of

def page_config (date_of):
    date = datetime.strptime(date_of,'%Y%m%d')
    st.set_page_config(
    page_title=f"Registrants as of {date.strftime('%d %b %Y')}",
    page_icon="https://www.sba.gov/brand/assets/sba/img/pages/logo/logo.svg",
   # layout="wide",
    initial_sidebar_state="expanded",
)
    st.header(f"SAM.gov Registrants as of {date.strftime('%d %B %Y')}")

def SAM_small_bus (data):
    options = ["All registrants","Small Business for any NAICS"
               ]
    small_bus = st.sidebar.radio ("Registrant Size", options, index = 1)

    if small_bus == options[1]:
        data = data.filter(
                           (pl.col("PURPOSE.OF.REGISTRATION").str.contains("Z2|Z5")) &
                           ~(pl.col("BUS.TYPE.STRING").str.contains("CY|12|2F|2R|3I|OH|A7|2U|A8|1D")) 
                           # & ~(pl.col("SAM.EXTRACT.CODE").str.contains("E|1|4")) #only active records
                           ).filter((pl.col("NAICS.CODE.STRING").str.contains("Y")) | 
                           (pl.col("NAICS.EXCEPTION.STRING").str.contains("Y")))
    return data
#%%
def socioeconomic_filter (data):

    def get_SAM_extract(): #v6.0 v3 as of 2023-04-12
        url = "https://open.gsa.gov/api/sam-entity-extracts-api/v1/SAM%20Master%20Extract%20Mapping%20v6.0%20Sensitive%20File%20V3%20Layout.xlsx"
        SAM_Extract = pd.read_excel(url
                                    ,sheet_name="STRING Clarification",skiprows=64,nrows=78
                                    ,usecols=[1,2])
        SAM_Extract=SAM_Extract.set_index(['Business Type Name'])
        SAM_Extract.to_csv("SAM_Extract_codes.csv")

    def get_SAM_extract_local ():
        SAM_Extract = pd.read_csv("SAM_Extract_codes.csv").set_index(['Business Type Name'])
        SAM_Extract_dict = SAM_Extract.squeeze().to_dict()
        return SAM_Extract_dict

    SAM_Extract_dict = get_SAM_extract_local ()

    options = list(SAM_Extract_dict.keys())
    
    bus_types = st.sidebar.multiselect("Business/Socioeconomic Types (select multi)", options= options)

    if len(bus_types) > 0:
        lookup_list = [SAM_Extract_dict[x] for x in bus_types]
        lookupstring = '|'.join(lookup_list)
        data = data.filter(pl.col("BUS.TYPE.STRING").str.contains(lookupstring))


    return data


#%%
def state_filter (data):
    def get_state_names ():
        state_names=pd.read_csv("https://raw.githubusercontent.com/jasonong/List-of-US-States/master/states.csv")
        state_names=state_names.set_index("Abbreviation")
        terr_dict={"PR":"Puerto Rico","GU":"Guam"
                                        ,"AS":"American Samoa"	
                                        ,"MP":"Northern Mariana Is."
                                        ,"VI":"Virgin Islands"}
        terr_df = pd.DataFrame(list(terr_dict.items()), columns=['Abbreviation','State']).set_index("Abbreviation")
        state_names = pd.concat([state_names, terr_df])
        state_names.to_csv("state_names.csv")
    
    def get_state_names_local ():
        state_names = pd.read_csv("state_names.csv").set_index("State")
        state_names_dict = state_names.squeeze().to_dict()
        return state_names_dict  

    state_names_dict = get_state_names_local ()

    options = list(state_names_dict.keys())
    options.sort()

    state_select = st.sidebar.multiselect("State (select multi)", options = options)

    city_select = st.sidebar.text_input("City", disabled = (len(state_select)==0))

    if len(state_select) > 0:
        lookup_list = [state_names_dict[x] for x in state_select]
        data = data.filter(pl.col("PHYSICAL.ADDRESS.PROVINCE.OR.STATE").is_in(lookup_list))

    if len(city_select) > 0:
        city_select = city_select.upper()
        data = data.filter(pl.col("PHYSICAL.ADDRESS.CITY").str.starts_with(city_select))

    return data

def display_list_and_count (data):
    col_dict = {
        "LEGAL.BUSINESS.NAME":"Name",
        "UNIQUE.ENTITY.ID":"UEI",
        'SAM.EXTRACT.CODE':"SAM Status (Active/Expired)",
        "PHYSICAL.ADDRESS.LINE.1":"Address1",
        "PHYSICAL.ADDRESS.LINE.2":"Address2",
        "PHYSICAL.ADDRESS.CITY":"City",
        "PHYSICAL.ADDRESS.PROVINCE.OR.STATE":"State",
        "PHYSICAL.ADDRESS.ZIP/POSTAL.CODE":"ZIP",
        "ENTITY.URL":"URL",
        #,POC:"paste0(GOVT.BUS.POC.FIRST.NAME, " ",GOVT.BUS.POC.LAST.NAME)"
        "GOVT.BUS.POC.U.S..PHONE":"Phone",
        "GOVT.BUS.POC.EMAIL.":"Email",
        "PSC.CODE.STRING":"PSC",
        'PRIMARY.NAICS':"Primary NAICS",
        #'NAICS.CODE.STRING':"NAICS String",
        #'NAICS.EXCEPTION.STRING':"NAICS ",
    }

    addl_cols= ["GOVT.BUS.POC.FIRST.NAME", "GOVT.BUS.POC.LAST.NAME",
                "NAICS.CODE.STRING", 'NAICS.EXCEPTION.COUNTER',"NAICS.EXCEPTION.STRING"]
    
    all_cols = list(col_dict.keys()) + addl_cols

    active_count = data.filter(pl.col("SAM.EXTRACT.CODE")=="A").select(
        pl.col("UNIQUE.ENTITY.ID").n_unique()).collect().to_pandas()
    
    st.write("Count of Active Registrations: ", str(active_count.iloc[0,0]))

    if st.button("Show Registrations"):
        data_df = data.select(all_cols).collect().to_pandas()
        POC = data_df["GOVT.BUS.POC.FIRST.NAME"] + " " + data_df["GOVT.BUS.POC.LAST.NAME"]
        data_df.insert(10, "POC", POC)
        
        data_df["NAICS String"] = [x if (y == 0)
                                else (x + " ~ " + z)
                                for x, y, z in zip(data_df['NAICS.CODE.STRING'], data_df['NAICS.EXCEPTION.COUNTER'], data_df['NAICS.EXCEPTION.STRING'])]
        
        data_df = data_df.drop(addl_cols, axis = 1).rename(columns = col_dict)

        st.write(data_df)
       
        st.download_button ("Download table"
           ,data_df.to_csv()
	       ,file_name="registrations.csv"
	    )

# %%
if __name__ == '__main__':
    #data, date_of = get_data()
    
    data, date_of = get_data_local ()
    page_config(date_of)
    data = SAM_small_bus (data)
    data = socioeconomic_filter (data)
    data = state_filter (data)
    display_list_and_count (data)
