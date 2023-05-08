#%%
import pandas as pd
import streamlit as st
from datetime import datetime
import snowflake.snowpark as sp
from snowflake.snowpark.functions import max, col, countDistinct

#%%
def get_data ():
    connection_parameters = st.secrets.snowflake_credentials
    global session
    session = sp.Session.builder.configs(connection_parameters).create()
    data = session.table ("SAM_PUBLIC_MONTHLY_FILTERED")
    date_of = data.select(max("LAST_UPDATE_DATE")).toPandas().iat[0,0]
    return data, date_of

data, date_of = get_data ()
#%%
def page_config (date_of):
    st.set_page_config(
    page_title=f"Registrants as of {date_of.strftime('%d %b %Y')}",
    page_icon="https://www.sba.gov/brand/assets/sba/img/pages/logo/logo.svg",
   # layout="wide",
    initial_sidebar_state="expanded",
)
    st.header(f"SAM.gov Registrants as of {date_of.strftime('%d %B %Y')}")
#%%
def SAM_small_bus (data):
    options = ["All registrants","Small Business for any NAICS"
               ]
    small_bus = st.sidebar.radio ("Registrant Size", options, index = 1)

    if small_bus == options[1]:
        data = data.filter(
                           (col("PURPOSE_OF_REGISTRATION").rlike("Z2|Z5")) &
                           ~(col("BUS_TYPE_STRING").rlike("CY|12|2F|2R|3I|OH|A7|2U|A8|1D")) &
                            ~(col("SAM_EXTRACT_CODE").rlike("E|1|4")) & #only active records
                            ((col("NAICS_CODE_STRING").like("%Y%")) | 
                            (col("NAICS_EXCEPTION_STRING").like("%Y%"))))
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
    
    bus_types = st.sidebar.multiselect("Business/Socioeconomic Types", options= options)
    st.sidebar.caption ("Combine types for AND filter")

    if len(bus_types) > 0:
        lookup_list = [SAM_Extract_dict[x] for x in bus_types]
        for x in lookup_list:
            data = data.filter(col("BUS_TYPE_STRING").like(f"%{x}%"))


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
        df_for_in = session.create_dataframe(lookup_list, schema=["col1"])
        data = data.filter(col("PHYSICAL_ADDRESS_PROVINCE_OR_STATE").isin(df_for_in))

    if len(city_select) > 0:
        city_select = city_select.upper()
        data = data.filter(col("PHYSICAL_ADDRESS_CITY").startswith(city_select))

    return data

def display_list_and_count (data):
    col_dict = {
        "LEGAL_BUSINESS_NAME":"Name",
        "UNIQUE_ENTITY_ID":"UEI",
        'SAM_EXTRACT_CODE':"SAM Status (Active/Expired)",
        "PHYSICAL_ADDRESS_LINE_1":"Address1",
        "PHYSICAL_ADDRESS_LINE_2":"Address2",
        "PHYSICAL_ADDRESS_CITY":"City",
        "PHYSICAL_ADDRESS_PROVINCE_OR_STATE":"State",
        "PHYSICAL_ADDRESS_ZIPPOSTAL_CODE":"ZIP",
        "ENTITY_URL":"URL",
        "PSC_CODE_STRING":"PSC",
        'PRIMARY_NAICS':"Primary NAICS",
    }

    addl_cols= ["GOVT_BUS_POC_FIRST_NAME", "GOVT_BUS_POC_LAST_NAME",
                "NAICS_CODE_STRING", 'NAICS_EXCEPTION_COUNTER',"NAICS_EXCEPTION_STRING"]
    
    all_cols = list(col_dict.keys()) + addl_cols

    active_count = data.filter(col("SAM_EXTRACT_CODE")=="A").select(countDistinct(col("UNIQUE_ENTITY_ID"))).toPandas().iat[0,0]        
    
    st.write("Count of Active Registrations: ", '{:,}'.format(active_count))

    if st.button("Show Registrations"):
        data_df = data.select(all_cols).toPandas()
        POC = data_df["GOVT_BUS_POC_FIRST_NAME"] + " " + data_df["GOVT_BUS_POC_LAST_NAME"]
        data_df.insert(10, "POC", POC)
        
        data_df["NAICS String"] = [x if (y == 0)
                                else (x + " ~ " + z)
                                for x, y, z in zip(data_df['NAICS_CODE_STRING'], data_df['NAICS_EXCEPTION_COUNTER'], data_df['NAICS_EXCEPTION_STRING'])]
        
        data_df = data_df.drop(addl_cols, axis = 1).rename(columns = col_dict)

        st.write(data_df)
       
        st.download_button ("Download table"
           ,data_df.to_csv()
	       ,file_name="registrations.csv"
	    )

# %%
if __name__ == '__main__':
   
    data, date_of = get_data ()
    page_config(date_of)
    data = SAM_small_bus (data)
    data = socioeconomic_filter (data)
    data = state_filter (data)
    display_list_and_count (data)
