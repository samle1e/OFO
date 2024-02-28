#%%
import pandas as pd
import streamlit as st
import plotly.express as px
import warnings
warnings.filterwarnings('ignore')
import numpy as np
from snowflake.connector import connect


page_title= "SBA Scorecard Data Explorer"

st.set_page_config(
    page_title=page_title,
    page_icon="https://www.sba.gov/brand/assets/sba/img/pages/logo/logo.svg",
    layout="wide",
    initial_sidebar_state="expanded")

hide_streamlit_style = """
             <style>
             footer {visibility: hidden;}
             </style>
             """

st.markdown(hide_streamlit_style, unsafe_allow_html=True)

#%%
# Columns for numbers and dollar amounts
dolcols=["TOTAL_SB_ACT_ELIGIBLE_DOLLARS","SMALL_BUSINESS_DOLLARS","SDB_DOLLARS","WOSB_DOLLARS","CER_HUBZONE_SB_DOLLARS","SRDVOB_DOLLARS","EIGHT_A_PROCEDURE_DOLLARS"]
# Mapping the renaming of the dollar amount columns
dolcols_rename=["Total$","SmallBusiness$","SDB$","WOSB$","HUBZone$","SDVOSB$","8(a)$"]
doldict = {k:v for k,v in zip(dolcols, dolcols_rename)}

tb_name = 'TMP_SBGR_GROUPED'

#%%
@st.cache_resource
def get_data(query, params=None):
    con = connect(**st.secrets.snowflake_credentials)
    cursor = con.cursor()
    if params:
        cursor.execute(query, params)
    else: 
        cursor.execute(query)
    results = cursor.fetch_pandas_all()
    return results

@st.cache_data
def get_columns():
    query = "select COLUMN_NAME from information_schema.columns where table_name = %s"
    cols = get_data(query, (tb_name)).squeeze().to_list()
    return cols

@st.cache_data
def get_filters(cols, linked_cols):
    filters = {}
    for col in cols:
        if col not in dolcols:
            if (col not in linked_cols.keys()) and (col not in linked_cols.values()):
                query = f"select distinct {col} from {tb_name} where {col} !='total'"
                options = get_data(query).squeeze().sort_values().to_list()
                filters[col]=options
            elif col in linked_cols.keys():
                query = f"select distinct {col}, {linked_cols[col]} from {tb_name} where {col} != 'total' and {linked_cols[col]} != 'total'"
                options_tbl = get_data(query)
                options_dict = options_tbl.groupby(col)[linked_cols[col]].apply(list).to_dict()
                filters[col]=options_dict
    filters = dict(sorted(filters.items()))
    return filters

def filter_sidebar(filters, linked_cols):
    st.sidebar.header("Choose Your Filters:")
    
    selections = {}
    for filter in filters.keys():
        if (filter != 'FISCAL_YEAR') and (filter not in linked_cols.keys()):
            selections[filter] = st.sidebar.multiselect(filter.replace('_',' '), sorted(filters[filter]))
        elif filter in linked_cols.keys():
            selections[filter] = st.sidebar.multiselect(filter.replace('_',' '), sorted(filters[filter].keys()))
            if len(selections[filter]) == 1:
                options=sorted(filters[filter][selections[filter][0]])
            else: options=[]
            selections[linked_cols[filter]] = st.sidebar.multiselect(linked_cols[filter].replace('_',' '), 
                                                                     options,
                                                                     disabled = len(options)==0)
    return selections

def FY_table(cols, selections):
    cols_small = [col for col in cols if col not in dolcols and col != 'FISCAL_YEAR']
    filters = {}
    for col in cols_small:
        if col in selections.keys() and len(selections[col])>0:
            filters[col] = selections[col]
        else: filters[col] = ['total']
    dolcols_str = ', '.join([f'sum({dol}) as {dol}' for dol in dolcols])
    where_str = ' and '.join([f'{k} in (%({k})s)' for k,v in filters.items()])
    query = f"select FISCAL_YEAR, {dolcols_str} from {tb_name} where {where_str} group by FISCAL_YEAR order by 1"
        
    FY_table = get_data(query, filters).set_index('FISCAL_YEAR').rename(columns=doldict)
    return FY_table
    

def display_chart(data, is_percentage):
    if is_percentage:
        chart_title = "Percentage of Dollars by Category and Year"
    else:
        chart_title = "Cumulative Dollars by Category and Year"
    
    data_long = data.melt(ignore_index=False).rename(columns={"variable": "Category", "value": "Value"})
    data_long.index = data_long.index.astype(str).rename("Fiscal Year")

    pal = ["#002e6d", "#cc0000", "#969696", "#007dbc", "#197e4e", "#f1c400"]
 
    fig = px.line(data_long, x=data_long.index, y="Value", color='Category',
                  color_discrete_sequence=pal,markers=True)
    
    fig.update_layout(xaxis=dict(tickmode='linear'))
    fig.update_layout(xaxis_tickformat='%Y')
    fig.update_traces(mode='markers+lines')
 
    st.subheader(chart_title)
    st.plotly_chart(fig,use_container_width=True)

def percent_chart(year_df):
    is_percentage = st.toggle("View as Percentage", value=True)
 
    if is_percentage:
        select_pct = year_df.iloc[:, 1:].div(year_df.iloc[:, 0], axis=0).multiply(100).round(2)
        select_pct.columns = select_pct.columns.str.replace("$", "", regex=False)
    else:
        select_pct=year_df[["SmallBusiness$","SDB$","WOSB$","HUBZone$","SDVOSB$","8(a)$"]].copy()
    
    display_chart(select_pct, is_percentage)
    return select_pct

# Create table by Year and dolcols
#%%
def table_chart_one(year_df):
    year_df_chart=year_df.copy()
    year_df_chart[dolcols_rename]=year_df_chart[dolcols_rename].applymap(lambda x: '${:,.0f}'.format(x))
    year_df_chart=year_df_chart
    st.table(year_df_chart)
    return year_df_chart
   
def table_percent(year_df):
    year_df_pct = year_df.copy()
    year_df_pct = year_df_pct.iloc[:, 1:].div(year_df_pct.iloc[:, 0], axis=0).multiply(100).round(2) 
    year_df_pct.columns = year_df_pct.columns.str.replace("$", "%", regex=False)
    st.table(year_df_pct.style.format('{:.2f}%'))
    return year_df_pct

def download_data(year_df,year_df_pct):
    year_df=year_df.set_index('FISCAL_YEAR')
    merge_df= pd.merge(year_df,year_df_pct, left_index=True, right_index=True)
    merge_df = merge_df[["Total$","SmallBusiness$","SmallBusiness%","SDB$","SDB%","WOSB$","WOSB%","HUBZone$","HUBZone%","SDVOSB$","SDVOSB%","8(a)$","8(a)%"]]
    st.download_button(label="Download Data",data=merge_df.to_csv(),file_name="scorecard.csv")

def expander(show_df):
    if len(show_df) <= 262144:
        with st.expander("CLICK HERE TO VIEW DETAILED DATA (INCLUDING ALL THE COLUMNS LOCATED ON THE LEFT FILTER).", expanded=False):
            detailed_df = show_df.groupby(['FISCAL_YEAR', 'VENDOR_ADDRESS_STATE_NAME', 'FUNDING_DEPARTMENT_NAME', 'FUNDING_AGENCY_NAME','NAICS'], as_index=False)[dolcols].sum()
            doldict = {"TOTAL_SB_ACT_ELIGIBLE_DOLLARS": "Total$", "SMALL_BUSINESS_DOLLARS": "SmallBusiness$", "SDB_DOLLARS": "SDB$",
                       "WOSB_DOLLARS": "WOSB$", "CER_HUBZONE_SB_DOLLARS": "HUBZone$", "SRDVOB_DOLLARS": "SDVOSB$",
                       "EIGHT_A_PROCEDURE_DOLLARS": "8(a)$", 'FISCAL_YEAR': 'Fiscal Year', 'VENDOR_ADDRESS_STATE_NAME': 'Vendor State',
                       'FUNDING_DEPARTMENT_NAME': 'Department', 'FUNDING_AGENCY_NAME': 'Agency', 'NAICS': 'NAICS Code'}
            detailed_df = detailed_df.rename(columns=doldict)
            detailed_df[dolcols_rename] = detailed_df[dolcols_rename].apply(lambda x: round(x,2))  # Round to 2 decimal placeS
            
            percent_df = detailed_df.iloc[:, 6:].div(detailed_df.iloc[:, 5], axis=0).multiply(100)
            percent_df.columns = percent_df.columns.str.replace("$", "%", regex=False)
            merged_df=pd.merge(detailed_df,percent_df,left_index=True, right_index=True)
            merged_df=merged_df[['Fiscal Year', 'Vendor State', 'Department', 'Agency','NAICS Code', 'Total$', 'SmallBusiness$','SmallBusiness%','SDB$','SDB%', 'WOSB$','WOSB%','HUBZone$','HUBZone%','SDVOSB$','SDVOSB%','8(a)$','8(a)%']]
            st.dataframe(merged_df)
            
            #To Download Data
            csv = detailed_df.to_csv(index=False).encode('utf-8')
            st.download_button("Download Data", data=csv, file_name="detailed_data.csv", mime="text/csv",
                               help='Click here to download the data as a CSV file')
    else:
        st.warning("To view a detailed dataset with columns such as Fiscal Year, State, Department, Agency,and NAICS please narrow down the options using the filters on the left.")

if __name__ == "__main__":
    st.header(page_title)
    cols=get_columns()
    linked_cols={'FUNDING_DEPARTMENT_NAME':'FUNDING_AGENCY_NAME', 'STATE_NAME':'CD'}
    filters = get_filters(cols, linked_cols)
    selections = filter_sidebar(filters, linked_cols)
    FY_table = FY_table(cols, selections)
    
    table_chart_one(FY_table)
    
    table_percent(FY_table) #Percent Table

    #download_df=download_data(group_df,percent_table)#Download data
    #expander_df= expander(filter)#Create Expander

    # start_time = time.time()
    # data = get_data()
    # st.write("--- %s seconds get_data ---" % (time.time() - start_time))

    # start_time = time.time()
    # filter = filter_sidebar(data)
    # st.write("--- %s seconds filter_sidebar ---" % (time.time() - start_time))
    
    # start_time = time.time()
    # group_df=group_data_year(filter)
    # st.write("--- %s seconds group_data_year ---" % (time.time() - start_time))
    
    # start_time = time.time()
    # selected_pct= percent_chart(group_df)
    # st.write("--- %s seconds percent_chart ---" % (time.time() - start_time))

    # start_time = time.time()
    # show_table=table_chart_one(group_df) #table dollars 
    # st.write("--- %s seconds table_chart_one ---" % (time.time() - start_time))

    # #percent_table=table_percent(selected_pct) #percent table
    # start_time = time.time()
    # percent_table=table_percent(group_df)
    # st.write("--- %s seconds table_percent ---" % (time.time() - start_time))

    # start_time = time.time()
    # download_df=download_data(group_df,percent_table)#download data
    # st.write("--- %s seconds download_data ---" % (time.time() - start_time))

    # start_time = time.time()
    # expander_df= expander(filter)
    # st.write("--- %s seconds expander---" % (time.time() - start_time))


    st.caption("""Source: SBA Small Business Goaling Reports, FY10-FY22. Location is based on vendor business address. This data does not apply double-credit adjustments and will not match up with the SBA small-business scorecard.\n
Abbreviations: SDB - Small Disadvantaged Business, WOSB - Women-owned small business, HUBZone - Historically Underutilized Business Zone, SDVOSB - Service-disabled veteran-owned small business, 8(a) - 8(a) - Socially and Economically disadvantaged Small Business\n
Total dollars are total scorecard-eligible dollars after applying the exclusions on the [SAM.gov Small Business Goaling Report Appendix](https://sam.gov/reports/awards/standard/F65016DF4F1677AE852B4DACC7465025/view) (login required).""")

   
 