#%%
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from snowflake.connector import connect

page_title= "SBA Scorecard Data Explorer"

top_caption_text = '''This report shows the dollars awarded to small businesses in the following demographic categories: small business, small disadvantaged business (SDB),
women-owned small business (WOSB), service-disabled veteran-owned (SDVOSB), and HUBZone small businesses. The final column shows the dollars awarded using contracts in the SBA's 8(a) program. 
Options include filtering by Department, Agency, or NAICS code, and viewing the results as a dollar amount or as percentage of total scorecard-eligible spending.
On the graph, single-clicking a category in the key will deselect, and double-clicking will isolate that category.
'''

bottom_caption_text =  '''Source: SBA Small Business Goaling Reports. Location is based on vendor business address. The departments and agencies are based on funding of the contract. 
Dollars are scorecard-eligible dollars after applying the exclusions on the [SAM.gov Small Business Goaling Report Appendix](https://sam.gov/reports/awards/standard/F65016DF4F1677AE852B4DACC7465025/view) (login required).\
'''

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

doldict={"TOTAL_SB_ACT_ELIGIBLE_DOLLARS":'Total Dollars',"SMALL_BUSINESS_DOLLARS":'Small Business Dollars',
         "SDB_DOLLARS":'SDB Dollars',"WOSB_DOLLARS":'WOSB Dollars',
         "SRDVOB_DOLLARS":'SDVOSB Dollars',"CER_HUBZONE_SB_DOLLARS":'HUBZone Dollars',
         "EIGHT_A_PROCEDURE_DOLLARS":'8(a) Procedures Dollars'}
doldict_pct = doldict|{k.replace('DOLLARS', 'DOLLARS_PCT'):v.replace('Dollars', 'Percent') for k,v in doldict.items()}
tb_name = 'STREAMLIT_SCORECARD'
linked_cols={'FUNDING_DEPARTMENT_NAME':'FUNDING_AGENCY_NAME', 'VENDOR_STATE':'VENDOR_CONGRESSIONAL_DIST'}
group_by_col = 'FISCAL_YEAR'


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
        if col not in doldict.keys():
            if (col not in linked_cols.keys()) and (col not in linked_cols.values()):
                query = f"select distinct {col} from {tb_name} where {col} is not null"
                options = get_data(query).squeeze().sort_values().to_list()
                filters[col]=options
            elif col in linked_cols.keys():
                query = f"select distinct {col}, {linked_cols[col]} from {tb_name} where {col} is not null and {linked_cols[col]} is not null"
                options_tbl = get_data(query)
                options_dict = options_tbl.groupby(col)[linked_cols[col]].apply(list).to_dict()
                filters[col]=options_dict
    filters = dict(sorted(filters.items()))
    return filters

def filter_sidebar(filters, linked_cols):
    st.sidebar.header("Choose Your Filters:")
    
    selections = {}
    for filter in filters.keys():
        if (filter != group_by_col) and (filter not in linked_cols.keys()):
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
    cols_small = [col for col in cols if col not in doldict.keys() and col != group_by_col]
    filters = {}
    for col in cols_small:
        if col in selections.keys() and len(selections[col])>0:
            filters[col] = selections[col]
    dolcols_str = ', '.join([f'sum({dol}) as {dol}, sum({dol})/nullif(sum(TOTAL_SB_ACT_ELIGIBLE_DOLLARS), 0) as {dol}_PCT' for dol in doldict.keys() if dol != 'TOTAL_SB_ACT_ELIGIBLE_DOLLARS'])
    where_str = ''.join([f'and {k} in (%({k})s)' for k,v in filters.items()])
    query = f'select {group_by_col} as "FISCAL YEAR", {dolcols_str} from {tb_name} where 1=1 {where_str} group by {group_by_col} order by 1'
        
    FY_table = get_data(query, filters).set_index('FISCAL YEAR').rename(columns=doldict_pct)
    return FY_table

def show_dollars (data):
    pal = ["#002e6d", "#cc0000", "#969696", "#007dbc", "#197e4e", "#f1c400"]

    tbl = data.filter(regex='Dollars')    
    data_bar = [go.Bar(name=col, x = tbl.index, y=tbl[col], marker_color = p) for col, p in zip(tbl, pal)]
    fig = go.Figure(data=data_bar)
    fig.update_layout(barmode='group')
    
    st.plotly_chart(fig)
    st.write(tbl.reset_index()\
             .style.format({col:'${:,.0f}' for col in tbl.columns if col != 'FISCAL YEAR'}).hide(axis="index")\
             .to_html(),unsafe_allow_html=True) 

        
def show_percent (tbl):
    pal = ["#002e6d", "#cc0000", "#969696", "#007dbc", "#197e4e", "#f1c400"]
    tbl = tbl * 100
    data_scatter = [go.Scatter(name=col, x = tbl.index, y=tbl[col], marker_color = p, mode='lines') for col, p in zip(tbl, pal)]
    fig = go.Figure(data=data_scatter)
    fig.update_layout()

    st.plotly_chart(fig)
    st.write(tbl.reset_index()\
             .style.format({col:'{:.2f}%' for col in tbl.columns if col != 'FISCAL YEAR'}).hide(axis="index")\
             .to_html(),unsafe_allow_html=True) 
        
def percent_or_dollars(data):
    is_percentage = st.toggle("View as Percentage of Total Dollars")
    
    if ~data.empty:
        if is_percentage:
            show_percent (data.filter(regex='Percent'))
        else: show_dollars (data.filter(regex='Dollars'))
    else: st.write('No Data')

def download (data):
    st.download_button(label="Download Data"
           ,data=data.to_csv()
		   ,file_name='SBA_scorecard_data.csv'
	   )

if __name__ == "__main__":
    st.header(page_title)
    st.caption(top_caption_text)
    cols=get_columns()
    filters = get_filters(cols, linked_cols)
    selections = filter_sidebar(filters, linked_cols)
    FY_table = FY_table(cols, selections)
    percent_or_dollars (FY_table)
    download(FY_table)

    st.caption(bottom_caption_text)   
 