# OFO
Local dashboards for SBA contracting data

The Local Scorecard dashboard is the main dashboard. To deploy it, you need:
1. Local_Scorecard.py
2. A secrets.toml file in the .streamlit folder that follows the structure in https://docs.streamlit.io/streamlit-community-cloud/get-started/deploy-an-app/connect-to-data-sources/secrets-management.
3. A config.toml file in the .streamlit folder
4. Python 3.8 and the packages in requirements.txt

The dashboard pulls from SBA's snowflake tables, which you need credentials to access.
