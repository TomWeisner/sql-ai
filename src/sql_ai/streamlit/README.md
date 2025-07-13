# Streamlit

This directory defines various streamlit apps. These apps are UI that allow
users to ask questions about LNER data. There are currently two apps:

- app_kb: this is pointed to our various knowledge stores on aws bedrock. these are for UNstructured data e.g. confluence pages, pdf docs etc. edit the code to choose the kb.
- app_athena: this is pointed to various athena tables. edit the code to choose the table

## Run the app

From the root of the project run:
`streamlit run src/sql_ai./bedrock/streamlit/app_<suffix>.py`