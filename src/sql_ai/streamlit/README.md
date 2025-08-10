# Streamlit

This directory creates the streamlit UI.

The app must be supplied a ChatbotApp class that is instantiated
with relevant Athena table objects.

An example is providedin `src/sql_ai/app_meta_objects/pixar_films.py`

## Run the app

Ensure your are logged into AWS via CLI (you can use the AWS Toolkit VS Code Extension for this)

Then, from the root of the project run:
`streamlit run src/sql_ai./bedrock/streamlit/app.py`