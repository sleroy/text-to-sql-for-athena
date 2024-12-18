#######################################################################
# This file loads a small applet using Streamlit
#######################################################################

import streamlit as st
import sys
import time
from text_sql_athena.custom_logger import logger
from text_sql_athena.aws_client_factory import AwsClientFactory
from text_sql_athena.chromadb_vc_embedding import EmbeddingBedrockChroma
from text_sql_athena.glue_table_schema_loader import GlueTableSchemaLoader
from text_sql_athena.llm_basemodel import LanguageModel
from text_sql_athena.sql_generator import RequestQueryBedrock
from text_sql_athena.vector_embedding import EmbeddingBedrock

# Variables
region = "us-east-1"
chromadb_path = "./chroma.db"
athena_bucket_name='athena-storage-silvanly'
llm_model_id = "us.anthropic.claude-3-5-sonnet-20241022-v2:0"
embedding_model_id ="amazon.titan-embed-text-v1"

logger.info("Loading the page")

@st.cache_resource
def get_client_factory():
    return AwsClientFactory()

@st.cache_resource
def get_language_model(region, _client_factory: AwsClientFactory):    
    return LanguageModel(bedrock_client= _client_factory.createBedrockRuntimeClient(), region_name=region, embed_model_id=embedding_model_id, llm_model_id=llm_model_id)

@st.cache_resource
def get_chromadb_component(chromadb_path:str, _embedding_bedrock: EmbeddingBedrock, _language_model: LanguageModel):
    return EmbeddingBedrockChroma( chromadb_path=chromadb_path, 
                                language_model=_language_model, 
                                embedding_bedrock=_embedding_bedrock)


client_factory = get_client_factory()
language_model = get_language_model(region , client_factory)
embedding_bedrock = EmbeddingBedrock(language_model)
ebr_chroma = get_chromadb_component(chromadb_path, embedding_bedrock, _language_model=language_model)

def userinput(rqst: RequestQueryBedrock, user_query: str):
    logger.info(f'Searching metadata from vector store')
    vector_search_match=rqst.getEmbedding( user_query)

    details="It is important that the SQL query complies with Athena syntax. During join if column name are same please use alias ex llm.customer_id in select statement. It is also important to respect the type of columns: if a column is string, the value should be enclosed in quotes. If you are writing CTEs then include all the required columns. While concatenating a non string column, make sure cast the column to string. For date columns comparing to string , please cast the string input. Alwayws use the database name along with the table name"
    final_question = "\n\nHuman:"+details + vector_search_match + user_query+ "n\nAssistant:"
    logger.info("FINAL QUESTION :::" + final_question)
    answer = rqst.generate_sql(final_question)
    return answer

st.title("Text to Athena Applet");
st.write("Please type the query you want to execute with Athena")
query = st.text_input("Prompt for Athena", "Give me an unique list of hosts detected by the agents and that has Linux as an operation system.")
# st.session_state.query
if st.button('Generate the Query'):
    st.subheader('Sql statement for Athena')
    results = ebr_chroma.get_similarity_search(query)
    result_data = ebr_chroma.transform_data(results)

    
    rqst=RequestQueryBedrock(ebr_chroma, client_factory, athena_bucket_name, language_model=language_model)
    sqlquery = userinput(rqst, query)
    st.markdown(f"""
                ```sql
                {sqlquery}
                ```""")
    
## Metadata import
st.markdown("## Import Glue database metadata")
st.write("You can import the schemas of your AWS Glue table as embedding to improve the accuracy of the engine")

# Display combobox with the list of Glue databases
glueSchema = GlueTableSchemaLoader(client_factory=client_factory, chromaEmbeddingDB=ebr_chroma, region=region, language_model=language_model)
databases = glueSchema.get_glue_databases()

def get_glue_database_name(database_item):
    return database_item['Name']

databases = list(map(get_glue_database_name, databases))

database_selection = st.selectbox(
    "Select a Glue Database",
    databases,
)

# Launch import
if st.button("Import metadata from Glue database"):
    progress_bar = st.progress(0)  # Initialize progress bar
    
    def progress_callback(progress):
        progress_bar.progress(progress)


    def load_with_progress(glueSchema, database_selection):
        glueSchema.load_embedding_from_glue_data_tables(database_selection, progress_callback)    
        
    if load_with_progress(glueSchema, database_selection):
        st.write("Data imported")
else:
    pass
