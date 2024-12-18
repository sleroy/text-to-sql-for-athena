import json
import sys
import traceback
import logging
import uuid

from text_sql_athena.aws_client_factory import AwsClientFactory
from .custom_logger import logger
from .chromadb_vc_embedding import EmbeddingBedrockChroma
from .llm_basemodel import ChatBedrock
from .llm_basemodel import LanguageModel


"""

    This code query glue databases and tables for their schema and declare them as embedding.

"""


def is_empty_or_whitespace(comment):
    """Checks if a string is None, empty, or contains only whitespace."""
    if comment is None:
        return True
    return not comment.strip()  # Check if the string is empty after removing whitespace


class GlueTableSchemaLoader:
    
    def __init__(self, chromaEmbeddingDB: EmbeddingBedrockChroma, region: str, client_factory: AwsClientFactory, language_model: LanguageModel): 
        self.chroma_db = chromaEmbeddingDB
        self.aws_region = region
        self.bedrock_runtime_client = client_factory.createBedrockRuntimeClient()
        self.glue_client = client_factory.createGlueClient()
        self.llm = language_model.llm
                

    def load_embedding_from_glue_data_tables(self, database_name):
        """Loads embeddings from Glue data tables into the Chroma collection."""
        # Get description from database      # Get description from database
        response = self.glue_client.get_database(Name=database_name)
        database_description = ""
        if 'Parameters' in response['Database'] and 'description' in response['Database']['Parameters']:
            database_description = response['Database']['Parameters']['description']
    
        
        response = self.glue_client.get_tables(DatabaseName=database_name)

        for table in response['TableList']:
            self.load_glue_table(table, database_description)  # Use helper method (shown below)
        
        while 'NextToken' in response:  # Handle pagination
            response = self.glue_client.get_tables(
                DatabaseName=database_name, NextToken=response['NextToken']
            )
            for table in response['TableList']:
                self.load_glue_table(table, database_description)

    def load_glue_table(self, table, database_description):
        """Helper method to load a single Glue table's schema."""
        
        databaseName = table['DatabaseName']
        tableName = table['Name']            

        logger.info(f"Loading table: {table['Name']}")
        logger.info(table)
        
        # Add a document with the information about the table.
        table_metadata = { "databaseName": databaseName, "tableName": tableName}
        self.chroma_db.add_json(document_text = f"""
                                The table {table['Name']} is part of the database {table['DatabaseName']}.
                                The database has for purpose {database_description}.
                                
                                The table metadata is "
                                {table}
                                "
                                """, metadata = table_metadata, doc_id = f"{databaseName}.{tableName}")
        
        for column in table['StorageDescriptor']['Columns']:
            columnName = column['Name']

            doc_id = str(uuid.uuid5(uuid .NAMESPACE_DNS, f"{databaseName}.{tableName}.{columnName}")) # Generate a deterministic ID

            # Check if an entry with this ID already exists
            existing_entry = self.chroma_db.collection.get(ids=[doc_id])
            if existing_entry and existing_entry['ids']:
                logger.info(f"Entry already exists for {tableName}.{columnName}, skipping.")
                continue  # Skip to the next column
            
                    
            metadata = { 
                "database_name": databaseName,
                "database_description": database_description,
                "table_name": tableName,
                "table_description" : table.get('Description', ''),
                "column_name": columnName,
                "column_type": column['Type'],
                "column_description": column.get('Comment', '')
            }
            
            comment = self.enrich_comment(table, metadata)
            
            doc_text =f"""
            Database : {databaseName}
            Database Description : {database_description}
            Table : {metadata['table_name']}
            Table Description : {metadata['table_description']}
            Column : {columnName}
            Column Type : {metadata['column_type']}
            Column Description : {metadata['column_description']}
            """
            


            
            column['Comment'] = comment[:254]
            
            self.chroma_db.add_json(document_text = doc_text, metadata = metadata, doc_id = doc_id)
        
        # Update the table schema directly in Glue        
        # updated_columns = table['StorageDescriptor']['Columns']  # Accumulate columns

        # try:
        #     self.glue_client.update_table(
        #         DatabaseName=table['DatabaseName'],
        #         TableInput={"Name": table['Name'], "StorageDescriptor": {"Columns": updated_columns}}
        #     )
        #     logger.info(f"Glue table {table['Name']} schema updated.")
        # except Exception as e:
        #     logger.error(f"Error updating Glue schema: {e}")
        
            

    def get_glue_databases(self):
        databases = []
        response = self.glue_client.get_databases()
        databases.extend(response['DatabaseList'])

        while 'NextToken' in response:
            response = self.glue_client.get_databases(NextToken=response['NextToken'])
            databases.extend(response['DatabaseList'])
        
        return databases

    def enrich_comment(self, table, doc):   
        comment = doc['column_description']
        if is_empty_or_whitespace(comment):
            logger.debug(f"Generating comment for {table['Name']}.{doc['column_name']}")

            messages = [
                (
                    "system",
                    f"""
                You are a helpful AI assistant specialized in generating database column descriptions.  \
                Given the table name and column details below, provide a concise and informative description of the column's purpose.
            """
                ),
                ("human", f"""
                
                Table Name: {table['Name']}
                Column Name: {doc['column_name']}
                Column Type: {doc['column_type']}
                """),
            ]
            ai_msg = self.llm.invoke(messages)
            comment = ai_msg.content

            doc['column_description'] = comment  # Update the doc dictionary
            logger.info(f"Column updated for {table['Name']}.{doc['column_name']} with {comment}")

        else:
            logger.debug(f"Column description already exist for {table['Name']}.{doc['column_name']}")
        return comment
