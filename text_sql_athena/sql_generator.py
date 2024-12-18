# Contains the class to generate Sql query

import traceback
from .athena_execution import AthenaQueryExecute
from .aws_client_factory import AwsClientFactory
from .chromadb_vc_embedding import EmbeddingBedrockChroma
from .llm_basemodel import LanguageModel
from .custom_logger import logger

class RequestQueryBedrock:
    def __init__(self, ebropen2: EmbeddingBedrockChroma, client_factory: AwsClientFactory, athena_bucket_name:str, language_model:LanguageModel):
        self.language_model = language_model
        self.embedding_generator = ebropen2
        self.sqlsyntax_checker = AthenaQueryExecute(client_factory, athena_bucket_name)
        self.llm = self.language_model.llm
        
    def getEmbedding(self, user_query):
        qresult=self.embedding_generator.get_similarity_search(user_query)
        documents = self.embedding_generator.transform_data(qresult)
        
        return self.embedding_generator.format_metadata(documents)

        
    def generate_sql(self,prompt, max_attempt=4) ->str:
            """
            Generate and Validate SQL query.

            Args:
            - prompt (str): Prompt is user input and metadata from Rag to generating SQL.
            - max_attempt (int): Maximum number of attempts correct the syntax SQL.

            Returns:
            - string: Sql query is returned .
            """
            attempt = 0
            error_messages = []
            prompts = [prompt]  
            sql_query = ""
            while attempt < max_attempt:
                logger.info(f'Sql Generation attempt Count: {attempt+1}')
                try:
                    logger.info(f'we are in Try block to generate the sql and count is :{attempt+1}')
                    generated_sql = self.llm.predict(prompt)
                    logger.info(f"Generated sql : {generated_sql}")
                    query_str = generated_sql.split("```")[1]
                    query_str = " ".join(query_str.split("\n")).strip()                    
                    sql_query = query_str[3:] if query_str.startswith("sql") else query_str
                    logger.info(sql_query)
                    # return sql_query
                    syntaxcheckmsg=self.sqlsyntax_checker.syntax_checker(sql_query)
                    if syntaxcheckmsg=='Passed':
                        logger.info(f'syntax checked for query passed in attempt number :{attempt+1}')
                        return sql_query
                    else:
                        prompt = f"""{prompt}
                        This is syntax error: {syntaxcheckmsg}. 
                        To correct this, please generate an alternative SQL query which will correct the syntax error.
                        The updated query should take care of all the syntax issues encountered.
                        Follow the instructions mentioned above to remediate the error. 
                        Update the below SQL query to resolve the issue:
                        {sql_query}
                        Make sure the updated SQL query aligns with the requirements provided in the initial question."""
                        prompts.append(prompt)
                        attempt += 1
                except Exception as e:
                    logger.info(e)
                    msg = str(e)                    
                    logger.error(f'FAILED -> Sql Generation attempt Count: {attempt+1} {e}')
                    error_messages.append(msg)
                    attempt += 1
            return sql_query