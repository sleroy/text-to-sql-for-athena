# sys.path.append("/home/ec2-user/SageMaker/llm_bedrock_v0/")
from .custom_logger import logger
from text_sql_athena.aws_client_factory import AwsClientFactory
import time
import pandas as pd
import io


class AthenaQueryExecute:
    def __init__(self, clientFactory: AwsClientFactory, glue_databucket_name='ATHENA-OUTPUT-BUCKET'):
        self.glue_databucket_name=glue_databucket_name
        self.athena_client = clientFactory.createAthenaClient()
        self.s3_client = clientFactory.createS3Client()
    
    def execute_query(self, query_string):
        # logger.info("Inside execute query", query_string)
        result_folder='athena_output'
        result_config = {"OutputLocation": f"s3://{self.glue_databucket_name}/{result_folder}"}
        query_execution_context = {
            "Catalog": "AwsDataCatalog",
        }
        logger.info(f"Executing: {query_string}")
        query_execution = self.athena_client.start_query_execution(
            QueryString=query_string,
            ResultConfiguration=result_config,
            QueryExecutionContext=query_execution_context,
        )
        execution_id = query_execution["QueryExecutionId"]
        time.sleep(120)

        #self.wait_for_execution(execution_id)
        file_name = f"{result_folder}/{execution_id}.csv"
        logger.info(f'checking for file :{file_name}')
        local_file_name = f"./tmp/{file_name}"

        logger.info(f"Calling download fine with params {local_file_name}, {result_config}")
        obj = self.s3_client.get_object(Bucket= self.glue_databucket_name , Key = file_name)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8')
        # logger.info(df.head())
        return df
        
    def syntax_checker(self,query_string):
        logger.info("Inside syntax_checker", query_string)
        query_result_folder='athena_query_output/'
        query_config = {"OutputLocation": f"s3://{self.glue_databucket_name}/{query_result_folder}"}
        query_execution_context = {
            "Catalog": "AwsDataCatalog",
        }
        query_string="Explain  "+query_string
        logger.info(f"Executing: {query_string}")
        try:
            logger.info(" I am checking the syntax here")
            query_execution = self.athena_client.start_query_execution(
                QueryString=query_string,
                ResultConfiguration=query_config,
                QueryExecutionContext=query_execution_context,
            )
            execution_id = query_execution["QueryExecutionId"]
            logger.info(f"execution_id: {execution_id}")
            time.sleep(3)
            results = self.athena_client.get_query_execution(QueryExecutionId=execution_id)
            # logger.info(f"results: {results}")
            status=results['QueryExecution']['Status']
            logger.info("Status :",status)
            if status['State']=='SUCCEEDED':
                return "Passed"
            else:  
                logger.info(results['QueryExecution']['Status']['StateChangeReason'])
                errmsg=results['QueryExecution']['Status']['StateChangeReason']
                return errmsg
            # return results
        except Exception as e:
            logger.error("Error in exception")
            msg = str(e)
            logger.error(msg)