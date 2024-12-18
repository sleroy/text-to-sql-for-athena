
from langchain_community.embeddings import BedrockEmbeddings
from langchain_aws import ChatBedrock

class LanguageModel():
    def __init__(self,bedrock_client, embed_model_id="amazon.titan-embed-text-v1", llm_model_id= "us.anthropic.claude-3-5-sonnet-20241022-v2:0" , region_name= "us-east-1"):
        self.bedrock_client = bedrock_client
        self.embed_model_id = embed_model_id
        self.llm_model_id = llm_model_id
        self.region_name = region_name

        ############
        # Anthropic Claude     
        # Bedrock LLM
        inference_modifier = {
                ### "max_tokens_to_sample": 3000,
                "temperature": 0,
                "top_k": 20,
                "top_p": 1,
                "stop_sequences": ["\n\nHuman:"],
            }
        self.llm = ChatBedrock(
            #model_id = "anthropic.claude-v2:1",
            model_id = self.llm_model_id,
            region_name = self.region_name,
            client = self.bedrock_client, 
            model_kwargs = inference_modifier 
        )
        
        # Embeddings Modules
        self.embeddings = BedrockEmbeddings(
            client=self.bedrock_client, 
            model_id=self.embed_model_id, 
            region_name=self.region_name
        )
