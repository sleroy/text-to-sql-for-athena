##from langchain.document_loaders import JSONLoader
from .custom_logger import logger
import os,sys

sys.path.append("/home/ec2-user/SageMaker/llm_bedrock_v0/")

from .llm_basemodel import LanguageModel

from langchain_community.vectorstores import FAISS
from datetime import datetime

class EmbeddingBedrock:
    def __init__(self, language_model: LanguageModel):
        self.language_model = language_model
        self.llm = self.language_model.llm
        self.embeddings = self.language_model.embeddings
        self.embeddings_model_id=self.language_model.embed_model_id
        
    
    # def create_embeddings(self):
    #     documents = JSONLoader(file_path='imdb_schema.jsonl', jq_schema='.', text_content=False, json_lines=False).load()
    #     try:
    #         vector_store = FAISS.from_documents(documents, self.embeddings)
    #     except Exception:
    #         raise Exception("Failed to create vector store")
    #     logger.info("Created vector store")
    #     return vector_store
    
    def save_local_vector_store(self,vector_store, vector_store_path):
        time_now = datetime.now().strftime("%d%m%Y%H%M%S")
        vector_store_path=vector_store_path+'/'+time_now+'.vs'
        embeddings_model_id=self.embeddings_model_id
        try:
            if vector_store_path == "":
                vector_store_path = f"../vector_store/{time_now}.vs"
            os.makedirs(os.path.dirname(vector_store_path), exist_ok=True)
            vector_store.save_local(vector_store_path)
            with open(f"{vector_store_path}/embeddings_model_id", 'w') as f:
                f.write(embeddings_model_id)
        except Exception:
            logger.info("Failed to save vector store, continuing without saving...")
        return vector_store_path
    
    def load_local_vector_store(self,vector_store_path):
        try:
            with open(f"{vector_store_path}/embeddings_model_id", 'r') as f:
                embeddings_model_id = f.read()
            vector_store = FAISS.load_local(vector_store_path, self.embeddings)
            logger.info("Loaded vector store")
            return vector_store
        except Exception:
            logger.error("Failed to load vector store, continuing creating one...")

        # logger.info(load_local_vector_store(save_local_vector_store_path))
        
        
    def format_metadata(self,metadata):
        docs = []
        # Remove indentation and line feed
        for elt in metadata:
            processed = elt.page_content
            for i in range(20, -1, -1):
                processed = processed.replace('\n' + ' ' * i, '')
            docs.append(processed)
        result = '\n'.join(docs)
        # Escape curly brackets
        result = result.replace('{', '{{')
        result = result.replace('}', '}}')
        return result