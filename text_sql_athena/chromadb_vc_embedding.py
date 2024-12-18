

from typing import Any
from chromadb import chromadb, QueryResult
from langchain_community.document_loaders import JSONLoader

from text_sql_athena.vector_embedding import EmbeddingBedrock
from .custom_logger import logger

from .llm_basemodel import LanguageModel
from .aws_client_factory import AwsClientFactory

"""
    Here we will use Chroma as our local vector database. More details here https://docs.trychroma.com/getting-started

"""
def is_empty_or_whitespace(comment):
    """Checks if a string is None, empty, or contains only whitespace."""
    if comment is None:
        return True
    return not comment.strip()  # Check if the string is empty after removing whitespace


class EmbeddingBedrockChroma:
    def __init__(self, embedding_bedrock: EmbeddingBedrock, language_model: LanguageModel, collection_name="athena_embed_collection", chromadb_path="chroma.db"): 
        self.language_model = language_model
        self.embeddings = embedding_bedrock.embeddings

        self.chroma_client = chromadb.PersistentClient(path=chromadb_path)
        if not self.check_if_collection_exists(collection_name):  # Check if collection exists
            self.collection = self.chroma_client.get_or_create_collection(name=collection_name)
            logger.warning(f"Collection '{collection_name}' created.") #confirmation message to user.
        else:
            self.collection = self.chroma_client.get_collection(name=collection_name) # Get collection if exist
            logger.info(f"Collection '{collection_name}' already exists. Loading existing collection.")
    
    def check_if_collection_exists(self, collection_name: str):
        try:
            self.chroma_client.get_collection(name=collection_name)
            return True  # Collection exists
        except Exception as e:
            if "does not exist" in str(e):  # Specific error for non-existent collection
                return False  # Collection does not exist
            else:
                raise  # Re-raise other exceptions
    
    def add_documents(self, file_name: str):
        documents = JSONLoader(file_path=file_name, jq_schema='.', text_content=False, json_lines=False).load()

        self.collection.add(
            documents=[d.page_content for d in documents], # Assuming page_content holds the text
            embeddings=self.embeddings.embed_documents([d.page_content for d in documents]), # Embed with your model
            metadatas=[d.metadata for d in documents],  # Add metadata
            ids=[d.metadata.get("id") or str(i) for i, d in enumerate(documents)] # Use existing IDs or generate new ones
        )
    
    def add_json(self, document_text: str, metadata: dict[str, Any], doc_id: str):
        documents_to_add = [document_text]

        self.collection.add(
            documents=documents_to_add,
            embeddings=self.embeddings.embed_documents(documents_to_add), # Embed with your model
            metadatas=[metadata],  # Add metadata
            ids=[doc_id]
        )        

    def get_similarity_search(self, user_query: str, k=200) : # remove vcindex parameter
        query_embedding = self.embeddings.embed_query(user_query) # Embed the query

        results : QueryResult = self.collection.query(
            query_embeddings=query_embedding,
            n_results=k
        )
        return results # ChromaDB returns distances, ids, embeddings, and metadatas

    
    def format_metadata(self,documents: list):
        docstr = map(lambda x: x['doc'], documents)
        result = '\n'.join(docstr)

        return result
    
    def transform_data(self, results : QueryResult):        
        docs = []
        nb_docs = len(results['ids'][0])
        logger.debug(nb_docs)
        for i in range(nb_docs):
            document = {
                "id": results['ids'][0][i],
                "doc": results['documents'][0][i],
                "metadata": results['metadatas'][0][i],
                "distance": results['distances'][0][i],
                    
            }
            docs.append( document )
        
        return docs
