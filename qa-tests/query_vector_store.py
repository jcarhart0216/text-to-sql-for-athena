import os
from langchain_community.vectorstores import FAISS
from llm_basemodel import LanguageModel
from boto_client import Clientmodules

# Absolute path to the vector store
vector_store_path = 'C:/code/text-to-sql-for-athena/vector_store/'

# Check if the vector store exists
if os.path.exists(vector_store_path):
    print("Vector store found!")
else:
    print("Vector store not found.")
    exit()


# Load the vector store
bedrock_client = Clientmodules.createBedrockRuntimeClient()
language_model = LanguageModel(bedrock_client)
embeddings = language_model.embeddings

vector_store = FAISS.load_local(vector_store_path, embeddings, allow_dangerous_deserialization=True)

print("Vector store loaded!")


# Query the vector store with a natural language input
user_query = "show me all the titles in the US region"

# Assuming you already have the vector store loaded as 'vector_store' from the previous step
if vector_store:
    results = vector_store.similarity_search(query=user_query, k=5)
    for result in results:
        print(result.metadata, result.page_content)
