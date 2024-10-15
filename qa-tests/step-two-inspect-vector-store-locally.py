import os
from langchain_community.vectorstores import FAISS
from llm_basemodel import LanguageModel
from boto_client import Clientmodules

# Replace with your actual path
vector_store_path = 'C:/code/text-to-sql-for-athena/vector_store/01102024202318.vs'

# Check if the vector store exists
if os.path.exists(vector_store_path):
    print("Vector store found!")

    # Create the Bedrock client and load embeddings
    bedrock_client = Clientmodules.createBedrockRuntimeClient()
    language_model = LanguageModel(bedrock_client)
    embeddings = language_model.embeddings

    # Load the vector store with embeddings
    vector_store = FAISS.load_local(vector_store_path, embeddings, allow_dangerous_deserialization=True)

    # Check the number of vectors stored using 'ntotal'
    print(f"Vector store loaded with {vector_store.index.ntotal} embeddings.")
else:
    print("Vector store not found.")

