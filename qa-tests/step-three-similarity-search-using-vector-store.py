import os
from langchain_community.vectorstores import FAISS
from llm_basemodel import LanguageModel
from boto_client import Clientmodules  # Make sure this is imported to create the Bedrock client

# Replace with your actual path
vector_store_path = 'C:/code/text-to-sql-for-athena/vector_store/01102024202318.vs'

if os.path.exists(vector_store_path):
    print("Vector store found!")

    # Load the vector store (with embeddings)
    language_model = LanguageModel(Clientmodules.createBedrockRuntimeClient())
    embeddings = language_model.embeddings  # Assuming you want to use embeddings during loading
    vector_store = FAISS.load_local(vector_store_path, embeddings, allow_dangerous_deserialization=True)

    # Check the number of vectors stored
    print(f"Vector store loaded with {vector_store.index.ntotal} embeddings.")

    # Run similarity search
    user_query = "show me all the titles in the US region"
    if vector_store:
        results = vector_store.similarity_search(query=user_query, k=5)
        for result in results:
            print(result.metadata, result.page_content)
else:
    print("Vector store not found.")
