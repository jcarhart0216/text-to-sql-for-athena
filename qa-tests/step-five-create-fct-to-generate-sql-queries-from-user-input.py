
# Run this using Athena?

from langchain_community.vectorstores import FAISS
from athena_execution import AthenaQueryExecute
from llm_basemodel import LanguageModel
from boto_client import Clientmodules

# Step 1: Define the SQL Query Generator Function
def generate_sql_query(user_query, metadata):
    """
    Generate SQL queries based on user input and metadata from the vector store.
    :param user_query: The user’s natural language query.
    :param metadata: Metadata from the vector store (e.g., table names, column names).
    :return: A SQL query.
    """
    sql_query = ""

    if "titles in the US region" in user_query:
        sql_query = f"""
        SELECT title, region 
        FROM {metadata['table_name']} 
        WHERE region = 'US';
        """

    elif "highest rated movies" in user_query:
        sql_query = f"""
        SELECT title, averagerating 
        FROM {metadata['table_name']} t
        INNER JOIN {metadata['related_table']} r
        ON t.titleId = r.tconst
        ORDER BY averagerating DESC
        LIMIT 10;
        """

    elif "released in 2020" in user_query:
        sql_query = f"""
        SELECT title 
        FROM {metadata['table_name']} 
        WHERE startYear = 2020;
        """

    elif "movies have ratings above 9.5" in user_query:
        sql_query = f"""
        SELECT title, averagerating 
        FROM {metadata['table_name']} t
        INNER JOIN {metadata['related_table']} r
        ON t.titleId = r.tconst
        WHERE averagerating > 9.5;
        """

    return sql_query

# Step 2: Initialize Embeddings and Vector Store, then Generate SQL Query
def main():
    # Initialize the embeddings using the LanguageModel class from llm_basemodel.py
    bedrock_client = Clientmodules.createBedrockRuntimeClient()
    language_model = LanguageModel(bedrock_client)
    embeddings = language_model.embeddings  # Initialize embeddings

    # Load the vector store from local storage with dangerous deserialization enabled
    vector_store_path = 'C:/code/text-to-sql-for-athena/vector_store/01102024202318.vs'
    vector_store = FAISS.load_local(vector_store_path, embeddings, allow_dangerous_deserialization=True)  # Allow dangerous deserialization

    # Example user query
    user_query = "show me all the titles in the US region"

    # Perform similarity search
    results = vector_store.similarity_search(query=user_query, k=5)

    # Extract metadata from the results
    metadata = {
        'table_name': 'title',  # Example: adjust based on vector store results
        'related_table': 'title_ratings'  # Relationship information for joins
    }

    # Generate the SQL query based on user query and metadata
    sql_query = generate_sql_query(user_query, metadata)

    # Print the generated SQL query
    print(f"Generated SQL Query:\n{sql_query}")

    # Execute the SQL query in Athena (you could also skip this part to test the SQL generation first)
    athena_executor = AthenaQueryExecute()
    query_result = athena_executor.execute_query(sql_query)
    print("Query Result:", query_result)

if __name__ == "__main__":
    main()
