# Import necessary modules if not already done
from langchain_community.vectorstores import FAISS
from llm_basemodel import LanguageModel
from boto_client import Clientmodules

# Define the vector store path
vector_store_path = 'C:/code/text-to-sql-for-athena/vector_store/01102024202318.vs'

# Load the vector store
language_model = LanguageModel(Clientmodules.createBedrockRuntimeClient())
embeddings = language_model.embeddings
vector_store = FAISS.load_local(vector_store_path, embeddings, allow_dangerous_deserialization=True)

# Test multiple queries
test_queries = [
    "show me all the titles in the US region",
    "what are the top 10 highest rated movies",
    "how many titles were released in 2020",
    "which movies have ratings above 9.5"
]

# Perform similarity search for each query
for user_query in test_queries:
    print(f"\nUser query: {user_query}")
    results = vector_store.similarity_search(query=user_query, k=5)
    for result in results:
        print(f"Metadata: {result.metadata}")
        print(f"Page Content: {result.page_content}")


# The output you're seeing looks correct in terms of what the vector store is returning. However, let's break it down a bit further.
#
# Each query you run is fetching metadata and page content from your vector store. The metadata field shows information like the file source (imdb_schema.jsonl in this case), and the page_content contains a detailed representation of the schema or tables from the IMDb dataset.
#
# Here’s a breakdown of what you're seeing for each query:
#
# User Query: "show me all the titles in the US region"
#
# The vector store returns metadata and page content, which includes the schema of the title and title_rating tables from your IMDb schema, such as primary keys, column names, and types. This includes columns like titleId, ordering, region, and averagerating.
# User Query: "what are the top 10 highest rated movies"
#
# Similar information is returned, mainly detailing the structure of the tables involved (title and title_rating), but it doesn’t actually generate a SQL query. The vector store isn't generating an actual answer yet; it’s returning the schema, likely to help the next step, which would be generating a query against this schema.
# User Query: "how many titles were released in 2020"
#
# Same schema details are returned. The response contains metadata about the dataset rather than the actual query result, which is the expected behavior from this step.
# User Query: "which movies have ratings above 9.5"
#
# Again, the output is schema information. This includes averagerating and numVotes from the title_rating table, which would be helpful for constructing a SQL query that filters based on ratings.