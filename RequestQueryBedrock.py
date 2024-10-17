import boto3
import logging
from boto_client import Clientmodules
from llm_basemodel import LanguageModel
from athena_execution import AthenaQueryExecute
from openSearchVCEmbedding import EmbeddingBedrockOpenSearch
from langchain.schema import AIMessage

session = boto3.session.Session()
bedrock_client = session.client('bedrock')
print(bedrock_client.list_foundation_models()['modelSummaries'][0])

rqstath = AthenaQueryExecute()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

index_name = 'text_to_sql_index'
domain = 'https://e0hc00i67ga6mpn1xkxa.us-east-1.aoss.amazonaws.com'  # Update with your OpenSearch domain
region = 'us-east-1'
vector_name = 'embeddings_vector'
fieldname = 'id'

ebropen2 = EmbeddingBedrockOpenSearch(domain, vector_name, fieldname)
if ebropen2 is None:
    print("ebropen2 is null")
else:
    attrs = vars(ebropen2)
    print(', '.join("%s: %s" % item for item in attrs.items()))


class RequestQueryBedrock:
    def __init__(self, ebropen2):
        self.ebropen2 = ebropen2
        self.bedrock_client = ebropen2.bedrock_client
        if self.bedrock_client is None:
            self.bedrock_client = Clientmodules.createBedrockRuntimeClient()
        else:
            print("the bedrock_client is not null")
        self.language_model = LanguageModel(self.bedrock_client)
        self.llm = self.language_model.llm

    def getOpenSearchEmbedding(self, index_name, user_query):
        vcindxdoc = self.ebropen2.getDocumentfromIndex(index_name=index_name)
        document = self.ebropen2.getSimilaritySearch(user_query, vcindxdoc)
        return self.ebropen2.get_data(document)

    def generate_sql(self, prompt, max_attempt=4) -> str:
        attempt = 0
        error_messages = []
        prompts = [prompt]
        sql_query = ""

        while attempt < max_attempt:
            logger.info(f'Sql Generation attempt Count: {attempt + 1}')
            try:
                logger.info(f'we are in Try block to generate the sql and count is :{attempt + 1}')
                generated_sql = self.llm.invoke(prompt)

                # Handle AIMessage object
                if isinstance(generated_sql, AIMessage):
                    content = generated_sql.content
                elif isinstance(generated_sql, str):
                    content = generated_sql
                else:
                    content = str(generated_sql)

                # Extract SQL query from content
                sql_parts = content.split("```")
                if len(sql_parts) > 1:
                    query_str = sql_parts[1]
                else:
                    query_str = content

                query_str = " ".join(query_str.split("\n")).strip()
                sql_query = query_str[3:] if query_str.lower().startswith("sql") else query_str

                print(sql_query)
                syntaxcheckmsg = rqstath.syntax_checker(sql_query)
                if syntaxcheckmsg == 'Passed':
                    logger.info(f'syntax checked for query passed in attempt number :{attempt + 1}')
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
            except Exception as e:
                print(e)
                logger.error('FAILED')
                msg = str(e)
                error_messages.append(msg)
            finally:
                attempt += 1

        # If all attempts fail, raise an exception with details
        raise Exception(f"Failed to generate SQL after {max_attempt} attempts. Errors: {', '.join(error_messages)}")


rqst = RequestQueryBedrock(ebropen2)


def userinput(user_query):
    logger.info(f'Searching metadata from vector store')
    vector_search_match = rqst.getOpenSearchEmbedding(index_name, user_query)

    details = f"""It is important that the SQL query complies with Athena syntax.
              
              For our testing purposes, please only use the following data source(s), database(s) and table(s):
              
              Data Source: AWSDataCatalog              
              Database: imdb_stg
              Tables: basics, ratings
              Unique ID: tconst
              
              During a join, if two column names are the same please use alias (example: basics.tconst in select 
              statement). It is also important to pay attention to and not alter column format: if a column is string, 
              then leave column formatting alone and return a value that is a string. 
              
              If you are writing CTEs then include all the required columns. While concatenating a non-string column, 
              make sure to cast the column to string format first. If you encounter any instances where we must 
              compare date columns to strings, please cast the string input as a date and format as such. 
              
              REMEMBER: Only use the data source(S), database(s), and table(s) mentioned above. In addition,
              always include the database name along with the table name in the query."""

    final_question = "\n\nHuman:" + details + vector_search_match + user_query + "\n\nAssistant:"
    print("FINAL QUESTION :::" + final_question)

    try:
        answer = rqst.generate_sql(final_question)
        return answer
    except Exception as e:
        logger.error(f"Failed to generate SQL: {str(e)}")
        return None  # Or handle the error in a way that makes sense for your application


def main():
    # user_query = 'How many records in our database are from the year 1892?'
    user_query = 'What was the total number of votes for all movies with the word clown in the title?'
    # user_query = 'I need all of the unique ids from the Animation genre with an average rating of 5 or higher and at least 1000 votes'
    querygenerated = userinput(user_query)

    if querygenerated:
        import pprint
        my_printer = pprint.PrettyPrinter()
        my_printer.pprint(querygenerated)

        QueryOutput = rqstath.execute_query(querygenerated)
        print(QueryOutput)
    else:
        print("Failed to generate a valid SQL query.")


if __name__ == '__main__':
    main()
