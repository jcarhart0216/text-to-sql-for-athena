import sys
import time
import traceback

from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection

from typing import List, Tuple
import logging
import numpy as np
import boto3

# from langchain.text_splitter import RecursiveCharacterTextSplitter
# from langchain_community.embeddings import BedrockEmbeddings
# from langchain_community.document_loaders import PyPDFLoader, PyPDFDirectoryLoader
from langchain_community.vectorstores import OpenSearchVectorSearch
from langchain_community.document_loaders import JSONLoader

logger = logging.getLogger()
# logging.basicConfig(format='%(asctime)s,%(module)s,%(processName)s,%(levelname)s,%(message)s', level=logging.INFO, stream=sys.stderr)

# sys.path.append("/home/ec2-user/SageMaker/llm_bedrock_v0/")#
# sys.path.append("//")
from llm_basemodel import LanguageModel
from boto_client import Clientmodules

# from opensearchpy import AWSV4SignerAuth
"""

    Connecting OpenSearch in AWS done in multiple ways. Here we are going to use userid and password to connect.
    Opensearch cluster can be inside VPC or Public. Recommended is inside VPC for all good reasons.
    Here for this demo I have made public. Opensearch is massively scalable search engine ,
    I have used it mostly for UI applications to render data in fraction of second. However it can also
    be used for Vector store. It provides similarity search using KNN, Cosine or more.
    We will have separate document for that. Here we will read PDF file and store in Openserach so that we can use
    that in our RAG Architecture.

"""

# Here Keeping the required parameter. can be used from config store.

opensearch_domain_endpoint = 'https://e0hc00i67ga6mpn1xkxa.us-east-1.aoss.amazonaws.com'
aws_region = 'us-east-1'
index_name = 'text_to_sql_index'
service = 'aoss'
region = 'us-east-1'
credentials = boto3.Session().get_credentials()

##auth = AWSV4SignerAuth(credentials, region, service)


awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
logger.info(awsauth)
i = 0


class EmbeddingBedrockOpenSearch:
    def __init__(self, domain, vector_name, fieldname):
        self.bedrock_client = Clientmodules.createBedrockRuntimeClient()
        self.language_model = LanguageModel(self.bedrock_client)
        print(self.language_model)
        self.llm = self.language_model.llm
        self.embeddings = self.language_model.embeddings
        self.opensearch_domain_endpoint = domain
        self.http_auth = awsauth
        self.vector_name = vector_name
        self.fieldname = fieldname

        logger.info("created for doamin " + domain)
        logger.info(credentials.access_key)

    def check_if_index_exists(self, index_name: str, region: str, host: str, http_auth: Tuple[str, str]) -> OpenSearch:
        hostname = host.replace("https://", "")

        logger.info(hostname)
        aos_client = OpenSearch(
            hosts=[{'host': hostname, 'port': 443}],
            http_auth=awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            timeout=300,
            ssl_show_warn=True
        )

        exists = aos_client.indices.exists(index_name)
        print("exist check", exists)
        return exists

    def add_documnets(self, index_name: str, file_name: str):
        documents = JSONLoader(file_path=file_name, jq_schema='.', text_content=False, json_lines=True).load()

        # Ensure metadata is in dictionary format to avoid validation errors
        for doc in documents:
            if isinstance(doc.metadata, str):
                doc.metadata = eval(doc.metadata)

        docs = OpenSearchVectorSearch.from_documents(embedding=self.embeddings,
                                                     opensearch_url=self.opensearch_domain_endpoint,
                                                     http_auth=self.http_auth,
                                                     documents=documents,
                                                     index_name=index_name,
                                                     engine="faiss")

        index_exists = self.check_if_index_exists(index_name,
                                                  aws_region,
                                                  self.opensearch_domain_endpoint,
                                                  self.http_auth)
        logger.info(index_exists)
        print(index_exists)
        if not index_exists:
            logger.info(f'index :{index_name} is not existing ')
            sys.exit(-1)
        else:
            logger.info(f'index :{index_name} Got created')

    def getDocumentfromIndex(self, index_name: str):
        try:
            logger.info("the opensearch_url is " + self.opensearch_domain_endpoint + "")
            logger.info(self.http_auth)
            ##http_auth = ('ll_vector','@')
            hostname = self.opensearch_domain_endpoint
            docsearch = OpenSearchVectorSearch(opensearch_url=hostname,
                                               embedding_function=self.embeddings,
                                               http_auth=self.http_auth,
                                               index_name=index_name,
                                               use_ssl=True,
                                               connection_class=RequestsHttpConnection
                                               )

            return docsearch
        except Exception:
            print(traceback.format_exc())

    def getSimilaritySearch(self, user_query: str, vcindex):
        docs = vcindex.similarity_search(user_query, k=200, vector_field=self.vector_name, text_field=self.fieldname)
        return docs

    def format_metadata(self, metadata):
        docs = []
        # Remove indentation and line feed
        for elt in metadata:
            processed = elt.page_content
            print(processed)
            chunk = elt.metadata['AMAZON_BEDROCK_TEXT_CHUNK']
            print(repr(chunk))
            for i in range(20, -1, -1):
                processed = processed.replace('\n' + ' ' * i, '')

            docs.append(processed)
        result = '\n'.join(docs)
        # Escape curly brackets
        result = result.replace('{', '{{')
        result = result.replace('}', '}}')
        return result

    def get_data(self, metadata):
        docs = []
        # Remove indentation and line feed
        for elt in metadata:
            chunk = elt.metadata['AMAZON_BEDROCK_TEXT_CHUNK']
            for i in range(20, -1, -1):
                chunk = chunk.replace('\n' + ' ' * i, '')
                chunk = chunk.replace('\r' + ' ' * i, '')
            docs.append(chunk)
        result = '\n'.join(docs)
        return result


def main(user_query):
    print('main() executed')
    index_name1 = 'text_to_sql_index'
    domain = 'https://e0hc00i67ga6mpn1xkxa.us-east-1.aoss.amazonaws.com'
    vector_field = 'embeddings_vector'
    fieldname = 'id'
    try:
        ebropen = EmbeddingBedrockOpenSearch(domain, vector_field, fieldname)
        ebropen.check_if_index_exists(index_name=index_name1, region='us-east-1', host=domain, http_auth=awsauth)

        vcindxdoc = ebropen.getDocumentfromIndex(index_name=index_name1)

        document = ebropen.getSimilaritySearch(user_query, vcindex=vcindxdoc)

        result = ebropen.get_data(document)

        print(result)
    except Exception as e:
        print(e)
        traceback.print_exc()

    logger.info(vcindxdoc)


if __name__ == '__main__':
    # Accept the user_query dynamically from the Jupyter Notebook or other sources
    user_query = sys.argv[1] if len(sys.argv) > 1 else 'default query'
    main(user_query)