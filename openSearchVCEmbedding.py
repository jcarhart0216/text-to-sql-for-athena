import sys
import time
import traceback

from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection

from typing import List, Tuple
import logging
import numpy as np
import boto3

from langchain_community.vectorstores import OpenSearchVectorSearch
from langchain_community.document_loaders import JSONLoader

from boto_client import Clientmodules
from llm_basemodel import LanguageModel

logger = logging.getLogger()

# Required parameters, can be used from config store.
opensearch_domain_endpoint = 'https://e0hc00i67ga6mpn1xkxa.us-east-1.aoss.amazonaws.com'
aws_region = 'us-east-1'
index_name = 'text_to_sql_index'

service = 'aoss'
region = 'us-east-1'
credentials = boto3.Session().get_credentials()

awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
logger.info(awsauth)


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

        logger.info("created for domain " + domain)
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

    def add_documents(self, index_name: str, file_name: str):
        documents = JSONLoader(file_path=file_name, jq_schema='.', text_content=False, json_lines=True).load()

        # Ensure metadata is in dictionary format to avoid validation errors
        for doc in documents:
            if isinstance(doc.metadata, str):
                try:
                    doc.metadata = eval(doc.metadata)
                except Exception as e:
                    logger.error(f"Failed to convert metadata to dictionary for doc: {doc}. Error: {e}")
                    doc.metadata = {}

            # Ensure that the metadata is a dictionary
            if not isinstance(doc.metadata, dict):
                doc.metadata = {}

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
        for elt in metadata:
            processed = elt.page_content
            print(processed)
            chunk = elt.metadata.get('AMAZON_BEDROCK_TEXT_CHUNK', '')
            print(repr(chunk))
            for i in range(20, -1, -1):
                processed = processed.replace('\n' + ' ' * i, '')
            docs.append(processed)
        result = '\n'.join(docs)
        result = result.replace('{', '{{')
        result = result.replace('}', '}}')
        return result

    def get_data(self, metadata):
        docs = []
        for elt in metadata:
            chunk = elt.metadata.get('AMAZON_BEDROCK_TEXT_CHUNK', '')
            for i in range(20, -1, -1):
                chunk = chunk.replace('\n' + ' ' * i, '')
                chunk = chunk.replace('\r' + ' ' * i, '')
            docs.append(chunk)
        result = '\n'.join(docs)
        return result


def main():
    print('main() executed')
    index_name1 = 'text_to_sql_index'
    domain = 'https://e0hc00i67ga6mpn1xkxa.us-east-1.aoss.amazonaws.com'
    vector_field = 'embeddings_vector'
    fieldname = 'id'
    try:
        ebropen = EmbeddingBedrockOpenSearch(domain, vector_field, fieldname)
        ebropen.check_if_index_exists(index_name=index_name1, region='us-east-1', host=domain, http_auth=awsauth)

        vcindxdoc = ebropen.getDocumentfromIndex(index_name=index_name1)

        user_query = 'show me all the titles in US region'
        document = ebropen.getSimilaritySearch(user_query, vcindex=vcindxdoc)

        result = ebropen.get_data(document)

        print(result)
    except Exception as e:
        print(e)
        traceback.print_exc()

    logger.info(vcindxdoc)


if __name__ == '__main__':
    main()
