from typing import List

from langchain_community.graphs.graph_document import GraphDocument
from langchain_core.documents import Document
from langchain_experimental.graph_transformers import LLMGraphTransformer
from langchain_neo4j import Neo4jGraph
from neo4j.exceptions import ClientError

CREATE_DB_QUERY = "CREATE DATABASE {kg_db_name}"

async def create_knowledge_graph_schema(docs: list[Document], llm, allowed_nodes: List[str], allowed_relationships: List[str], node_properties: List[str], relationship_properties: List[str]) -> list[GraphDocument]:
    graph_transformer = LLMGraphTransformer(llm=llm,
                                            allowed_nodes=allowed_nodes,
                                            allowed_relationships=allowed_relationships,
                                            node_properties=node_properties,
                                            relationship_properties=relationship_properties
                                            )
    return await graph_transformer.aconvert_to_graph_documents(docs)

def create_knowledge_graph(docs: list[GraphDocument], kg_url, kg_username, kg_password, kg_db_name) -> None:
    try:
        graph_db = Neo4jGraph(url=kg_url, username=kg_username, password=kg_password, database=kg_db_name)
    except ClientError as e:
        print("Database not found, creating a new one...")
        # Inizializza la connessione al database Neo4j
        base_graph_db = Neo4jGraph(url=kg_url, username=kg_username, password=kg_password)
        base_graph_db.query(CREATE_DB_QUERY.format(kg_db_name))
        graph_db = Neo4jGraph(url=kg_url, username=kg_username, password=kg_password, database=kg_db_name)
    graph_db.add_graph_documents(docs)
    return
