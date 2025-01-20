from typing import List, Type, Union

from langchain_community.graphs.graph_document import GraphDocument
from langchain_core.documents import Document
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import Runnable, RunnableSerializable
from langchain_experimental.graph_transformers import LLMGraphTransformer
from langchain_neo4j import Neo4jGraph
from neo4j.exceptions import ClientError
from pydantic import BaseModel

CREATE_DB_QUERY = "CREATE DATABASE {kg_db_name}"

async def create_knowledge_graph_schema(docs: list[Document], llm, allowed_nodes: List[str], allowed_relationships: List[str], node_properties: List[str], relationship_properties: List[str]) -> list[GraphDocument]:
    """
    Converts a list of documents into graph documents using a language model.

    Parameters:
    docs (list[Document]): A list of Document objects to be converted.
    llm: The language model to use for conversion.
    allowed_nodes (List[str]): A list of allowed node types.
    allowed_relationships (List[str]): A list of allowed relationship types.
    node_properties (List[str]): A list of properties for nodes.
    relationship_properties (List[str]): A list of properties for relationships.

    Returns:
    list[GraphDocument]: A list of GraphDocument objects representing the knowledge graph schema.
    """
    graph_transformer = LLMGraphTransformer(llm=llm,
                                            allowed_nodes=allowed_nodes,
                                            allowed_relationships=allowed_relationships,
                                            node_properties=node_properties,
                                            relationship_properties=relationship_properties
                                            )
    return await graph_transformer.aconvert_to_graph_documents(docs)

def create_knowledge_graph(docs: list[GraphDocument], kg_url, kg_username, kg_password, kg_db_name) -> None:
    """
    Creates a knowledge graph in a Neo4j database from a list of graph documents.

    Parameters:
    docs (list[GraphDocument]): A list of GraphDocument objects to be added to the knowledge graph.
    kg_url: The URL of the Neo4j database.
    kg_username: The username for the Neo4j database.
    kg_password: The password for the Neo4j database.
    kg_db_name: The name of the Neo4j database.

    Returns:
    None
    """
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

prompt = ChatPromptTemplate.from_messages(
    [
        (
            "system", "You are a top-tier algorithm able to extract information from a document about the following entities: {entities}. Be clear and concise when filling the informations!"
                      "DO NOT FORGET TO INCLUDE ANY ENTITY THAT APPEARS IN THE DOCUMENT. BE SURE TO READ AND PARSE ALL THE DOC AND EXTRACT ALL THE INFO",
        ),
        (
            "human", "Given the following document:\n<document>\n{document}\n</document>\nSummarize and extract the needed information."
                     "For example, for a document like 'The pizza is a dish made of dough topped with tomato sauce and cheese and baked in an oven.' you should extract the entities 'piatto' (pizza), a list of 'ingrediente' (tomato sauce, cheese) and 'tecnica' (baked).",
        ),
    ]
)

def create_info_extraction_chain(llm, output_class: Type[BaseModel]) -> RunnableSerializable[dict, Union[dict, BaseModel]]:
    """
    Creates an information extraction chain using a language model and a specified output class.

    Parameters:
    llm: The language model to use for information extraction.
    output_class (Type[BaseModel]): The class type representing the structured output.

    Returns:
    RunnableSerializable[dict, Union[dict, BaseModel]]: A runnable chain for extracting information.
    Remember it should be invoked with document and entities, i.e. Node Label you will use in the graph
    Example:
    extraction_chain.invoke({"document": document, "entities": entities})
    """
    return prompt | llm.with_structured_output(output_class)