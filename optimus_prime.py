import json
import os
from typing import List, Type, Union, Optional

from langchain_community.graphs.graph_document import GraphDocument
from langchain_core.documents import Document
from langchain_core.language_models import BaseChatModel
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableSerializable
from langchain_experimental.graph_transformers import LLMGraphTransformer
from langchain_neo4j import Neo4jGraph
from neo4j.exceptions import ClientError
from pydantic import BaseModel

CREATE_DB_QUERY = "CREATE DATABASE {kg_db_name}"

async def create_knowledge_graph_schema(docs: list[Document], llm: BaseChatModel, allowed_nodes: List[str], allowed_relationships: List[str], node_properties: List[str], relationship_properties: List[str]) -> list[GraphDocument]:
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
                                            relationship_properties=relationship_properties)
    return await graph_transformer.aconvert_to_graph_documents(docs)

def create_knowledge_graph(docs: list[GraphDocument], kg_url: Optional[str] = None, kg_username: Optional[str] = None, kg_password: Optional[str] = None, kg_db_name: Optional[str] = None) -> None:
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
    if not (kg_url := os.environ.get("NEO4J_URI", kg_url)):
        raise ValueError("Neo4j URL not provided.")
    if not (kg_username := os.environ.get("NEO4J_USERNAME", kg_username)):
        raise ValueError("Neo4j username not provided.")
    if not (kg_password := os.environ.get("NEO4J_PASSWORD", kg_password)):
        raise ValueError("Neo4j password not provided.")
    if not (kg_db_name := os.environ.get("NEO4J_DB_NAME", kg_db_name)):
        raise ValueError("Neo4j database name not provided.")

    try:
        graph_db = Neo4jGraph(url=kg_url, username=kg_username, password=kg_password, database=kg_db_name)
    except ClientError as e:
        print("Database not found, creating a new one...")
        # Inizializza la connessione al database Neo4j
        base_graph_db = Neo4jGraph(url=kg_url, username=kg_username, password=kg_password)
        base_graph_db.query(CREATE_DB_QUERY.format(kg_db_name=kg_db_name))
        graph_db = Neo4jGraph(url=kg_url, username=kg_username, password=kg_password, database=kg_db_name)
    graph_db.add_graph_documents(docs)
    return

extraction_chain_prompt = ChatPromptTemplate.from_messages(
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

def create_info_extraction_chain(llm: BaseChatModel, output_class: Type[BaseModel]) -> RunnableSerializable[dict, Union[dict, BaseModel]]:
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
    return extraction_chain_prompt | llm.with_structured_output(output_class)


def json_reorganizer(data: dict, llm: BaseChatModel, summarize_all: bool = False, summarize_info: bool = False, summarize_paragraphs: bool = False) -> list:
    """
    Reorganizes a JSON object into a list of dictionaries, each containing a key-value pair from the original object. The first key-value pair is extracted and used as an introduction for each object. Only the first subkey of the first object is used as introduction is used.

    Params
    data: The JSON object to reorganize
    llm: The language model to use for summarization. Must be a Langchain ChatModel
    summarize_all: Whether to summarize all the text
    summarize_info: Whether to summarize the text for information extraction
    summarize_paragraphs: Whether to summarize the text for paragraph extraction

    Returns: A list of dictionaries, each containing a key-value pair from the original object
    """
    summarizer_prompt = ChatPromptTemplate.from_template(
        "You are a top-tier algorithm able to summarize the text. Be clear and concise when summarizing the text. Extract all the relevant information as they will be used to construct a graph DB."
        "Here is the text to summarize:"
        "<document>"
        "{document}"
        "</document> "
    )
    summarizer_chain = summarizer_prompt | llm | StrOutputParser()
    # Extract the introduction with the first subkey only
    introduction_key, introduction_value = list(data.items())[0]
    introduction = {introduction_key: {list(introduction_value.items())[0][0]: list(introduction_value.items())[0][1]}}
    introduction_key, introduction_value = introduction

    if (summarize_info or summarize_all or summarize_paragraphs) and not llm:
        raise ValueError("A language model is required to summarize the text.")

    if summarize_info or summarize_all:
        summarized_info = summarizer_chain.invoke({"document": introduction_value})
        introduction_value = summarized_info

    def recursive_reorganize(data: dict, parent_key: str):
        result = []
        for key, value in data.items():
            if isinstance(value, dict):  # If the value is a nested dictionary
                result.extend(recursive_reorganize(value, key))  # Recurse and add the results
            elif isinstance(value, str):
                if summarize_info or summarize_all:
                    summarized_text = summarizer_chain.invoke({"document": value})
                    result.append({key: summarized_text})
                else:
                    result.append({parent_key: value})  # Add the text field with its key
        return result

    # Add the introduction to each reorganized object
    reorganized_data = recursive_reorganize(data, introduction_key)
    for i, item in enumerate(reorganized_data):
        if any(key in item for key in introduction_value.keys()):
            reorganized_data[i] = {
                introduction_key: introduction_value
            }  # Completely replace the item with the introduction
        else:
            item[introduction_key] = introduction_value # Adding the introduction as part of each object
    print(reorganized_data)
    return reorganized_data

