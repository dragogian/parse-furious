import os.path
from typing import List, Optional, Union

from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_neo4j.graphs.graph_document import GraphDocument
from pydantic import BaseModel, Field

import optimus_prime
import pdf_loader
from pdf_loader import PdfLoader


def clean_and_build_documents(documents: List[str]):
    """
    Load documents into a knowledge graph.

    Parameters:
    documents (List[str]): A list of paths of the files to be loaded into the knowledge graph.

    Returns:
    List[str]: A list of documents to be loaded into the knowledge graph.
    """
    if documents is None or len(documents):
        raise ValueError("No documents to load.")

    docs_to_load = []

    for doc in documents:
        # Load the document into the knowledge graph
        file_name = os.path.basename(doc)
        doc_type = file_name.split('.')[0]

        match doc_type:
            case "pdf":
                # Load the PDF document
                santized_pdf = pdf_loader.sanitize_pdf(doc)
                loader = PdfLoader(
                    files=[santized_pdf]
                )
                pdf_doc = loader.load_pdf_documents()
                hierarchical_json = pdf_loader.build_hierarchical_structure_json(pdf_doc.json)
                docs_to_load.extend(pdf_loader.build_hierarchical_structure_json(hierarchical_json))
            case _:
                raise ValueError("Invalid document type. Only PDF documents are supported.")
    return docs_to_load

async def load_documents_into_knowledge_graph(documents: List[str],
                                        llm,
                                        allowed_nodes: Optional[List[str]],
                                        allowed_relationships=Optional[List[str]],
                                        node_properties=Optional[Union[List[str]] | bool],
                                        relationship_properties=Optional[Union[List[str]] | bool]) -> List[GraphDocument]:
    """
    Load documents into a knowledge graph.

    Parameters:
    documents (List[str]): A list of paths of the files to be loaded into the knowledge graph.
    llm: The language model to use for conversion.
    allowed_nodes (List[str]): A list of allowed node types.
    allowed_relationships (List[str]): A list of allowed relationship types.
    node_properties (List[str]): A list of properties for nodes.
    relationship_properties (List[str]): A list of properties for relationships.

    Returns:
    None
    """
    if documents is None or len(documents):
        raise ValueError("No documents to load.")

    docs_to_load = clean_and_build_documents(documents)
    graph_schema = await optimus_prime.create_knowledge_graph_schema(docs=docs_to_load,
                                                                     llm=llm,
                                                                     allowed_nodes=allowed_nodes,
                                                                     allowed_relationships=allowed_relationships,
                                                                     node_properties=node_properties,
                                                                     relationship_properties=relationship_properties)
    return graph_schema

class ERModel(BaseModel):
    """
    The ERModel class represents the structure of the ER model.
    '''
    """
    entities: List[str] = Field(
        ...,
        description="The list of possible values for entities in the ER model.",
    )
    relationships: List[str] = Field(
        ...,
        description="The list of possible values for relationships in the ER model.",
    )
    entity_property: Optional[List[str]] = Field(
        None,
        description="The list of relevant properties for entities to be stored in the ER model.",
    )
    relationship_property: Optional[List[str]] = Field(
        None,
        description="The list of relevant properties for relationships to be stored in the ER model.",
    )

def has_method(obj, method_name):
    """
    Check if an object has a given method.

    Args:
        obj: The object to check.
        method_name (str): The name of the method to check for.

    Returns:
        bool: True if the method exists, False otherwise.
    """
    return callable(getattr(obj, method_name, None))

def get_entities_from_document(document: str, llm) -> Union[BaseModel, dict]:
    """
    Extract entities from a document.
    :param document: The document to extract entities from.
    :param llm: The language model to use for extraction.

    Returns:
    BaseModel: The extracted entities from the document.
    """
    if has_method(llm, "with_structured_output"):
        prompt = ChatPromptTemplate.from_template(
            "You are a top tier algorithm capable of extracting information from a text document and modelling it a document into entities and relationships."
            "Given the following document:"
            "<document>"
            "{document}"
            "</document>"
            "Extract all the relevant information to be sure that the ER model is correctly built and can properly map the document.")
        structured_llm = llm.with_structured_output(ERModel)
        entities_creation_chain = prompt | structured_llm.invoke({"document": document})
    else:
        json_struct = {field_name: {
            "description": field_info.description,
            "type": field_info.annotation
        } for field_name, field_info in ERModel.model_fields.items()}
        prompt = ChatPromptTemplate.from_template(
            "You are a top tier algorithm capable of extracting information from a text document and modelling it a document into entities and relationships."
            "Given the following document:"
            "<document>"
            "{document}"
            "</document>"
            "Extract all the relevant information to be sure that the ER model is correctly built and can properly map the document."
            "Always answer with only the relevant information and in JSON format. DO NOT INVENT OR ADD ANY ADDITIONAL INFORMATION EXCEPT FOR THE JSON OUTPUT"
            "JSON should follow the following format:"
            "<format>"
            "{json_struct}"
            "</format>").partial(json_struct=json_struct)
        entities_creation_chain = prompt | llm.invoke({"document": document}) | JsonOutputParser()
    return entities_creation_chain