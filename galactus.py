import os.path
from typing import List, Optional, Union, Tuple

from langchain_core.documents import Document
from langchain_core.language_models import BaseChatModel
from langchain_core.output_parsers import JsonOutputParser, StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_neo4j.graphs.graph_document import GraphDocument
from pydantic import BaseModel, Field

import optimus_prime
import pdf_loader
from pdf_loader import PdfLoader


def clean_and_build_documents(documents: List[str],
                              llm: BaseChatModel = None,
                              directory_prefix: str = "",
                              include_titles: bool = False,
                              reorganize: bool = False,
                              summarize_all: bool = False,
                              summarize_info: bool = False,
                              summarize_paragraphs: bool = False,
                              additional_prompt: str = "") -> List[Document]:
    """
    Load documents into a knowledge graph.

    Parameters:
    documents (List[str]): A list of paths of the files to be loaded into the knowledge graph.

    Returns:
    List[str]: A list of documents to be loaded into the knowledge graph.
    """
    if documents is None or len(documents) == 0:
        raise ValueError("No documents to load.")

    docs_to_load = []

    for doc in documents:
        # Load the document into the knowledge graph
        file_name = os.path.basename(doc)
        doc_type = file_name.split('.')[1]

        match doc_type:
            case "pdf":
                print("Document type: PDF")
                # Load the PDF document
                doc_path = os.path.join(directory_prefix, doc)
                print(f"Sanitizing PDF: {doc_path} ...")
                santized_pdf = pdf_loader.sanitize_pdf(doc_path)
                loader = PdfLoader(
                    files=[santized_pdf]
                )
                print("Parsing PDF...")
                pdf_doc = loader.load_pdf_documents()
                print(f"Parsed PDF: {pdf_doc.json}")
                print("Getting document hierarchical representation...")
                hierarchical_json = pdf_loader.get_hierarchical_json_representation(pdf_doc.json, include_titles)
                print(f"Document hierarchical representation: {hierarchical_json}")
                if reorganize:
                    print("Reorganizing document...")
                    hierarchical_reorganized_json = reorganize_json(hierarchical_json, llm, summarize_all, summarize_info, summarize_paragraphs, additional_prompt)
                    print("Converting document to Langchain document...")
                    converted_docs = pdf_loader.convert_llmsherpa_dict_to_langchain_doc(hierarchical_reorganized_json, file_name)
                    print(f"Converted docs are: {[doc.model_dump_json() for doc in converted_docs]}")
                    docs_to_load.extend(converted_docs)
                    continue
                converted_docs = pdf_loader.convert_llmsherpa_dict_to_langchain_doc(hierarchical_json,
                                                                                    file_name)
                print(f"Converted docs are: {[doc.model_dump_json() for doc in converted_docs]}")
                docs_to_load.extend(converted_docs)
            case _:
                raise ValueError(f"Invalid document type for document {doc}. Only PDF documents are supported.")
    return docs_to_load

def reorganize_json(data: dict, llm: BaseChatModel = None, summarize_all: bool = False, summarize_info: bool = False, summarize_paragraphs: bool = False, additional_prompt: str = "") -> list:
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
        "</document>"
        "{additional_prompt}"
    )
    if additional_prompt != "":
        summarizer_prompt = summarizer_prompt.partial(additional_prompt=additional_prompt)
    summarizer_chain = summarizer_prompt | llm | StrOutputParser()
    # Extract the introduction with the first subkey only
    introduction_key, introduction_value = list(data.items())[0]
    introduction = {introduction_key: {list(introduction_value.items())[0][0]: list(introduction_value.items())[0][1]}}
    introduction_key, introduction_value = next(iter(introduction.items()))

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
        if type(introduction_value) == dict:
            if any(key in item for key in introduction_value.keys()):
                reorganized_data[i] = {
                    introduction_key: introduction_value
                }  # Completely replace the item with the introduction
        else:
            item[introduction_key] = introduction_value # Adding the introduction as part of each object
    print(reorganized_data)
    return reorganized_data

async def load_documents_into_knowledge_graph(documents: List[str],
                                        llm: BaseChatModel = None,
                                        directory_prefix: str = "",
                                        allowed_nodes: List[str] = [],
                                        allowed_relationships: Union[List[str], List[Tuple[str, str, str]]] = [],
                                        node_properties: Union[List[str]] | bool = False,
                                        relationship_properties: Union[List[str] | bool] = False,
                                        include_titles: bool = False,
                                        reorganize: bool = False,
                                        summarize_all: bool = False,
                                        summarize_info: bool = False,
                                        summarize_paragraphs: bool = False,
                                        additional_prompt: str = "") -> List[GraphDocument]:
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
    if not llm:
        raise ValueError("A language model is required for loading documents into a knowledge graph.")

    if documents is None or len(documents) == 0:
        raise ValueError("No documents to load.")

    print(f"Cleaning files: {documents}")
    docs_to_load = clean_and_build_documents(documents=documents,
                                             llm=llm,
                                             directory_prefix=directory_prefix,
                                             include_titles=include_titles,
                                             reorganize = reorganize,
                                             summarize_all = summarize_all,
                                             summarize_info = summarize_info,
                                             summarize_paragraphs = summarize_paragraphs,
                                             additional_prompt=additional_prompt)

    print(f"Cleaning completed. Documents to be loaded are: {[doc.model_dump_json() for doc in docs_to_load]}")
    print(f"Loading documents into knowledge graph schema...")
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