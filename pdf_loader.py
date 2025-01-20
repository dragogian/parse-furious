import json
import os.path
from typing import Optional, List, Union

import pikepdf
from langchain_core.documents import Document as LangchainDocument
from langchain_community.document_loaders.llmsherpa import LLMSherpaFileLoader
from llmsherpa.readers import LayoutPDFReader, Document as LLMSherpaDocument


def convert_llmsherpa_dict_to_langchain_doc(document: dict, filename: str) -> List[LangchainDocument]:
    """
    Converts an LLM Sherpa dict to a Langchain document.
        :param document:
        :return: List[LangchainDocument]
    """
    docs = []
    for item in document:
        langchain_doc = LangchainDocument(
            page_content=json.dumps(item),
            metadata={
                "source": filename
            }
        )
        docs.append(langchain_doc)
    return docs


def build_hierarchical_structure_langchain(data) -> dict:
    """
    Constructs a nested dictionary from a list of JSON objects.

    Parameters:
    data (list): A list of dictionaries containing document elements with levels and tags.

    Returns:
    dict: A hierarchical structure representing the document content.
    """
    structure = {}
    levels = {}

    for item in data:
        sentences = item['sentences']  # Extract text sentences
        level = item['level']  # Extract document hierarchy level
        tag = item['tag']  # Extract the type of content (header/para)

        if tag == 'header':
            # Store header values based on the level
            levels[level] = sentences[0]
        elif tag == 'para':
            parent = structure
            # Traverse the hierarchy based on levels
            for l in range(level):
                parent = parent.setdefault(levels[l], {})
            # Add sentences to the correct nested position
            parent.setdefault('sentences', []).extend(sentences)

    return structure


def build_hierarchical_structure_json(data: dict) -> dict:
    """
    Constructs a nested dictionary from a list of JSON objects.

    Parameters:
    data (list): A list of dictionaries containing document elements with levels and tags. Dictionary should be obtained from the LLM Sherpa pdf_reader.read_pdf -> .json API.

    Returns:
    dict: A hierarchical structure representing the document content.
    """
    structure = {}
    levels = {}

    for item in data:
        sentences = item['sentences']  # Extract text sentences
        level = item['level']  # Extract document hierarchy level
        tag = item['tag']  # Extract the type of content (header/para)

        if tag == 'header':
            # Store header values based on the level
            levels[level] = sentences[0]
        elif tag == 'para':
            parent = structure
            # Traverse the hierarchy based on levels
            for l in range(level):
                parent = parent.setdefault(levels[l], {})
            # Add sentences to the correct nested position
            #parent.setdefault('sentences', []).extend(sentences)
            parent.setdefault('text', "").join(sentences)

    return structure


def sanitize_pdf(file: str) -> str:
    """
    Sanitizes a PDF file by removing any non-UTF-8 characters or broken content.

    Parameters:
    file (str): The path to the PDF file to clean.

    Returns:
    str: The path to the cleaned PDF file.
    """
    cleaned_file = f"{os.path.splitext(file)[0]}_cleaned.pdf"
    with pikepdf.Pdf.open(file) as pdf:
        for page in pdf.pages:
            content = page.extract_text()
            if content:
                # Remove non-UTF-8 characters
                cleaned_content = content.encode('utf-8', 'ignore').decode('utf-8')
                page.contents = pikepdf.Stream(pdf, cleaned_content)
        pdf.save(cleaned_file)
    return cleaned_file


class PdfLoader:

    def __init__(self, files: List[str],
                 llmsherpa_api_url: Optional[str] = "http://localhost:5010/api/parseDocument?renderFormat=all",
                 apply_ocr: Optional[bool] = False,
                 new_indent_parser: Optional[bool] = False,
                 strategy: Optional[str] = "chunks",
                 provider: Optional[str] = "llmsherpa"):
        """
            Initializes the PdfLoader class.

            Parameters:
            files (List[str]): A list of PDF files to load.
            llmsherpa_api_url (str): The API URL for the LLM Sherpa service.
            apply_ocr (bool): Whether to apply OCR to the PDF files. Default is False.
            new_indent_parser (bool): Whether to use the new indent parser. Default is False.
            strategy (str): The strategy for splitting the PDF files. Options include "chunks" and "pages".
            provider (str): The provider of the PDF files. Default is "llmsherpa". Possible values are "llmsherpa" and "langchain".
        """
        self.files = files
        self.llmsherpa_api_url = llmsherpa_api_url
        self.apply_ocr = apply_ocr
        self.new_indent_parser = new_indent_parser
        self.strategy = strategy
        self.sherpaReader = LayoutPDFReader(llmsherpa_api_url)
        self.provider = provider

    def load_pdf_documents(self) -> Union[List[LLMSherpaDocument], List[LangchainDocument]]:
        """
            Loads and splits PDF documents into smaller chunks.

            Returns:
            List[Document]: A list of Document objects containing the split content of the PDF files.
        """
        docs = []
        if not self.files:
            return []
        for file in self.files:
            if os.path.basename(file).split('.')[-1] != 'pdf':
                raise ValueError(f'{file} is not a PDF file')
            match self.provider:
                case "langchain":
                    pdf_reader = LLMSherpaFileLoader(file_path=file,
                                                     new_indent_parser=self.new_indent_parser,
                                                     apply_ocr=self.apply_ocr,
                                                     strategy=self.strategy,
                                                     llmsherpa_api_url=self.llmsherpa_api_url)
                    docs.extend(pdf_reader.load())
                case "llmsherpa":
                    pdf_reader = self.sherpaReader
                    docs.extend(pdf_reader.read_pdf(file))
                case _:
                    raise ValueError(f"Unsupported provider: {self.provider}")
        return docs
