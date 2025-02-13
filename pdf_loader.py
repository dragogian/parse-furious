import json
import os.path
from typing import Optional, List, Union

import pikepdf
from langchain_core.documents import Document as LangchainDocument
from langchain_community.document_loaders.llmsherpa import LLMSherpaFileLoader
from llmsherpa.readers import LayoutPDFReader, Document as LLMSherpaDocument

CLEANED_PDF_PREFIX = "cleaned_resources/"

def convert_llmsherpa_dict_to_langchain_doc(document: Union[dict, List], filename: str) -> List[LangchainDocument]:
    """
    Converts an LLM Sherpa dict to a Langchain document.

    Params
    document: Union[dict, List]
    filename: str

    Returns
    List[LangchainDocument]
    """
    docs = []
    if type(document) == dict:
        for item in document.items():
            langchain_doc = LangchainDocument(
                page_content=json.dumps(item, indent=4, ensure_ascii=False),
                metadata={
                    "source": filename
                }
            )
            docs.append(langchain_doc)
        return docs
    else:
        for item in document:
            langchain_doc = LangchainDocument(
                page_content=json.dumps(item, indent=4, ensure_ascii=False),
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

def remove_duplicates(data, parent_key=None):
    """
    Recursively remove from each dictionary the key that matches its own name
    in the parent dictionary.
    """
    if isinstance(data, dict):
        cleaned = {}
        for k, v in data.items():
            cleaned[k] = remove_duplicates(v, k)  # recurse, passing current key as parent_key

        # remove the key matching the parent's key if it exists
        if parent_key in cleaned:
            del cleaned[parent_key]
        return cleaned

    elif isinstance(data, list):
        return [remove_duplicates(item, parent_key) for item in data]

    return data

def __build_hierarchy_json(data: dict) -> dict:
    """
    Constructs a nested dictionary from a list of JSON objects.

    Parameters:
    data (list): A list of dictionaries containing document elements with levels and tags. Dictionary should be obtained from the LLM Sherpa pdf_reader.read_pdf -> .json API.

    Returns:
    dict: A hierarchical structure representing the document content.
    """
    structure = {}
    levels = {}
    titles = []
    for item in data:
        sentences = item.get('sentences', "")  # Extract text sentences
        level = item.get('level', 0)  # Extract document hierarchy level
        tag = item.get('tag', 'para')  # Extract the type of content (header/para)

        if level == 0 and tag == 'header':
            titles.append(sentences[0])  # Store all level 0 headers

        if tag == 'header':
            # Store header values based on the level
            levels[level] = sentences[0]
        elif tag == 'para':
            parent = structure
            # Traverse the hierarchy based on levels
            for l in range(level):
                parent = parent.setdefault(levels[l], {})
            # Concatenate sentences and store as text
            if 'text' in parent:
                parent['text'] += '. ' + '. '.join(sentences)
            else:
                parent['text'] = '. '.join(sentences)
    return structure

def __build_hierarchy_json_with_titles(data: dict) -> dict:
    """
    Constructs a nested dictionary from a list of JSON objects.

    Parameters:
    data (list): A list of dictionaries containing document elements with levels and tags. Dictionary should be obtained from the LLM Sherpa pdf_reader.read_pdf -> .json API.

    Returns:
    dict: A hierarchical structure representing the document content.
    """
    structure = {}
    levels = {}
    main_title = None

    for item in data:
        sentences = item['sentences']  # Extract text sentences
        level = item['level']  # Extract document hierarchy level
        tag = item['tag']  # Extract the type of content (header/para)

        if level == 0 and tag == 'header':
            if main_title is None:
                main_title = sentences[0]  # Set the first level 0 header as main title

        if tag == 'header':
            # Store header values based on the level
            levels[level] = sentences[0]
        elif tag == 'para':
            parent = structure
            # Traverse the hierarchy based on levels
            for l in range(level):
                if l in levels:
                    parent = parent.setdefault(levels[l], {})
            # Concatenate sentences and store as text
            if 'text' in parent:
                parent['text'] += '. ' + '. '.join(sentences)
            else:
                parent['text'] = '. '.join(sentences)
            if main_title:
                parent['document_title'] = main_title  # Add main title to every object
            for l in range(level + 1):
                if l in levels and levels[l] not in parent:
                    parent[levels[l]] = levels[l]  # Add all titles up to the current level

    return remove_duplicates(structure)

def build_flat_json(data: LLMSherpaDocument) -> dict:
    """
    Constructs a flat dictionary from a list of JSON objects.

    Parameters:
    data (list): A list of dictionaries containing document elements with levels and tags.

    Returns:
    dict: A flat dictionary representing the document content.
    """
    result = {}
    current_section_key = None
    section_number = 0
    pending_list_items = []  # Hold list items until we flush them (or use them as table headers)

    for block in data.json:
        tag = block.get('tag', '')
        level = block.get('level', 0)
        # Join the sentences using newline characters to preserve any embedded newlines.
        block_text = "\n".join(block.get('sentences', []))

        # If this is a header AND its level is 0 or 1, then start a new section.
        if tag == "header" and level <= 1:
            # Flush any pending list items if they exist.
            if pending_list_items and current_section_key is not None:
                list_text = "\n" + "\n".join(["- " + item for item in pending_list_items])
                result[current_section_key]["page_content"] += "\n\n" + list_text
                pending_list_items = []
            # Create a new section.
            section_key = block_text.replace(" ", "").replace("\\", "").replace("'", "")
            result[section_key] = {
                "id": None,
                "metadata": {
                    "source": "./resources/demo/L infinito in un Boccone_cleaned.pdf",
                    "section_number": section_number,
                    "section_title": block_text
                },
                "page_content": block_text,  # start with the header text
                "type": "Document"
            }
            current_section_key = section_key
            section_number += 1

        # If this is a header with level greater than 1, treat it like a paragraph.
        elif tag == "header" and level > 1:
            if current_section_key is not None:
                result[current_section_key]["page_content"] += "\n\n" + block_text
            else:
                # If no section has been created yet, create a default section.
                default_key = "DefaultSection"
                if default_key not in result:
                    result[default_key] = {
                        "id": None,
                        "metadata": {
                            "source": "./resources/demo/L infinito in un Boccone_cleaned.pdf",
                            "section_number": section_number,
                            "section_title": default_key
                        },
                        "page_content": "",
                        "type": "Document"
                    }
                    current_section_key = default_key
                    section_number += 1
                result[current_section_key]["page_content"] += "\n\n" + block_text

        elif tag == "list_item":
            # Save list items for later flush or for use as table header.
            pending_list_items.append(block_text)

        elif tag == "table":
            table_content = ""
            # If there are pending list items, assume these are column headers.
            if pending_list_items:
                header_line = " | ".join(pending_list_items)
                table_content += header_line + "\n"
                pending_list_items = []
            # Process table rows if present.
            if "table_rows" in block:
                for row in block["table_rows"]:
                    cells = [cell.get("cell_value", "") for cell in row.get("cells", [])]
                    row_text = " | ".join(cells)
                    table_content += row_text + "\n"
            # If the table block itself contains text, prepend it.
            if block_text:
                table_content = block_text + "\n" + table_content
            if current_section_key is not None:
                result[current_section_key]["page_content"] += "\n\n" + table_content.strip()
            else:
                # Optionally create a default section.
                default_key = "DefaultSection"
                if default_key not in result:
                    result[default_key] = {
                        "id": None,
                        "metadata": {
                            "source": "./resources/demo/L infinito in un Boccone_cleaned.pdf",
                            "section_number": section_number,
                            "section_title": default_key
                        },
                        "page_content": "",
                        "type": "Document"
                    }
                    current_section_key = default_key
                    section_number += 1
                result[current_section_key]["page_content"] += "\n\n" + table_content.strip()

        else:
            # For any other block (like a para), flush pending list items if any.
            if pending_list_items and current_section_key is not None:
                list_text = "\n" + "\n".join(["- " + item for item in pending_list_items])
                result[current_section_key]["page_content"] += "\n\n" + list_text
                pending_list_items = []
            if current_section_key is not None:
                result[current_section_key]["page_content"] += "\n\n" + block_text
            else:
                # Create a default section if needed.
                default_key = "DefaultSection"
                if default_key not in result:
                    result[default_key] = {
                        "id": None,
                        "metadata": {
                            "source": "./resources/demo/L infinito in un Boccone_cleaned.pdf",
                            "section_number": section_number,
                            "section_title": default_key
                        },
                        "page_content": "",
                        "type": "Document"
                    }
                    current_section_key = default_key
                    section_number += 1
                result[current_section_key]["page_content"] += "\n\n" + block_text

    # Flush any remaining list items at the end.
    if pending_list_items and current_section_key is not None:
        list_text = "\n" + "\n".join(["- " + item for item in pending_list_items])
        result[current_section_key]["page_content"] += "\n\n" + list_text

    return result

def clean_non_utf8_characters(text):
    """Remove non-UTF-8 characters."""
    return text.encode('utf-8', 'ignore').decode('utf-8')

def sanitize_pdf(file: str, prefix: Optional[str] = CLEANED_PDF_PREFIX) -> str:
    """
    Sanitizes a PDF file by removing any non-UTF-8 characters or broken content.

    Parameters:
    file (str): The path to the PDF file to clean.
    prefix (str): The prefix to add to the cleaned PDF file. I.e. the directory where the cleaned PDF file will be saved. Default is "cleaned_resources/".

    Returns:
    str: The path to the cleaned PDF file.
    """
    if not os.path.exists(prefix):
        os.makedirs(prefix)
    cleaned_file = f"{prefix}{os.path.basename(file).split('.')[0]}_cleaned.pdf"
    with pikepdf.Pdf.open(file) as pdf:
        for page in pdf.pages:
            page.remove_unreferenced_resources()
            if '/Contents' in page:
                # Extract raw content from the PDF (as bytes)
                content_bytes = page['/Contents'].read_bytes()
                content_text = content_bytes.decode('utf-8', 'ignore')  # Decode ignoring errors

                # Remove non-UTF-8 characters
                cleaned_content = clean_non_utf8_characters(content_text)

                # Convert cleaned content back to bytes
                cleaned_content_bytes = cleaned_content.encode('utf-8')

                # Replace the page contents with the cleaned stream
                page['/Contents'] = pikepdf.Stream(pdf, cleaned_content_bytes)
        pdf.save(cleaned_file)
    return cleaned_file

def get_hierarchical_json_representation(data:dict, include_titles: bool = False) -> dict:
    """
    Get a hierarchical JSON representation of the document content.

    Parameters:
    data (dict): The document content.
    include_titles (bool): Whether to include titles in the JSON representation. Default is False.

    Returns:
    dict: A hierarchical JSON representation of the document content.
    """
    if include_titles:
        return __build_hierarchy_json_with_titles(data)
    return __build_hierarchy_json(data)

def build_llmsherpa_api_url(llmsherpa_api_url: str, apply_ocr: Optional[bool] = False, new_indent_parser: Optional[bool] = False) -> str:
    """
    Builds the URL for the LLM Sherpa API.

    Parameters:
    file (str): The path to the PDF file.
    llmsherpa_api_url (str): The API URL for the LLM Sherpa service.

    Returns:
    str: The URL for the LLM Sherpa API.
    """
    apply_ocr = "&applyOcr=yes" if apply_ocr else ""
    new_indent_parser = "&useNewIndentParser=yes" if new_indent_parser else ""
    return f"{llmsherpa_api_url}{apply_ocr}{new_indent_parser}"

class PdfLoader:

    def __init__(self, files: List[str],
                 llmsherpa_api_url: Optional[str] = "http://localhost:5010/api/parseDocument?renderFormat=all",
                 apply_ocr: Optional[bool] = False,
                 new_indent_parser: Optional[bool] = False,
                 strategy: Optional[str] = "sections",
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
        self.llmsherpa_api_url = build_llmsherpa_api_url(llmsherpa_api_url, apply_ocr, new_indent_parser)
        self.apply_ocr = apply_ocr
        self.new_indent_parser = new_indent_parser
        self.strategy = strategy
        self.sherpaReader = LayoutPDFReader(llmsherpa_api_url)
        self.provider = provider

    def load_pdf_documents(self) -> Union[LLMSherpaDocument, List[LangchainDocument]]:
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
                                                     llmsherpa_api_url=self.llmsherpa_api_url,
                                                     )
                    docs.extend(pdf_reader.load())
                    return docs
                case "llmsherpa":
                    pdf_reader = self.sherpaReader
                    return pdf_reader.read_pdf(file)
                case _:
                    raise ValueError(f"Unsupported provider: {self.provider}")