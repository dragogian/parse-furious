import json

from langchain_community.document_loaders import LLMSherpaFileLoader

from pdf_loader import get_hierarchical_json_representation

pdf_url = './L infinito in un Boccone_cleaned.pdf'
from llmsherpa.readers import LayoutPDFReader
llmsherpa_api_url = "http://localhost:5010/api/parseDocument?renderFormat=all"

#---------------------LLMSHERPA VERSION-------------------------

# pdf_reader = LayoutPDFReader(llmsherpa_api_url)
# doc = pdf_reader.read_pdf(pdf_url)
#
# # Open the file in write mode and save the JSON data - Uncomment to save json file
# # with open('parsed_pdf.json', 'w') as json_file:
# #     json.dump(doc.json, json_file, indent=4)
# with open('output/output_sherpa.json', 'w') as json_file:
#     json.dump(doc.json, json_file, indent=4)
# hierarchical_structure = build_hierarchical_structure_json(doc.json)
# with open('output/hierarchical_new_pdf.json', 'w') as json_file:
#     json.dump(hierarchical_structure, json_file, indent=4)
#---------------------LANGCHAIN VERSION-------------------------

# pdf_reader = LLMSherpaFileLoader(file_path=pdf_url, llmsherpa_api_url=llmsherpa_api_url, strategy="sections")
# docs = pdf_reader.load()
# json_obj = {}
# for index, doc in enumerate(docs):
#     json_doc = json.loads(doc.model_dump_json().replace("      ", ""))
#     json_obj[f"{json_doc["metadata"]["section_title"]}"]=json_doc
# with open('output/langchain_pdf.json', 'w') as json_file:
#     json.dump(json_obj, json_file, indent=4)


import json


def transform_to_hierarchical_json(input_file_, output_file_):
    with open(input_file_, 'r', encoding='utf-8') as f:
        data = json.load(f)

    hierarchical_data = get_hierarchical_json_representation(data, include_titles=False)

    with open(output_file_, 'w', encoding='utf-8') as f:
        json.dump(hierarchical_data, f, ensure_ascii=False, indent=4)

# Example usage
input_file = 'output/output_sherpa.json'
output_file = 'output/hierarchical_prova_pdf.json'
transform_to_hierarchical_json(input_file, output_file)