import asyncio
import json
import os
from typing import List

from dotenv import load_dotenv
from langchain_community.document_loaders import PyPDFLoader

from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field

from pdf_loader import PdfLoader, build_flat_json

load_dotenv()
llm= ChatOpenAI(model="gpt-4o", temperature=0)

prompt = ChatPromptTemplate.from_messages([
    ("system", "You are a top tier NER algorithm capable of extracting entities from a document. These are the named entities you can extract:\n<entities>\n{entities}\n</entities>."
               "Pay particular attention to not confuse entities. For example, ingredients only refer to food used to make dishes!"
               "Do not try to infer entities that are not expressly mentioned in the text."
               "Keep into account that you can receive standalone piece of text to analyze, so it's highly possible that text won't contain all entities.  For example, you could receive a piece of text explaining a dish without mentioning the restaurant or the chef."
               "If a entity is not present in the text, just don't include it in the output."
               "Here you have already extracted entities from the document:\n<already_extracted_entities>\n{already_extracted_entities}\n</already_extracted_entities>."
               "You can add more entities where they are array, but you CANNOT modify or remove entities that are already present."),
    ("user", "Extract the named entities from this document:\n<document>\n{document}\n</document>")]
)

class License(BaseModel):
    Name: str = Field(..., description="The names of the licenses the chef has acquired")
    Level: str = Field(..., description="The level of the license")

class Dish(BaseModel):
    Name: str = Field(..., description="The name of the dish")
    Ingredients: List[str] = Field(..., description="The names of the ingredients needed to cook the dish")
    Techniques: List[str] = Field(..., description="The names of the techniques")

class Chef(BaseModel):
    Name: str = Field(..., description="The name of the chef")
    Licenses: List[License]

class EntityContainer(BaseModel):
    Restaurant: str = Field(..., description="The name of the restaurant")
    Chef: Chef
    Dishes: List[Dish]
    Planet: str = Field(..., description="The name of the planet")

chain = prompt | llm.with_structured_output(EntityContainer)
metadata=[]
entities_list=["Restaurant", "Chef", "Dish", "Ingredient", "Technique", "License", "Planet"]



def extract_entities(document, already_extracted_entities):
    results = chain.invoke({'document': document['page_content'], 'entities': entities_list, 'already_extracted_entities': already_extracted_entities.model_dump_json()})
    return results


def merge_restaurant_objects(objs: list[EntityContainer]) -> dict:
    final_obj = {
        "Restaurant": "",
        "Chef": {
            "Name": "",
            "Licenses": []
        },
        "Dishes": [],
        "Planet": ""
    }
    for obj_ in objs:
        obj = obj_.model_dump()
        # --- Merge Restaurant ---
        restaurant = obj.get("Restaurant", "").strip()
        if restaurant:
            if not final_obj["Restaurant"]:
                final_obj["Restaurant"] = restaurant
            else:
                # If both are non-empty, choose the longer (more descriptive) one.
                if len(restaurant) > len(final_obj["Restaurant"]):
                    final_obj["Restaurant"] = restaurant

        # --- Merge Chef Name ---
        chef_name = obj.get("Chef", {}).get("Name", "").strip()
        if chef_name:
            if not final_obj["Chef"]["Name"]:
                final_obj["Chef"]["Name"] = chef_name
            else:
                # If the current name is "Unknown" or empty, or if the new name is longer, update it.
                current = final_obj["Chef"]["Name"].lower()
                if current in ["", "unknown"] and chef_name.lower() not in ["", "unknown"]:
                    final_obj["Chef"]["Name"] = chef_name
                elif chef_name.lower() not in ["", "unknown"] and len(chef_name) > len(final_obj["Chef"]["Name"]):
                    final_obj["Chef"]["Name"] = chef_name

        # --- Merge Chef Licenses (union) ---
        for lic in obj.get("Chef", {}).get("Licenses", []):
            if lic not in final_obj["Chef"]["Licenses"]:
                final_obj["Chef"]["Licenses"].append(lic)

        # --- Merge Dishes ---
        # Only add a dish if both its Ingredients and Techniques lists are non-empty.
        for dish in obj.get("Dishes", []):
            ingredients = dish.get("Ingredients", [])
            techniques = dish.get("Techniques", [])
            if not ingredients or not techniques:
                # Skip dish if either list is empty
                continue

            dish_name = dish.get("Name", "").strip()
            # Check if a dish with the same name already exists (case-insensitive)
            found = False
            for existing in final_obj["Dishes"]:
                if existing.get("Name", "").strip().lower() == dish_name.lower():
                    # Merge by unioning ingredients, techniques, and licenses.
                    for ing in ingredients:
                        if ing not in existing["Ingredients"]:
                            existing["Ingredients"].append(ing)
                    for tech in techniques:
                        if tech not in existing["Techniques"]:
                            existing["Techniques"].append(tech)
                    for lic in dish.get("Licenses", []):
                        if lic not in existing["Licenses"]:
                            existing["Licenses"].append(lic)
                    found = True
                    break
            if not found:
                final_obj["Dishes"].append(dish)

        # --- Merge Planet ---
        planet = obj.get("Planet", "").strip()
        if planet:
            if not final_obj["Planet"]:
                final_obj["Planet"] = planet
            else:
                # If different, choose the one with more characters (assuming it is more complete)
                if len(planet) > len(final_obj["Planet"]):
                    final_obj["Planet"] = planet

    return final_obj

async def handle_file(path):
    loader = PdfLoader([path], provider='llmsherpa')
    sherpa_doc = loader.load_pdf_documents()

    docs = build_flat_json(sherpa_doc)
    path_base_name = os.path.basename(path).split(".")[0]
    with open(f'output/flat_{path_base_name}.json', 'w', encoding='utf-8') as f:
        json.dump(docs, f, ensure_ascii=False, indent=4)
    extracted_entities = EntityContainer(Restaurant="", Chef=Chef(Name="", Licenses=[]), Dishes=[], Planet="")
    for doc in docs.values():
        extracted_entities = extract_entities(doc, extracted_entities)
    with open(f'output/metadata_{path_base_name}.json', 'a+', encoding='utf-8') as f:
        json.dump(extracted_entities.model_dump(), f, ensure_ascii=False, indent=4)
        f.write("\n")

    # final_obj = merge_restaurant_objects(metadata)

    # with open('output/merged_prova_pdf.json', 'a+', encoding='utf-8') as f:
    #     json.dump(final_obj, f, ensure_ascii=False, indent=4)

async def main():
    file_list = os.listdir("cleaned_resources")
    pdf_paths = [os.path.join("cleaned_resources", file) for file in file_list]
    tasks = [
        asyncio.create_task(handle_file(path))
        for path in pdf_paths
    ]
    results = await asyncio.gather(*tasks)
    return results

if __name__ == "__main__":
    asyncio.run(main())