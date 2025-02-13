import asyncio
import os

from dotenv import load_dotenv

import galactus
import optimus_prime
from resources.demo.llm import llm

load_dotenv()

graph_params = {
  "allowed_nodes": [
    "Piatto",
    "Ingrediente",
    "Tecnica",
    "Ristorante",
    "Pianeta",
    "Chef",
    "Licenza",
    "Ordine"
  ],
  "allowed_relationships": [
    ("Piatto", "CONTIENE_INGREDIENTE", "Ingrediente"),
    ("Piatto", "APPLICA_TECNICA", "Tecnica"),
    ("Piatto", "SERVITO_IN", "Ristorante"),
    ("Piatto", "PREPARATO_DA", "Chef"),
    ("Ristorante", "LOCALIZZATO_SU", "Pianeta"),
    ("Chef", "HA_LICENZA", "Licenza"),
    ("Chef", "LAVORA_IN", "Ristorante"),
    ("Chef", "APPARTIENE_ORDINE", "Ordine"),
    ("Chef", "PREPARA", "Piatto"),
    #("Pianeta", "DISTA_DA", "Pianeta"),
    ("Tecnica", "USATA_PER_PREPARARE", "Piatto"),
    ("Ingrediente", "UTILIZZATO_PER_PREPARARE", "Piatto"),
  ],
  "node_properties": [
    "nome",
    "descrizione",
    "quantita",
    "unita_di_misura",
    "leggendario",
    "categoria",
    "principi_fondamentali",
    "livello"
  ],
  "relationship_properties": [
    "descrizione",
      "quantitaUtilizzata",
      "unitaDiMisura",
      "distanzaInAnniLuce",
      "gradoRichiesto",
      "certificazioniRichieste",
      "dataInizio",
      "dataFine",
      "condizioniParticolari"

  ]
}

additional_prompt = ("Here are the key information you have to extract from the text:"
                     f"Entity types: {graph_params['allowed_nodes']}"
                     f"Relationship types between entities: {graph_params['allowed_relationships']}"
                     f"Allowed properties of an entity: {graph_params['node_properties']}"
                     f"Allowed properties of a relationship: {graph_params['relationship_properties']}"
                     f"Pay attention to the difference between ingredients and dishes. Do not confuse them!")

async def main():
    files = os.listdir("../Menu")[:3]
    schema = await galactus.load_documents_into_knowledge_graph(files,
                                                                llm=llm,
                                                                directory_prefix="../Menu",
                                                                allowed_nodes=graph_params["allowed_nodes"],
                                                                allowed_relationships=graph_params["allowed_relationships"],
                                                                node_properties=False,
                                                                relationship_properties=False,
                                                                include_titles = False,
                                                                reorganize = True,
                                                                summarize_all = False,
                                                                summarize_info = False,
                                                                summarize_paragraphs = False,
                                                                additional_prompt= additional_prompt)
    optimus_prime.create_knowledge_graph(schema)
    print(f"Knowledge graph {os.environ['NEO4J_DB_NAME']} created.")

if __name__ == "__main__":
    print("Running main...")
    asyncio.run(main())
