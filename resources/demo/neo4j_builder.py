import json
import time

from neo4j import GraphDatabase

def create_database_if_not_exists(driver, database_name):
    # Utilizza il database "system" per gestire la creazione di altri database
    with driver.session(database="system") as sys_session:
        # Recupera la lista dei database esistenti
        result = sys_session.run("SHOW DATABASES").data()
        db_exists = any(record.get("name") == database_name for record in result)
        if not db_exists:
            print(f"Database '{database_name}' non esiste. Creo il database...")
            sys_session.run(f"CREATE DATABASE {database_name}")
            # Facoltativamente, attendi che il database sia online prima di procedere
            time.sleep(2)  # Attendi 5 secondi (modifica se necessario)
        else:
            print(f"Database '{database_name}' già esistente.")

def build_neo4j_graph(json_data):
    # Parametri di connessione (modifica secondo la tua configurazione)
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "password"  # Sostituisci con la password corretta
    database_name = "ingestor"

    driver = GraphDatabase.driver(uri, auth=(user, password))

    # Verifica e crea il database se non esiste
    create_database_if_not_exists(driver, database_name)

    with driver.session(database=database_name) as session:
        # 1. Creazione del nodo Pianeta e del nodo Ristorante
        planet = json_data.get("Planet")
        restaurant_name = json_data.get("Restaurant")

        session.run("MERGE (p:Pianeta {name: $name})", name=planet)
        session.run("MERGE (r:Ristorante {name: $name})", name=restaurant_name)
        # Relazione: (Ristorante)-[:LOCALIZZATO_SU]->(Pianeta)
        session.run("""
            MATCH (r:Ristorante {name: $r_name}), (p:Pianeta {name: $p_name})
            MERGE (r)-[:LOCALIZZATO_SU]->(p)
        """, r_name=restaurant_name, p_name=planet)

        # 2. Creazione del nodo Chef e la relazione LAVORA_IN
        chef_data = json_data.get("Chef", {})
        chef_name = chef_data.get("Name")
        session.run("MERGE (c:Chef {name: $name})", name=chef_name)
        session.run("""
            MATCH (c:Chef {name: $c_name}), (r:Ristorante {name: $r_name})
            MERGE (c)-[:LAVORA_IN]->(r)
        """, c_name=chef_name, r_name=restaurant_name)

        # 3. Creazione dei nodi Licenza e relazione HA_LICENZA
        licenses = chef_data.get("Licenses", [])
        for lic in licenses:
            lic_name = lic.get("Name")
            lic_level = lic.get("Level")
            session.run(
                "MERGE (l:Licenza {name: $name, level: $level})",
                name=lic_name, level=lic_level
            )
            session.run("""
                MATCH (c:Chef {name: $c_name}), (l:Licenza {name: $l_name, level: $l_level})
                MERGE (c)-[:HA_LICENZA]->(l)
            """, c_name=chef_name, l_name=lic_name, l_level=lic_level)

        # (Opzionale) Se è necessario creare il nodo "Ordine" e la relazione APPARTIENE_ORDINE,
        # si dovrà prevedere i dati relativi all'ordine. In questo esempio non sono presenti.

        # 4. Elaborazione dei piatti
        dishes = json_data.get("Dishes", [])
        for dish in dishes:
            dish_name = dish.get("Name")
            # Crea il nodo Piatto
            session.run("MERGE (d:Piatto {name: $name})", name=dish_name)
            # Relazione: (Piatto)-[:SERVITO_IN]->(Ristorante)
            session.run("""
                MATCH (d:Piatto {name: $d_name}), (r:Ristorante {name: $r_name})
                MERGE (d)-[:SERVITO_IN]->(r)
            """, d_name=dish_name, r_name=restaurant_name)
            # Relazione: (Piatto)-[:PREPARATO_DA]->(Chef) e (Chef)-[:PREPARA]->(Piatto)
            session.run("""
                MATCH (d:Piatto {name: $d_name}), (c:Chef {name: $c_name})
                MERGE (d)-[:PREPARATO_DA]->(c)
            """, d_name=dish_name, c_name=chef_name)
            session.run("""
                MATCH (c:Chef {name: $c_name}), (d:Piatto {name: $d_name})
                MERGE (c)-[:PREPARA]->(d)
            """, c_name=chef_name, d_name=dish_name)

            # 4a. Elaborazione degli ingredienti del piatto
            ingredients = dish.get("Ingredients", [])
            for ingredient in ingredients:
                session.run("MERGE (i:Ingrediente {name: $name})", name=ingredient)
                # Relazioni: (Piatto)-[:CONTIENE_INGREDIENTE]->(Ingrediente)
                session.run("""
                    MATCH (d:Piatto {name: $d_name}), (i:Ingrediente {name: $i_name})
                    MERGE (d)-[:CONTIENE_INGREDIENTE]->(i)
                """, d_name=dish_name, i_name=ingredient)
                # Relazione inversa: (Ingrediente)-[:UTILIZZATO_PER_PREPARARE]->(Piatto)
                session.run("""
                    MATCH (i:Ingrediente {name: $i_name}), (d:Piatto {name: $d_name})
                    MERGE (i)-[:UTILIZZATO_PER_PREPARARE]->(d)
                """, i_name=ingredient, d_name=dish_name)

            # 4b. Elaborazione delle tecniche utilizzate per il piatto
            techniques = dish.get("Techniques", [])
            for technique in techniques:
                session.run("MERGE (t:Tecnica {name: $name})", name=technique)
                # Relazione: (Piatto)-[:APPLICA_TECNICA]->(Tecnica)
                session.run("""
                    MATCH (d:Piatto {name: $d_name}), (t:Tecnica {name: $t_name})
                    MERGE (d)-[:APPLICA_TECNICA]->(t)
                """, d_name=dish_name, t_name=technique)
                # Relazione inversa: (Tecnica)-[:USATA_PER_PREPARARE]->(Piatto)
                session.run("""
                    MATCH (t:Tecnica {name: $t_name}), (d:Piatto {name: $d_name})
                    MERGE (t)-[:USATA_PER_PREPARARE]->(d)
                """, t_name=technique, d_name=dish_name)

    driver.close()


# Esempio di utilizzo:
if __name__ == "__main__":
    # Il JSON fornito, ad esempio letto da una stringa o da un file
    json_string = '''
    {
        "Restaurant": "Ristorante \\"Armonia Universale\\"",
        "Chef": {
            "Name": "Maestro Alessandro Stellanova",
            "Licenses": [
                {"Name": "Psionica", "Level": "I"},
                {"Name": "Gravitazionale", "Level": "I"},
                {"Name": "Magnetica", "Level": "I"},
                {"Name": "Quantistica", "Level": "8"},
                {"Name": "Luce", "Level": "II"}
            ]
        },
        "Dishes": [
            {
                "Name": "Sinfonia Cosmica di Sapore",
                "Ingredients": [
                    "Shard di Materia Oscura",
                    "Carne di Xenodonte",
                    "Fibra di Sintetex",
                    "Farina di Nettuno",
                    "Granuli di Nebbia Arcobaleno",
                    "Nduja Fritta Tanto",
                    "Ravioli al Vaporeon"
                ],
                "Techniques": [
                    "Idro-Cristallizzazione Sonora Quantistica",
                    "Saltare in Padella Realtà Energetiche Parallele",
                    "Sferificazione tramite Matrici Biofotiche",
                    "Affumicatura a Stratificazione Quantica",
                    "Sferificazione con Campi Magnetici Entropici"
                ]
            },
            {
                "Name": "Sinfonia Cosmica di Luminiscenze e Contrasti",
                "Ingredients": [
                    "Uova di Fenice",
                    "Carne di Drago",
                    "Carne di Kraken",
                    "Amido di Stellarion",
                    "Riso di Cassandra",
                    "Spezie Melange",
                    "Slurm"
                ],
                "Techniques": [
                    "Ebollizione Magneto-Cinetica Pulsante",
                    "Grigliatura Eletro-Molecolare a Spaziatura Variabile"
                ]
            },
            {
                "Name": "Sinfonia Cosmica in Otto Movimenti",
                "Ingredients": [
                    "Frammenti di Supernova",
                    "Foglie di Nebulosa",
                    "Alghe Bioluminescenti",
                    "Fibra di Sintetex",
                    "Carne di Mucca",
                    "Gnocchi del Crepuscolo",
                    "Lacrime di Andromeda",
                    "Spezie Melange"
                ],
                "Techniques": [
                    "Sferificazione con Campi Magnetici Entropici",
                    "Saltare in Padella Classica",
                    "Bollitura Termografica a Rotazione Veloce",
                    "Grigliatura Eletro-Molecolare a Spaziatura Variabile"
                ]
            },
            {
                "Name": "Portale Cosmico: Sinfonia di Gnocchi del Crepuscolo con Essenza di Tachioni e Sfumature di Fenice",
                "Ingredients": [
                    "Shard di Materia Oscura",
                    "Uova di Fenice",
                    "Gnocchi del Crepuscolo",
                    "Plasma Vitale",
                    "Essenza di Tachioni"
                ],
                "Techniques": [
                    "Bollitura Infrasonica Armonizzata",
                    "Cottura a Vapore Ecodinamico Bilanciato",
                    "Decostruzione Magnetica Risonante",
                    "Cottura a Vapore con Flusso di Particelle Isoarmoniche",
                    "Affumicatura Polarizzata a Freddo Iperbarico"
                ]
            },
            {
                "Name": "Sinfonia Cosmica: Versione Data",
                "Ingredients": [
                    "Polvere di Stelle",
                    "Radici di Gravità",
                    "Colonia di Mycoflora",
                    "Fusilli del Vento",
                    "Pane degli Abissi",
                    "Essenza di Tachioni"
                ],
                "Techniques": [
                    "Cottura a Vapore Termocinetica Multipla",
                    "Affumicatura a Stratificazione Quantica",
                    "Fermentazione Psionica Energetica"
                ]
            },
            {
                "Name": "Sinfonia Cosmologica",
                "Ingredients": [
                    "Shard di Materia Oscura",
                    "Radici di Gravità",
                    "Foglie di Nebulosa",
                    "Biscotti della Galassia",
                    "Essenza di Vuoto"
                ],
                "Techniques": [
                    "Affettamento a Pulsazioni Quantistiche",
                    "Cottura Sottovuoto Pulsar Magnetica",
                    "Marinatura Sotto Zero a Polarità Inversa",
                    "Grigliatura Eletro-Molecolare a Spaziatura Variabile"
                ]
            },
            {
                "Name": "Sinfonia Cosmica di Armonie Terrestri e Celesti",
                "Ingredients": [
                    "Cristalli di Memoria",
                    "Uova di Fenice",
                    "Carne di Kraken",
                    "Fibra di Sintetex",
                    "Granuli di Nebbia Arcobaleno",
                    "Lacrime di Andromeda",
                    "Spezie Melange"
                ],
                "Techniques": [
                    "Sferificazione tramite Matrici Biofotiche",
                    "Idro-Cristallizzazione Sonora Quantistica",
                    "Affettamento a Pulsazioni Quantistiche",
                    "Cottura Sottovuoto Pulsar Magnetica"
                ]
            },
            {
                "Name": "Cosmo Sferico di Sogni Rigenerativi",
                "Ingredients": [
                    "Radici di Gravità",
                    "Baccacedro",
                    "Lattuga Namecciana",
                    "Teste di Idra",
                    "Carne di Xenodonte",
                    "Farina di Nettuno",
                    "Nettare di Sirena"
                ],
                "Techniques": [
                    "Sferificazione con Campi Magnetici Entropici",
                    "Cottura a Vapore Ecodinamico Bilanciato",
                    "Congelamento Bio-Luminiscente Sincronico"
                ]
            }
        ],
        "Planet": "Pandora"
    }
    '''

    # Carica il JSON in un dizionario Python
    data = json.loads(json_string)
    # Costruisci il grafo in Neo4j
    build_neo4j_graph(data)
