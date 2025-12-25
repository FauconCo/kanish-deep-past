# NOMBRE: 01_kanish_export.py
# DESCRIPCIÓN: Extrae la inteligencia y crea el archivo para Kaggle
import json
from neo4j import GraphDatabase

# CONFIGURACIÓN (Tu Password: 14141010)
URI = "neo4j://localhost:7687"
AUTH = ("neo4j", "14141010")

class KanishLocalExporter:
    def __init__(self):
        self.driver = GraphDatabase.driver(URI, auth=AUTH)

    def close(self):
        self.driver.close()

    def freeze_knowledge(self, output_file="kanish_brain_frozen.json"):
        print(f"🧊 Conectando a {URI} para congelar conocimiento...")
        
        # Estructura del artefacto
        artifact = {
            "entities": {},     
            "commodities": {},  
            "relations": []     
        }

        with self.driver.session() as session:
            # 1. REGLAS DURAS DE MERCANCÍAS (Hardcoded para seguridad)
            # Esto es lo que evita que "kaspum" sea "money".
            artifact["commodities"] = {
                "kaspum": "silver",
                "hurasu": "gold",
                "annakum": "tin",
                "subatu": "textiles",
                "manum": "mina",
                "siqlum": "shekel"
            }

            # 2. EXTRACCIÓN DEL GRAFO (Dinámico)
            # Busca las personas que acabamos de insertar en el script 00
            result = session.run("""
                MATCH (p:Person)
                OPTIONAL MATCH (p)-[:SON_OF]->(f:Person)
                RETURN p.normalized_name as name, f.normalized_name as father
            """)
            
            count = 0
            for record in result:
                name = record["name"]
                father = record["father"]
                if name:
                    key = name.lower() 
                    if key not in artifact["entities"]:
                        artifact["entities"][key] = []
                    
                    # Crear contexto: "son of Imdi-ilum"
                    context_str = f"son of {father}" if father else "unknown lineage"
                    artifact["entities"][key].append(context_str)
                    count += 1
            
            print(f"📊 Entidades procesadas: {count}")

        # 3. GUARDAR JSON
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(artifact, f, indent=2)
        
        print(f"✅ ÉXITO TOTAL: Archivo '{output_file}' creado.")
        print(">> ESTE es el archivo que subirás a Kaggle como Dataset.")

if __name__ == "__main__":
    exporter = KanishLocalExporter()
    try:
        exporter.freeze_knowledge()
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        exporter.close()