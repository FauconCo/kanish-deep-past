# NOMBRE: 00_populate_graph.py
# DESCRIPCIÓN: Inyecta la Ontología y Datos Semilla en Neo4j Local
import logging
from neo4j import GraphDatabase

# CONFIGURACIÓN (Tu Password: 14141010)
URI = "neo4j://localhost:7687"
AUTH = ("neo4j", "14141010")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KanishSeeder")

class KanishSeeder:
    def __init__(self):
        self.driver = GraphDatabase.driver(URI, auth=AUTH)

    def close(self):
        self.driver.close()

    def clean_db(self):
        """BORRADO DE SEGURIDAD: Limpia la DB para empezar de cero."""
        with self.driver.session() as session:
            logger.warning("🧹 Limpiando base de datos completa...")
            session.run("MATCH (n) DETACH DELETE n")
            # Eliminar restricciones antiguas si existen
            try:
                session.run("DROP CONSTRAINT person_id IF EXISTS")
            except:
                pass

    def init_schema(self):
        """Define la estructura física del cerebro."""
        with self.driver.session() as session:
            logger.info("🏗️ Creando esquema y restricciones...")
            # Garantizar que no haya duplicados de Personas ni Textos
            session.run("CREATE CONSTRAINT person_id IF NOT EXISTS FOR (p:Person) REQUIRE p.id IS UNIQUE")
            session.run("CREATE INDEX person_norm_name IF NOT EXISTS FOR (p:Person) ON (p.normalized_name)")

    def seed_data(self):
        """Carga los datos 'verdad' (Ground Truth)."""
        with self.driver.session() as session:
            logger.info("🌱 Sembrando datos prosopográficos y comerciales...")
            
            # 1. Crear Nodos de Personas (La Red Social)
            session.run("""
                MERGE (p1:Person {id: 'P001', normalized_name: 'Puzur-Ashur', role: 'tamkarum'})
                MERGE (p2:Person {id: 'P002', normalized_name: 'Enlil-bani', role: 'merchant'})
                MERGE (p3:Person {id: 'P003', normalized_name: 'Imdi-ilum', role: 'head_of_firm'})
                MERGE (p4:Person {id: 'P004', normalized_name: 'Ashur-idi', role: 'transporter'})
            """)

            # 2. Crear Relaciones Genealógicas (Contexto para desambiguación)
            # Puzur-Ashur es hijo de Imdi-ilum (Datos ficticios basados en patrón real)
            session.run("""
                MATCH (son:Person {normalized_name: 'Puzur-Ashur'}), (father:Person {normalized_name: 'Imdi-ilum'})
                MERGE (son)-[:SON_OF]->(father)
            """)

            # 3. Datos de Entrenamiento Implícitos (Mercancías y Reglas)
            # Esto se usará para generar el diccionario de restricciones
            # No necesitamos nodos explícitos de mercancías si usamos reglas duras en el JSON,
            # pero las relaciones comerciales ayudan a validar.
            session.run("""
                MATCH (a:Person {normalized_name: 'Enlil-bani'}), (b:Person {normalized_name: 'Puzur-Ashur'})
                MERGE (a)-[:OWES {amount: 5.0, commodity: 'silver', unit: 'mina'}]->(b)
            """)
            
            logger.info("✅ Base de datos poblada exitosamente.")

if __name__ == "__main__":
    seeder = KanishSeeder()
    try:
        seeder.clean_db()   # PASO 1: Limpiar
        seeder.init_schema()# PASO 2: Esquema
        seeder.seed_data()  # PASO 3: Datos
        print("--- PROCESO DE POBLADO TERMINADO ---")
        print("Ahora tu instancia 'KANISH' tiene datos listos para exportar.")
    except Exception as e:
        logger.error(f"Error crítico: {e}")
    finally:
        seeder.close()