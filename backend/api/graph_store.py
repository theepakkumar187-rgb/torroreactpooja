from typing import Dict, Any, List, Optional
import os
import json

GRAPH_STORE_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'lineage_store.json')

class GraphStore:
    def __init__(self):
        self.backend = os.getenv('TORRO_GRAPH_BACKEND', 'json').lower()
        self.neo4j_uri = os.getenv('NEO4J_URI')
        self.neo4j_user = os.getenv('NEO4J_USER')
        self.neo4j_password = os.getenv('NEO4J_PASSWORD')
        self._driver = None
        if self.backend == 'neo4j' and self.neo4j_uri:
            try:
                from neo4j import GraphDatabase
                self._driver = GraphDatabase.driver(self.neo4j_uri, auth=(self.neo4j_user, self.neo4j_password))
            except Exception as e:
                print(f"WARN: Neo4j driver init failed: {e}; falling back to JSON store")
                self.backend = 'json'

    def close(self):
        if self._driver:
            self._driver.close()

    def upsert_nodes(self, nodes: List[Dict[str, Any]]):
        if self.backend == 'neo4j' and self._driver:
            try:
                cypher = """
                UNWIND $nodes AS n
                MERGE (a:Asset {id: n.id})
                SET a.name = n.name, a.type = n.type, a.catalog = n.catalog, a.connector_id = n.connector_id
                """
                with self._driver.session() as sess:
                    sess.run(cypher, nodes=nodes)
                return
            except Exception as e:
                print(f"WARN: Neo4j upsert_nodes failed: {e}; fallback to JSON")
        store = _load_json()
        store.setdefault('nodes', {})
        for n in nodes:
            store['nodes'][n['id']] = n
        _save_json(store)

    def upsert_edges(self, edges: List[Dict[str, Any]]):
        if self.backend == 'neo4j' and self._driver:
            try:
                cypher = """
                UNWIND $edges AS e
                MERGE (s:Asset {id: e.source})
                MERGE (t:Asset {id: e.target})
                MERGE (s)-[r:REL {sig: coalesce(e.edge_signature, e.relationship)}]->(t)
                SET r.relationship = e.relationship, r.confidence = e.confidence_score, r.updated_at = e.updated_at
                """
                with self._driver.session() as sess:
                    sess.run(cypher, edges=edges)
                return
            except Exception as e:
                print(f"WARN: Neo4j upsert_edges failed: {e}; fallback to JSON")
        store = _load_json()
        store.setdefault('edges', [])
        store['edges'].extend(edges)
        _save_json(store)

def _load_json() -> Dict[str, Any]:
    try:
        if os.path.exists(GRAPH_STORE_FILE):
            with open(GRAPH_STORE_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"WARN: graph_store load failed: {e}")
    return {}

def _save_json(payload: Dict[str, Any]):
    try:
        with open(GRAPH_STORE_FILE, 'w') as f:
            json.dump(payload, f, indent=2)
    except Exception as e:
        print(f"WARN: graph_store save failed: {e}")


