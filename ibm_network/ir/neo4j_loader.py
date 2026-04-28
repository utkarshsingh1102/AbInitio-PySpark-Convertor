"""Load a Graph from Neo4j (Parser handoff).

Cypher contract (v1, pending sign-off with the Parser team):

    (:Graph {name})-[:CONTAINS]->(:Component {
        id, ab_initio_type, name,
        params,         // JSON-encoded string of parameter dict
        transform,      // optional DML transform body, raw text
        in_ports,       // list<string>
        out_ports       // list<string>
    })

    (:Component)-[:HAS_DML {port}]->(:DML {name, raw_text})

    (:Component)-[:FLOW {from_port, to_port}]->(:Component)

The loader is forgiving: missing optional fields default to empty.
"""

from __future__ import annotations

import json
from typing import Any

from neo4j import GraphDatabase

from ibm_network.config.settings import Settings, get_settings
from ibm_network.ir.models import Component, DMLRef, Edge, Graph, Port


class Neo4jGraphLoader:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self._driver = GraphDatabase.driver(
            self.settings.neo4j_uri,
            auth=(self.settings.neo4j_user, self.settings.neo4j_password),
        )

    def close(self) -> None:
        self._driver.close()

    def __enter__(self) -> Neo4jGraphLoader:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def load_graph(self, graph_name: str) -> Graph:
        with self._driver.session(database=self.settings.neo4j_database) as session:
            comps = session.run(
                """
                MATCH (g:Graph {name: $name})-[:CONTAINS]->(c:Component)
                OPTIONAL MATCH (c)-[r:HAS_DML]->(d:DML)
                WITH c, collect({port: r.port, name: d.name, raw_text: d.raw_text}) AS dmls
                RETURN c, dmls
                """,
                name=graph_name,
            ).data()

            edges = session.run(
                """
                MATCH (g:Graph {name: $name})-[:CONTAINS]->(a:Component)
                MATCH (a)-[f:FLOW]->(b:Component)
                WHERE (g)-[:CONTAINS]->(b)
                RETURN a.id AS from_id, f.from_port AS from_port,
                       b.id AS to_id, f.to_port AS to_port
                """,
                name=graph_name,
            ).data()

        components = [_build_component(row["c"], row["dmls"]) for row in comps]
        edge_models = [
            Edge(
                from_component=r["from_id"],
                from_port=r["from_port"] or "out",
                to_component=r["to_id"],
                to_port=r["to_port"] or "in",
            )
            for r in edges
        ]
        return Graph(name=graph_name, components=components, edges=edge_models)


def _build_component(node: dict[str, Any], dmls_raw: list[dict[str, Any]]) -> Component:
    params_blob = node.get("params") or "{}"
    try:
        params = json.loads(params_blob) if isinstance(params_blob, str) else dict(params_blob)
    except json.JSONDecodeError:
        params = {}

    in_port_names = node.get("in_ports") or []
    out_port_names = node.get("out_ports") or []

    dml_refs: list[DMLRef] = []
    port_dml_map: dict[str, str] = {}
    for d in dmls_raw:
        if not d.get("name"):
            continue
        dml_refs.append(DMLRef(name=d["name"], raw_text=d.get("raw_text") or ""))
        if d.get("port"):
            port_dml_map[d["port"]] = d["name"]

    return Component(
        id=node["id"],
        ab_initio_type=node["ab_initio_type"],
        name=node.get("name") or node["id"],
        params=params,
        in_ports=[Port(name=p, dml_ref=port_dml_map.get(p)) for p in in_port_names],
        out_ports=[Port(name=p, dml_ref=port_dml_map.get(p)) for p in out_port_names],
        dml_refs=dml_refs,
        transform=node.get("transform"),
    )
