"""In-memory IR for an Ab Initio graph, populated from the Parser's Neo4j output."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class Port(BaseModel):
    name: str
    dml_ref: str | None = None


class DMLRef(BaseModel):
    """Either an inline DML body or a path/id to one stored elsewhere."""

    name: str
    raw_text: str


class Component(BaseModel):
    id: str
    ab_initio_type: str
    name: str
    params: dict[str, Any] = Field(default_factory=dict)
    in_ports: list[Port] = Field(default_factory=list)
    out_ports: list[Port] = Field(default_factory=list)
    dml_refs: list[DMLRef] = Field(default_factory=list)
    transform: str | None = None


class Edge(BaseModel):
    from_component: str
    from_port: str
    to_component: str
    to_port: str


class Graph(BaseModel):
    name: str
    components: list[Component] = Field(default_factory=list)
    edges: list[Edge] = Field(default_factory=list)

    def component(self, comp_id: str) -> Component:
        for c in self.components:
            if c.id == comp_id:
                return c
        raise KeyError(f"component not found: {comp_id}")

    def upstream(self, comp_id: str) -> list[Edge]:
        return [e for e in self.edges if e.to_component == comp_id]

    def downstream(self, comp_id: str) -> list[Edge]:
        return [e for e in self.edges if e.from_component == comp_id]

    def topological_order(self) -> list[Component]:
        """Kahn's algorithm. Raises ValueError on cycles."""
        indegree = {c.id: 0 for c in self.components}
        for e in self.edges:
            indegree[e.to_component] = indegree.get(e.to_component, 0) + 1
        ready = [cid for cid, d in indegree.items() if d == 0]
        order: list[str] = []
        while ready:
            cid = ready.pop(0)
            order.append(cid)
            for e in self.downstream(cid):
                indegree[e.to_component] -= 1
                if indegree[e.to_component] == 0:
                    ready.append(e.to_component)
        if len(order) != len(self.components):
            raise ValueError("cycle detected in graph")
        return [self.component(cid) for cid in order]
