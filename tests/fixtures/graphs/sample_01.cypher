// Sample graph fixture for the IBM Network convertor.
//   source(INPUT_FILE) -> reformat -> filter -> sort -> sink(OUTPUT_FILE)

MATCH (g:Graph {name: "sample_01"})
DETACH DELETE g;

CREATE (g:Graph {name: "sample_01"});

CREATE (src:Component {
    id: "src",
    ab_initio_type: "INPUT_FILE",
    name: "customers_in",
    params: '{"input_path": "/data/customers.csv"}',
    in_ports: [],
    out_ports: ["out"]
});
CREATE (dml_customer:DML {
    name: "customer_record",
    raw_text: "record decimal(10) id; string(20) name; integer(4) score; end"
});

CREATE (rfm:Component {
    id: "rfm",
    ab_initio_type: "REFORMAT",
    name: "reshape",
    params: "{}",
    transform: "out.id :: in.id; out.name :: string_upcase(in.name); out.tier :: if (in.score > 50) \"gold\" else \"silver\";",
    in_ports: ["in"],
    out_ports: ["out"]
});

CREATE (flt:Component {
    id: "flt",
    ab_initio_type: "FILTER_BY_EXPRESSION",
    name: "gold_only",
    params: '{"select_expr": "in.tier = \\"gold\\""}',
    in_ports: ["in"],
    out_ports: ["out"]
});

CREATE (srt:Component {
    id: "srt",
    ab_initio_type: "SORT",
    name: "sort_by_id",
    params: '{"key": "id asc"}',
    in_ports: ["in"],
    out_ports: ["out"]
});

CREATE (g)-[:CONTAINS]->(src);
CREATE (g)-[:CONTAINS]->(rfm);
CREATE (g)-[:CONTAINS]->(flt);
CREATE (g)-[:CONTAINS]->(srt);

CREATE (src)-[:HAS_DML {port: "out"}]->(dml_customer);

CREATE (src)-[:FLOW {from_port: "out", to_port: "in"}]->(rfm);
CREATE (rfm)-[:FLOW {from_port: "out", to_port: "in"}]->(flt);
CREATE (flt)-[:FLOW {from_port: "out", to_port: "in"}]->(srt);
