"use strict";

// ---------- tab switching ----------
document.querySelectorAll(".tab-btn").forEach((btn) => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".tab-btn").forEach((b) => b.classList.remove("is-active"));
    document.querySelectorAll(".tab-pane").forEach((p) => p.classList.remove("is-active"));
    btn.classList.add("is-active");
    document.getElementById("tab-" + btn.dataset.tab).classList.add("is-active");
  });
});

// ---------- file upload → textarea ----------
document.querySelectorAll('.upload input[type="file"]').forEach((input) => {
  input.addEventListener("change", () => {
    const file = input.files && input.files[0];
    if (!file) return;
    const target = document.getElementById(input.dataset.target);
    const reader = new FileReader();
    reader.onload = () => {
      target.value = reader.result;
    };
    reader.readAsText(file);
  });
});

// ---------- helpers ----------
async function postJson(url, body) {
  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    const err = new Error(data.detail || resp.statusText);
    throw err;
  }
  return data;
}

function showOutput(elId, text, isError = false) {
  const el = document.getElementById(elId);
  el.textContent = text;
  el.classList.toggle("is-error", isError);
}

function setHidden(elId, text) {
  const el = document.getElementById(elId);
  if (el) el.value = text;
}

function downloadText(filename, text) {
  const blob = new Blob([text], { type: "text/x-python" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

async function copyToClipboard(text) {
  try {
    await navigator.clipboard.writeText(text);
  } catch {
    // fallback
    const ta = document.createElement("textarea");
    ta.value = text;
    document.body.appendChild(ta);
    ta.select();
    document.execCommand("copy");
    document.body.removeChild(ta);
  }
}

// ---------- action dispatcher ----------
document.addEventListener("click", async (e) => {
  const btn = e.target.closest("button[data-action]");
  if (!btn) return;
  const action = btn.dataset.action;

  try {
    if (action === "convert-dml") {
      const text = document.getElementById("dml-input").value;
      const data = await postJson("/api/dml", { text });
      const display = "# Schema\nschema = " + data.schema_source + "\n\n# Read\ndf = " + data.read_source;
      showOutput("dml-output", display);
      setHidden("dml-py", data.py_file);
    } else if (action === "convert-transform") {
      const text = document.getElementById("xfr-input").value;
      const data = await postJson("/api/transform", { text });
      showOutput("xfr-output", data.select_args.join(",\n"));
      setHidden("xfr-py", data.py_file);
    } else if (action === "convert-expr") {
      const text = document.getElementById("expr-input").value;
      const data = await postJson("/api/expr", { text });
      showOutput("expr-output", data.column_source);
    } else if (action === "convert-pipeline") {
      const raw = document.getElementById("pipeline-input").value;
      let body;
      try {
        body = JSON.parse(raw);
      } catch (err) {
        showOutput("pipeline-output", "JSON parse error: " + err.message, true);
        return;
      }
      const data = await postJson("/api/pipeline", body);
      showOutput("pipeline-output", data.code);
      const notesEl = document.getElementById("pipeline-notes");
      notesEl.textContent = data.notes && data.notes.length
        ? "notes:\n  - " + data.notes.join("\n  - ")
        : "";
    } else if (action === "load-example") {
      document.getElementById("pipeline-input").value = JSON.stringify(EXAMPLE_GRAPH, null, 2);
    } else if (action === "copy") {
      const src = document.getElementById(btn.dataset.source);
      const text = "value" in src ? src.value : src.textContent;
      await copyToClipboard(text);
      const orig = btn.textContent;
      btn.textContent = "Copied!";
      setTimeout(() => (btn.textContent = orig), 1100);
    } else if (action === "download") {
      const src = document.getElementById(btn.dataset.source);
      const text = "value" in src && src.tagName === "TEXTAREA" ? src.value : src.textContent;
      if (!text) return;
      downloadText(btn.dataset.filename || "output.py", text);
    }
  } catch (err) {
    const outId = {
      "convert-dml": "dml-output",
      "convert-transform": "xfr-output",
      "convert-expr": "expr-output",
      "convert-pipeline": "pipeline-output",
    }[action];
    if (outId) showOutput(outId, "Error: " + err.message, true);
  }
});

// ---------- example graph ----------
const EXAMPLE_GRAPH = {
  name: "demo_pipeline",
  components: [
    {
      id: "src",
      ab_initio_type: "INPUT_FILE",
      name: "customers_in",
      params: { input_path: "/data/customers.csv" },
      out_ports: [{ name: "out", dml_ref: "cust" }],
      dml_refs: [
        {
          name: "cust",
          raw_text: "record decimal(10) id; string(20) name; integer(4) score; end",
        },
      ],
    },
    {
      id: "rfm",
      ab_initio_type: "REFORMAT",
      name: "reshape",
      transform:
        "out.id :: in.id; out.name :: string_upcase(in.name); out.score :: in.score; out.tier :: if (in.score > 50) \"gold\" else \"silver\";",
    },
    {
      id: "flt",
      ab_initio_type: "FILTER_BY_EXPRESSION",
      name: "only_gold",
      params: { select_expr: 'in.tier = "gold"' },
    },
    {
      id: "agg",
      ab_initio_type: "ROLLUP",
      name: "by_tier",
      params: { key: "tier" },
      transform: "out.cnt :: count_recs(); out.total_score :: sum(in.score);",
    },
  ],
  edges: [
    { from_component: "src", from_port: "out", to_component: "rfm", to_port: "in" },
    { from_component: "rfm", from_port: "out", to_component: "flt", to_port: "in" },
    { from_component: "flt", from_port: "out", to_component: "agg", to_port: "in" },
  ],
};
