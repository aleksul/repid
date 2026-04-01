<!-- markdownlint-disable MD033 -->
# Repid Playground

Try Repid _instantly_ in your browser!
This interactive playground is powered by
[Pyodide](https://pyodide.org) and [CodeMirror](https://codemirror.net/5/),
allowing you to experiment with Repid using real Python - no install required.

- **Repid is pure Python:** No C extensions or native binaries, so it
runs anywhere Python _itself_ can run - even in the browser.
- The browser-based environment uses modern WebAssembly technology.
- The editor below is fully interactive.

---

<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.13/codemirror.min.css">
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.13/codemirror.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.13/mode/python/python.min.js"></script>
<script src="https://cdn.jsdelivr.net/pyodide/v0.29.3/full/pyodide.js"></script>

<style>
/* Base editor styles mapped to MkDocs Material variables */
.CodeMirror {
  border-radius: 4px;
  border: 1px solid var(--md-default-fg-color--lightest);
  font-family: var(--md-code-font-family, "Roboto Mono", monospace);
  font-size: 0.85em;
  height: 400px;
  background-color: var(--md-code-bg-color);
  color: var(--md-code-fg-color);
  margin-bottom: 1rem;
}
.CodeMirror-gutters {
  background-color: var(--md-default-bg-color);
  border-right: 1px solid var(--md-default-fg-color--lightest);
}
.CodeMirror-linenumber {
  color: var(--md-default-fg-color--light);
}

/* Default (Light) Theme Syntax */
.cm-s-default .cm-keyword { color: #d73a49; font-weight: bold; }
.cm-s-default .cm-def { color: #6f42c1; font-weight: bold; }
.cm-s-default .cm-variable { color: var(--md-code-fg-color); }
.cm-s-default .cm-variable-2 { color: #005cc5; }
.cm-s-default .cm-variable-3 { color: #e36209; }
.cm-s-default .cm-string { color: #032f62; }
.cm-s-default .cm-number { color: #005cc5; }
.cm-s-default .cm-comment { color: #6a737d; font-style: italic; }
.cm-s-default .cm-builtin { color: #005cc5; }
.cm-s-default .cm-operator { color: #d73a49; }
.cm-s-default .cm-meta { color: #d73a49; }

/* Slate (Dark) Theme Syntax overridden via MkDocs data attribute */
[data-md-color-scheme="slate"] .cm-s-default .cm-keyword { color: #c678dd; }
[data-md-color-scheme="slate"] .cm-s-default .cm-def { color: #61afef; }
[data-md-color-scheme="slate"] .cm-s-default .cm-variable { color: var(--md-code-fg-color); }
[data-md-color-scheme="slate"] .cm-s-default .cm-variable-2 { color: #e06c75; }
[data-md-color-scheme="slate"] .cm-s-default .cm-variable-3 { color: #d19a66; }
[data-md-color-scheme="slate"] .cm-s-default .cm-string { color: #98c379; }
[data-md-color-scheme="slate"] .cm-s-default .cm-number { color: #d19a66; }
[data-md-color-scheme="slate"] .cm-s-default .cm-comment { color: #5c6370; font-style: italic; }
[data-md-color-scheme="slate"] .cm-s-default .cm-builtin { color: #e5c07b; }
[data-md-color-scheme="slate"] .cm-s-default .cm-operator { color: #56b6c2; }
[data-md-color-scheme="slate"] .cm-s-default .cm-meta { color: #c678dd; }
[data-md-color-scheme="slate"] .CodeMirror-cursor { border-left-color: #528bff; }

/* Playground Layout */
.playground-controls {
  margin-bottom: 1rem;
  display: flex;
  gap: 0.5rem;
}
.playground-output {
  min-height: 150px;
  max-height: 400px;
  overflow-y: auto;
  background-color: var(--md-code-bg-color);
  color: var(--md-code-fg-color);
  padding: 1rem;
  border-radius: 4px;
  border: 1px solid var(--md-default-fg-color--lightest);
  font-family: var(--md-code-font-family, "Roboto Mono", monospace);
  font-size: 0.85em;
  white-space: pre-wrap;
}
</style>

<div>
  <textarea id="code-editor"></textarea>
</div>

<div class="playground-controls">
  <button id="run-btn" class="md-button md-button--primary" disabled>Loading Pyodide...</button>
  <button id="reset-btn" class="md-button">Reset</button>
  <button id="clear-btn" class="md-button">Clear editor</button>
</div>

<h4>Output</h4>
<div id="output-block" class="playground-output">Initializing environment...</div>

<script>
const defaultCode = `import asyncio
from repid import Repid, Router, InMemoryServer

# 1. Initialize the application and register a server
app = Repid()
app.servers.register_server("default", InMemoryServer(), is_default=True)

# 2. Create a router and an actor
router = Router()

@router.actor(channel="tasks")
async def my_awesome_actor(user_id: int) -> None:
    print(f"Processing for {user_id=}")
    await asyncio.sleep(1.0)
    print(f"Done processing for {user_id=}")

app.include_router(router)

async def main() -> None:
    # 3. Open connection to interact with the queue
    async with app.servers.default.connection():
        # Producer: Send a message
        await app.send_message_json(
            channel="tasks",
            payload={"user_id": 123},
            headers={"topic": "my_awesome_actor"}
        )

        # Consumer: Run the worker loop
        print("Starting worker loop...")
        await app.run_worker(messages_limit=1)
        print("Worker loop finished!")

# Pyodide supports top-level await directly!
# We don't need \`asyncio.run(main())\` here because Pyodide's event loop is already running.
await main()
`;

const editorElement = document.getElementById("code-editor");
editorElement.value = defaultCode;

const editor = CodeMirror.fromTextArea(editorElement, {
    mode: "python",
    lineNumbers: true,
    indentUnit: 4,
    matchBrackets: true
});

let pyodide = null;
const runBtn = document.getElementById("run-btn");
const resetBtn = document.getElementById("reset-btn");
const clearBtn = document.getElementById("clear-btn");
const outputBlock = document.getElementById("output-block");

function printOutput(text) {
    if (text !== undefined) {
        outputBlock.textContent += text + "\n";
        outputBlock.scrollTop = outputBlock.scrollHeight;
    }
}

async function initPyodide() {
    try {
        pyodide = await loadPyodide({
            stdout: printOutput,
            stderr: printOutput
        });
        await pyodide.loadPackage("micropip");
        await pyodide.loadPackage("typing-extensions");
        await pyodide.loadPackage("ssl");
        const micropip = pyodide.pyimport("micropip");
        await micropip.install("repid>=2.0.0a17");
        runBtn.textContent = "Run";
        runBtn.disabled = false;
        outputBlock.textContent = "";
    } catch (err) {
        outputBlock.textContent = "Failed to load environment: " + err;
    }
}

runBtn.addEventListener("click", async () => {
    if (!pyodide) return;
    runBtn.disabled = true;
    runBtn.textContent = "Running...";
    outputBlock.textContent = ""; // Clear previous output
    try {
        await pyodide.runPythonAsync(editor.getValue());
    } catch (err) {
        printOutput(err.toString());
    }
    runBtn.disabled = false;
    runBtn.textContent = "Run";
});

resetBtn.addEventListener("click", () => {
    editor.setValue(defaultCode);
    outputBlock.textContent = "";
});

clearBtn.addEventListener("click", () => {
    editor.setValue("");
    outputBlock.textContent = "";
});

// Initialize Pyodide on page load
initPyodide();
</script>

---

## How does this work?

This playground uses Pyodide to install pure-Python packages from PyPI _in your browser_.
Because Repid has no C dependencies, you get the real thing (not a sandboxed reimplementation):

- The editor above is rendered using CodeMirror 5 and has a Run button.
- When you load the page, Python 3 is started in the browser, Repid is installed using micropip.
- When you click Run, your code runs as you typed, completely client-side.

Learn more about [pyodide](https://pyodide.org) in its docs.
