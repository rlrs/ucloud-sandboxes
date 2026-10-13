// Install Prime Agent's Python skills into its kernel venv, at toolkit build time.
//
// Prime Agent's install bootstraps the kernel with no Python skills; its first
// session then installs the skills it shows the model (`uv pip install
// --editable`, built with hatchling from PyPI) and rewrites the venv's
// .bootstrap-version. A relay-only sandbox has neither uv nor PyPI, so the
// kernel never starts ("uv is required to set up the Python kernel").
//
// This runs one Prime Agent session the way verifiers' prime-agent harness
// does (ACP mode, its settings and flags, an intercepted provider) against a
// stub model that asks for one ipython call, so Prime Agent itself installs
// exactly the skills a rollout's session sees and records them. Run it in a
// container laid out as a sandbox is (the toolkit at /opt/ucloud/toolkits/
// vf-agents, linked at /var/tmp/vf-* and /root/.prime/agent/kernel-venv), with
// uv on PATH and PyPI reachable: Prime Agent names its skills by its resolved
// package path. Exits 0 once the tool call ran and the venv records the skills.
import { spawn } from "node:child_process";
import { mkdirSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";

const AGENT_ROOT = "/var/tmp/vf-prime-agent";
const VENV = "/root/.prime/agent/kernel-venv";
const STATE = "/tmp/prime-kernel-sync";
const SENTINEL = "PRIME_KERNEL_SYNC_OK";
const commits = readdirSync(AGENT_ROOT).filter((name) => /^[0-9a-f]{40}$/.test(name));
if (commits.length !== 1) throw new Error(`expected one Prime Agent commit in ${AGENT_ROOT}: ${commits}`);
const bin = `${AGENT_ROOT}/${commits[0]}/bin/prime-agent`;

// The stub model: one ipython call, then a final answer that checks its result.
let calls = 0;
let toolResult = "";
const server = createServer((req, res) => {
  let body = "";
  req.on("data", (chunk) => (body += chunk));
  req.on("end", () => {
    const request = JSON.parse(body || "{}");
    calls += 1;
    const tool = [...(request.messages ?? [])].reverse().find((m) => m.role === "tool");
    if (tool) toolResult = JSON.stringify(tool.content);
    const message = calls === 1
      ? { role: "assistant", content: null, tool_calls: [{ id: "sync-1", type: "function",
          function: { name: "ipython", arguments: JSON.stringify({ code: `print("${SENTINEL}")` }) } }] }
      : { role: "assistant", content: "done" };
    const finish = calls === 1 ? "tool_calls" : "stop";
    const base = { id: `sync-${calls}`, created: Math.floor(Date.now() / 1000), model: request.model };
    if (!request.stream) {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ...base, object: "chat.completion",
        choices: [{ index: 0, message, finish_reason: finish }],
        usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 } }));
      return;
    }
    const delta = { ...message };
    if (delta.tool_calls) delta.tool_calls = delta.tool_calls.map((t, index) => ({ ...t, index }));
    const chunk = (choice) => `data: ${JSON.stringify({ ...base, object: "chat.completion.chunk", choices: [choice] })}\n\n`;
    res.writeHead(200, { "Content-Type": "text/event-stream" });
    res.end(chunk({ index: 0, delta, finish_reason: null }) + chunk({ index: 0, delta: {}, finish_reason: finish })
      + "data: [DONE]\n\n");
  });
});
await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
const port = server.address().port;

// The harness's per-run state: settings.json, models.json, TMPDIR (prime_agent/harness.py).
for (const dir of [`${STATE}/agent`, `${STATE}/tmp`, `${STATE}/work`]) mkdirSync(dir, { recursive: true, mode: 0o700 });
writeFileSync(`${STATE}/agent/settings.json`, JSON.stringify({
  autoRefine: { enabled: false },
  compaction: { enabled: false, reserveTokens: 32768, keepRecentTokens: 16384 },
}));
writeFileSync(`${STATE}/agent/models.json`, JSON.stringify({ providers: { intercept: {
  baseUrl: `http://127.0.0.1:${port}/v1`, api: "openai-completions", apiKey: "PRIME_AGENT_INTERCEPT_KEY",
  models: [{ id: "sync", contextWindow: 131072, maxTokens: 16384, reasoning: false, input: ["text", "image"] }],
} } }), { mode: 0o600 });

const agent = spawn(bin, ["--mode", "acp", "--provider", "intercept", "--model", "sync",
  "--daemon-socket", `${STATE}/daemon.sock`, "--offline"], {
  cwd: `${STATE}/work`,
  env: { ...process.env, PRIME_AGENT_INTERCEPT_KEY: "sync", PRIME_AGENT_CODING_AGENT_DIR: `${STATE}/agent`,
    TMPDIR: `${STATE}/tmp`, PRIME_AGENT_TELEMETRY: "0" },
  stdio: ["pipe", "pipe", "inherit"],
});

// ACP over stdio: newline-delimited JSON-RPC, as verifiers' runner speaks it.
let nextId = 0;
const pending = new Map();
const send = (message) => agent.stdin.write(JSON.stringify({ jsonrpc: "2.0", ...message }) + "\n");
const call = (method, params) => new Promise((resolve, reject) => {
  const id = ++nextId;
  pending.set(id, { resolve, reject });
  send({ id, method, params });
});
let buffer = "";
agent.stdout.on("data", (data) => {
  buffer += data;
  let newline;
  while ((newline = buffer.indexOf("\n")) >= 0) {
    const line = buffer.slice(0, newline).trim();
    buffer = buffer.slice(newline + 1);
    if (!line) continue;
    const message = JSON.parse(line);
    if (message.method && message.id !== undefined) {  // The agent asks the client.
      if (message.method === "session/request_permission") {
        const option = message.params.options.find((o) => o.kind === "allow_once" || o.kind === "allow_always");
        send({ id: message.id, result: { outcome: option
          ? { outcome: "selected", optionId: option.optionId } : { outcome: "cancelled" } } });
      } else {
        send({ id: message.id, error: { code: -32601, message: `${message.method} is not supported` } });
      }
    } else if (message.id !== undefined && pending.has(message.id)) {
      const { resolve, reject } = pending.get(message.id);
      pending.delete(message.id);
      message.error ? reject(new Error(JSON.stringify(message.error))) : resolve(message.result);
    }
  }
});
const exited = new Promise((resolve) => agent.on("exit", (code, signal) => resolve({ code, signal })));
const fail = (error) => { console.error(`prime kernel sync: ${error.message ?? error}`); agent.kill(); process.exit(1); };
const deadline = setTimeout(() => fail(new Error("timed out after 15 minutes")), 15 * 60 * 1000);
exited.then(({ code, signal }) => { if (pending.size) fail(new Error(`agent exited (${code ?? signal})`)); });

try {
  await call("initialize", { protocolVersion: 1, clientCapabilities: {} });
  const session = await call("session/new", { cwd: `${STATE}/work`, mcpServers: [] });
  const result = await call("session/prompt", { sessionId: session.sessionId,
    prompt: [{ type: "text", text: "Run the ipython call you are given." }] });
  if (!toolResult.includes(SENTINEL)) throw new Error(`the ipython call failed: ${toolResult.slice(0, 2000)}`);
  const version = JSON.parse(readFileSync(`${VENV}/.bootstrap-version`, "utf8"));
  const skills = (version.pythonSkills ?? []).map((skill) => skill.importName);
  if (!skills.length) throw new Error("the kernel venv records no Python skills");
  console.log(JSON.stringify({ stop_reason: result.stopReason, model_calls: calls, python_skills: skills }));
} catch (error) {
  fail(error);
}
clearTimeout(deadline);
agent.kill();
server.close();
