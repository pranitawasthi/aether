# Agent Infrastructure Runtime

Phase 1 provides an in-memory Rust runtime for asynchronously executing tasks. It includes managed
agents, priority scheduling, a bounded Tokio worker pool, task lifecycle tracking, cancellation, and a
REST API.

Phase 2 adds the tool system from the runtime spec: `ToolRegistry`, `ToolExecutor`, capability
permissions, timeouts, and retries. Registered tools are `echo`, `file_read`, `http_request`, `shell`,
and `python`. Sandboxing, messaging, persistence, and distributed workers remain later roadmap work.

## Run

```sh
cargo run
```

The API listens on `127.0.0.1:3000` by default. Set `AGENT_RUNTIME_ADDR` to bind a different address.

## Phase 1 API

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/agents` | Create and initialize a ready agent. |
| `GET` | `/agents/{id}` | Retrieve an agent's lifecycle state. |
| `POST` | `/agents/{id}/tasks` | Submit an asynchronous task for an agent. |
| `POST` | `/agents/{id}/tools` | Execute a registered tool using the agent's permissions. |
| `POST` | `/tasks` | Submit an unowned asynchronous task. |
| `GET` | `/tasks/{id}` | Retrieve the latest task state and result. |
| `POST` | `/tasks/{id}/cancel` | Cancel a queued or running task. |
| `GET` | `/runtime/status` | Retrieve queue, task, agent, and worker counts. |
| `GET` | `/tools` | List registered tools and required capabilities. |

Example:

```sh
curl -X POST http://127.0.0.1:3000/agents \
  -H 'content-type: application/json' \
  -d '{"name":"research-agent","permissions":["network_http","filesystem_read"]}'
```

Permissions also accept the dotted names from the spec (`filesystem.read`, `network.http`,
`shell.execute`, `python.execute`).

## Phase 2 tools

`echo` does not need a capability. `file_read` requires `filesystem_read` and can only read text files
inside `AGENT_RUNTIME_TOOL_ROOT` (the current directory by default). `http_request` requires
`network_http` and only connects to exact hostnames in `AGENT_RUNTIME_ALLOWED_HTTP_HOSTS`. Redirects
are disabled so an allowlisted host cannot redirect the request to another destination.

`shell` requires `shell_execute`. It runs an allowlisted program basename with an argument vector
(not `sh -c`) inside the tool root. Interactive shells are rejected until Phase 3 sandbox isolation.
`python` requires `python_execute` and only runs a script file under the tool root.

```sh
AGENT_RUNTIME_TOOL_ROOT="$PWD" \
AGENT_RUNTIME_ALLOWED_HTTP_HOSTS="api.github.com,example.com" \
AGENT_RUNTIME_ALLOWED_SHELL_PROGRAMS="echo,ls" \
AGENT_RUNTIME_PYTHON_BIN="python3" \
cargo run
```

Tool aliases `file.read` and `http.get` remain valid for existing clients.
