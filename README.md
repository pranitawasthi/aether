# Agent Infrastructure Runtime

Phase 1 provides an in-memory Rust runtime for asynchronously executing tasks. It includes managed
agents, priority scheduling, a bounded Tokio worker pool, task lifecycle tracking, cancellation, and a
REST API.

Phase 2 adds the tool system from the runtime spec: `ToolRegistry`, `ToolExecutor`, capability
permissions, timeouts, retries, and scheduled tool tasks. Registered tools are `echo`, `file_read`, and
`http_request`. Sandboxing, messaging, persistence, and distributed workers remain later roadmap work.

Phase 3 now has a fail-closed Docker sandbox backend. It is disabled unless a runtime-controlled image
is configured, and it never falls back to host-process execution.

Phase 4 provides a runtime-owned memory interface with configurable backends: Redis for working memory,
PostgreSQL for persistent memory, and deterministic vector-style semantic retrieval. The zero-config
default keeps both scopes in memory for local development.

Phase 4 also includes an opt-in PostgreSQL task-storage adapter and migration. The in-memory scheduler
remains the default execution backend until durable runtime wiring and crash recovery are added.

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
| `POST` | `/agents/{id}/memory` | Write or replace an agent-scoped memory record. |
| `GET` | `/agents/{id}/memory` | List active memory records for an agent. |
| `GET` | `/agents/{id}/memory/{key}?scope=working` | Retrieve a scoped memory record. |
| `GET` | `/agents/{id}/memory/search?q=cloud&limit=10` | Semantically retrieve matching memory records. |
| `POST` | `/agents/{id}/tasks` | Submit an asynchronous task for an agent. |
| `POST` | `/agents/{id}/tools` | Queue a permission-checked tool task for an agent. |
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

Tool requests are scheduled like every other task: the endpoint returns `202 Accepted`, then retrieve the
result through `GET /tasks/{id}`. Shell and Python tools are disabled by default until Phase 3 sandbox
isolation. They can only be enabled for local development with the explicit
`AGENT_RUNTIME_ENABLE_HOST_PROCESS_TOOLS=true` opt-in.

## Phase 3 Docker sandbox

Set `AGENT_RUNTIME_DOCKER_IMAGE` to register `sandbox.exec`. It requires the agent's `shell_execute`
capability and runs an allowlisted task inside a Docker container with a read-only workspace mount,
read-only root filesystem, no network, all Linux capabilities dropped, `no-new-privileges`, CPU/memory
limits, a PID limit, an unprivileged user, and a bounded `noexec` temporary filesystem. Docker must be
installed and its daemon must be running.

```sh
AGENT_RUNTIME_DOCKER_IMAGE="alpine:3.21" cargo run
```

```sh
AGENT_RUNTIME_TOOL_ROOT="$PWD" \
AGENT_RUNTIME_ALLOWED_HTTP_HOSTS="api.github.com,example.com" \
cargo run
```

Tool aliases `file.read` and `http.get` remain valid for existing clients.

## Phase 4 memory

Memory records are isolated by agent and scope. `working` is the default scope and may include an optional
`ttl_ms`; expired records are removed on access. By default, both scopes are process-local. Configure Redis
and PostgreSQL to externalize working and persistent memory respectively:

```sh
AGENT_RUNTIME_REDIS_URL="redis://127.0.0.1:6379" \
AGENT_RUNTIME_DATABASE_URL="postgres://user:password@127.0.0.1:5432/agent_runtime" \
cargo run
```

Semantic search uses deterministic hashed-vector cosine similarity across memory values. It is built in and
does not require an embedding API; a dedicated vector-database adapter can replace it without changing the
runtime or HTTP API.

```sh
curl -X POST http://127.0.0.1:3000/agents/{agent_id}/memory \
  -H 'content-type: application/json' \
  -d '{"key":"current_plan","value":["research","report"],"scope":"working","ttl_ms":60000}'
```

## PostgreSQL task storage

`PostgresStorage` is an SDK-facing adapter. Create it with `PostgresStorage::connect`, call `migrate`,
then use the `TaskStorage` trait for task CRUD. It serializes each runtime task as JSONB while preserving
UUID and timestamp columns for indexing. The initial migration is in `migrations/0001_create_tasks.sql`.
