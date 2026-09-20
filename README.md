# Agent Infrastructure Runtime

Agent Infrastructure Runtime is a small Rust control plane for running agent work safely and
observably. It gives you the pieces most agent applications need before they become production
systems: agents with explicit permissions, queued tasks, tool execution, sandboxed process execution,
memory, runtime events, direct agent messages, and optional distributed workers backed by PostgreSQL.

The project is intentionally usable in two modes:

- Local mode: one `cargo run` starts the API, in-memory scheduler, and worker pool. This is the
  default and is best for development.
- Durable mode: the API persists agents and queues tasks in PostgreSQL, while separate worker
  processes claim and execute tasks with lease tokens. This is the mode to use when you want crash
  recovery or multiple workers.

Attached specs and design documents describe the product direction, but this README is the operating
guide for the code in this repository.

## What It Does

The runtime exposes an HTTP API for:

- creating agents and assigning capabilities such as `filesystem_read`, `network_http`, and
  `shell_execute`;
- submitting generic asynchronous tasks;
- running tools through permission checks, timeouts, and retries;
- storing and searching agent-scoped memory;
- sending direct messages between registered agents;
- reading runtime status and recent events;
- scaling execution across separate durable worker processes.

The default executable uses a mock generic task executor, so generic tasks are useful for exercising
the runtime lifecycle. Tool tasks are real: `echo`, `file_read`, `http_request`, and, when configured,
host process tools or Docker sandbox execution.

## Quick Start

Run the local development server:

```sh
cargo run
```

The API listens on `127.0.0.1:3000` unless `AGENT_RUNTIME_ADDR` is set.

Create an agent:

```sh
curl -s -X POST http://127.0.0.1:3000/agents \
  -H 'content-type: application/json' \
  -d '{"name":"research-agent","permissions":["filesystem_read","network_http"]}'
```

Run the built-in `echo` tool for that agent:

```sh
curl -s -X POST http://127.0.0.1:3000/agents/{agent_id}/tools \
  -H 'content-type: application/json' \
  -d '{"tool":"echo","arguments":{"message":"hello runtime"}}'
```

The tool endpoint returns a task immediately. Poll the returned task ID until it reaches a terminal
state:

```sh
curl -s http://127.0.0.1:3000/tasks/{task_id}
```

Check runtime health:

```sh
curl -s http://127.0.0.1:3000/runtime/status
```

## API

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/agents` | Create a ready agent. |
| `GET` | `/agents/{id}` | Get an agent. |
| `POST` | `/agents/{id}/tasks` | Submit an asynchronous task owned by an agent. |
| `POST` | `/agents/{id}/tools` | Queue a permission-checked tool task. |
| `POST` | `/agents/{id}/memory` | Write or replace an agent-scoped memory record. |
| `GET` | `/agents/{id}/memory` | List active memory records for an agent. |
| `GET` | `/agents/{id}/memory/{key}?scope=working` | Get one memory record. |
| `GET` | `/agents/{id}/memory/search?q=plan&limit=10` | Search memory semantically. |
| `POST` | `/agents/{id}/messages` | Send a direct message from one agent to another. |
| `GET` | `/agents/{id}/messages?limit=100` | List an agent's inbox. |
| `POST` | `/tasks` | Submit an unowned asynchronous task. |
| `GET` | `/tasks/{id}` | Get task state and result. |
| `POST` | `/tasks/{id}/cancel` | Cancel a queued or running task. |
| `GET` | `/runtime/status` | Get queue, task, agent, worker, and sandbox status. |
| `GET` | `/runtime/events?limit=100` | List recent runtime events. |
| `GET` | `/tools` | List registered tools and required capabilities. |

### Generic Tasks

```sh
curl -s -X POST http://127.0.0.1:3000/tasks \
  -H 'content-type: application/json' \
  -d '{"name":"demo-task","payload":{"work":"example"},"priority":"Normal"}'
```

### Memory

Memory is isolated by agent and scope. `working` memory is the default and can expire with `ttl_ms`.
`persistent` memory is intended for longer-lived records. Search uses a deterministic built-in
vector-style similarity function, so it does not need an embedding service.

```sh
curl -s -X POST http://127.0.0.1:3000/agents/{agent_id}/memory \
  -H 'content-type: application/json' \
  -d '{"key":"current_plan","value":["research","report"],"scope":"working","ttl_ms":60000}'
```

```sh
curl -s 'http://127.0.0.1:3000/agents/{agent_id}/memory/search?q=research&scope=working&limit=5'
```

### Messages

Messages are direct and bounded. The recipient must already exist, and messages do not grant
permissions or automatically start work.

```sh
curl -s -X POST http://127.0.0.1:3000/agents/{sender_id}/messages \
  -H 'content-type: application/json' \
  -d '{"to_agent_id":"{recipient_id}","topic":"handoff","payload":{"task":"review"}}'
```

## Tools And Permissions

Tool execution always checks the target agent's permissions before running. Permission names can use
underscores or the dotted aliases from the spec, for example `filesystem_read` or `filesystem.read`.

| Tool | Capability | Notes |
| --- | --- | --- |
| `echo` | none | Returns the supplied JSON arguments. |
| `file_read`, `file.read` | `filesystem_read` | Reads UTF-8 files under `AGENT_RUNTIME_TOOL_ROOT`. |
| `http_request`, `http.get` | `network_http` | Allows only exact hosts from `AGENT_RUNTIME_ALLOWED_HTTP_HOSTS`; redirects are disabled. |
| `shell` | `shell_execute` | Disabled unless `AGENT_RUNTIME_ENABLE_HOST_PROCESS_TOOLS=true`. Runs only allowlisted programs. |
| `python` | `python_execute` | Disabled unless `AGENT_RUNTIME_ENABLE_HOST_PROCESS_TOOLS=true`. Runs scripts under the tool root. |
| `sandbox.exec` | `shell_execute` | Enabled when `AGENT_RUNTIME_DOCKER_IMAGE` is set. Runs inside a locked-down Docker container. |

Read a file from the configured tool root:

```sh
AGENT_RUNTIME_TOOL_ROOT="$PWD" cargo run
```

```sh
curl -s -X POST http://127.0.0.1:3000/agents/{agent_id}/tools \
  -H 'content-type: application/json' \
  -d '{"tool":"file_read","arguments":{"path":"README.md"}}'
```

Allow outbound HTTP to selected hosts:

```sh
AGENT_RUNTIME_ALLOWED_HTTP_HOSTS="api.github.com,example.com" cargo run
```

```sh
curl -s -X POST http://127.0.0.1:3000/agents/{agent_id}/tools \
  -H 'content-type: application/json' \
  -d '{"tool":"http_request","arguments":{"url":"https://example.com"}}'
```

## Docker Sandbox

Set `AGENT_RUNTIME_DOCKER_IMAGE` to register `sandbox.exec`. Docker must be installed, the daemon must
be running, and the image must already be available or pullable by Docker.

```sh
docker pull alpine:3.21

AGENT_RUNTIME_TOOL_ROOT="$PWD" \
AGENT_RUNTIME_DOCKER_IMAGE="alpine:3.21" \
cargo run
```

The sandbox backend mounts the tool root read-only, disables network access, drops Linux capabilities,
uses `no-new-privileges`, applies CPU and memory limits, sets a PID limit, runs as an unprivileged user,
and uses a bounded `noexec` temporary filesystem. If the sandbox is not configured, `sandbox.exec` is
not registered.

## Persistent Memory

By default, memory is process-local. Set Redis and/or PostgreSQL URLs to externalize it:

```sh
AGENT_RUNTIME_REDIS_URL="redis://127.0.0.1:6379" \
AGENT_RUNTIME_DATABASE_URL="postgres://user:password@127.0.0.1:5432/agent_runtime" \
cargo run
```

Redis backs working memory. PostgreSQL backs persistent memory and is also used for migrations and
durable task queues when durable mode is enabled.

## Durable Workers

Durable mode separates the API process from worker processes. The API stores agents and enqueues work in
PostgreSQL. Workers atomically claim tasks with `SKIP LOCKED`, renew leases while work is running, and
acknowledge terminal task snapshots with an unguessable lease token.

Start the API/control-plane process:

```sh
AGENT_RUNTIME_DURABLE_QUEUE=true \
AGENT_RUNTIME_DATABASE_URL="postgres://user:password@127.0.0.1:5432/agent_runtime" \
cargo run
```

Start one or more worker processes in separate terminals:

```sh
AGENT_RUNTIME_WORKER_ID="worker-1" \
AGENT_RUNTIME_DATABASE_URL="postgres://user:password@127.0.0.1:5432/agent_runtime" \
cargo run
```

Do not set `AGENT_RUNTIME_DURABLE_QUEUE` on worker processes. Worker mode is selected by
`AGENT_RUNTIME_WORKER_ID`, and it does not bind the HTTP listener.

In durable mode, `GET /runtime/status` includes `durable_workers`, counted from PostgreSQL heartbeats
seen in the last 90 seconds.

## Configuration

| Variable | Purpose |
| --- | --- |
| `AGENT_RUNTIME_ADDR` | API bind address. Default: `127.0.0.1:3000`. |
| `AGENT_RUNTIME_TOOL_ROOT` | Root directory for file, shell, Python, and Docker-sandbox tools. Default: current directory. |
| `AGENT_RUNTIME_ALLOWED_HTTP_HOSTS` | Comma-separated host allowlist for `http_request`. |
| `AGENT_RUNTIME_ENABLE_HOST_PROCESS_TOOLS` | Enables host `shell` and `python` tools when set to `true`, `TRUE`, or `1`. |
| `AGENT_RUNTIME_ALLOWED_SHELL_PROGRAMS` | Comma-separated allowlist for the host `shell` tool. |
| `AGENT_RUNTIME_PYTHON_BIN` | Python executable for the host `python` tool. Default: `python3`. |
| `AGENT_RUNTIME_DOCKER_IMAGE` | Enables `sandbox.exec` using this Docker image. |
| `AGENT_RUNTIME_REDIS_URL` | Redis URL for working memory. |
| `AGENT_RUNTIME_DATABASE_URL` | PostgreSQL URL for persistent memory, migrations, durable queue, and workers. |
| `AGENT_RUNTIME_DATABASE_MAX_CONNECTIONS` | PostgreSQL pool size. Default: `5`. |
| `AGENT_RUNTIME_DURABLE_QUEUE` | Enables durable queue mode on the API process. |
| `AGENT_RUNTIME_WORKER_ID` | Starts this process as a durable worker instead of an API server. |

## Development

Run the standard checks:

```sh
cargo fmt --check
cargo test
cargo clippy -- -D warnings
```

PostgreSQL and Redis integration tests are opt-in:

```sh
AGENT_RUNTIME_TEST_DATABASE_URL="postgres://user:password@127.0.0.1:5432/agent_runtime_test" \
AGENT_RUNTIME_TEST_REDIS_URL="redis://127.0.0.1:6379" \
cargo test
```

Migrations live in `migrations/`. They create task storage, memory storage, durable queue leases,
persisted agents, and worker heartbeats.
