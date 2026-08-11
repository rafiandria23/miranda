# Miranda

A Rust-based durable workflow engine. Run task graphs locally with zero
infrastructure, or scale out to a distributed fleet of workers coordinated by
a real control plane, backed by Postgres, MySQL, or SQLite.

Miranda borrows ideas from Temporal (workflow/activity model, retry policy,
worker polling) and Kafka (pull-based consumption, broker-agnostic queueing).
It is deliberately not a from-scratch Kubernetes. It aims to be small,
honest about what it does and doesn't do yet, and built in layers that each
work completely before the next one is added.

## What you get today

- **Run workflows with zero setup.** `miranda run workflow.yaml` executes a
  task DAG in a single process. No server, no network, no database beyond a
  local SQLite file.
- **Scale out when you need to.** `miranda-server` coordinates any number of
  worker processes over gRPC, backed by a real, durable Postgres, MySQL, or
  SQLite database. Proven under concurrent multi-worker load, not just
  single-worker happy paths.
- **Four built-in task types**: `shell`, `http`, `wait`, `noop`. Enough to
  express real automation without needing custom code yet (a plugin/SDK
  system for arbitrary user code is planned, see Roadmap below).
- **A real HTTP API and CLI** for registering workflows, submitting
  executions, and checking status against a running server.

## Quick start: run a workflow locally

```bash
cargo build -p miranda-cli
```

```yaml
# workflow.yaml
name: hello-miranda
timeout: 60

tasks:
  say_hello:
    type: shell
    command: "echo 'hello from Miranda'"

  check:
    type: http
    method: GET
    url: https://postman-echo.com/status/200
    depends_on: [say_hello]

  pause:
    type: wait
    duration: 2
    depends_on: [check]

  finish:
    type: noop
    message: "workflow complete"
    depends_on: [pause]
```

```bash
cargo run -p miranda-cli -- run workflow.yaml
```

That's it. No server, no database setup. Miranda stores execution state in
`~/.miranda/miranda.sqlite` (SQLite) by default.

## Running distributed

Miranda's distributed mode has three roles a process can take on, via two
independent flags:

```bash
# Control plane only. Coordinates workers, exposes gRPC + HTTP APIs.
miranda-server --control-plane --database-url postgres://user:pass@localhost/miranda

# Worker only. Connects to a control plane elsewhere.
miranda-server --worker --control-plane-url http://localhost:7433 \
  --capabilities shell,http,wait,noop

# Both, co-located in one process. Worker talks to the control plane
# in-process, no network hop. The natural shape for a single-node setup.
miranda-server --control-plane --worker --database-url sqlite:///tmp/miranda.sqlite
```

Then, from anywhere that can reach the control plane's HTTP API:

```bash
cargo run -p miranda-cli -- submit workflow.yaml --server http://localhost:8080
cargo run -p miranda-cli -- status <execution-id> --server http://localhost:8080
```

All three SQL backends (Postgres, MySQL, SQLite) are fully supported for
distributed mode, including the task queue and worker registry, which are
durable and shared across the whole database, not held in one process's
memory. A control plane restart doesn't lose in-flight work.

## Architecture, in one paragraph

A workflow is a DAG of tasks (`WorkflowDefinition` to `WorkflowTask`, with
dependencies). An execution is one run of a workflow. In embedded mode,
`EmbeddedEngine` drives an execution directly against a `TaskExecutor`. No
network involved. In distributed mode, a `ControlPlane` coordinates any
number of workers via leases, a durable task queue, and capability-based
routing; `miranda-server` exposes this over gRPC (worker-facing) and HTTP
(client-facing). Both modes share the same domain model (`miranda-core`) and
the same task executors (`miranda-worker`). Nothing about how a `shell` or
`http` task runs changes based on which mode you're in.

For the full crate-by-crate architecture, conventions, and design decisions,
see `MIRANDA_ARCHITECTURE.md`. For current status, what's verified versus
what's merely compiled, and the open backlog, see `MIRANDA_ROADMAP.md`.

## Task types

| Type | Purpose | Key fields |
| --- | --- | --- |
| `shell` | Run a command | `command`, `env`, `cwd`, `shell`, `success_codes` |
| `http` | Make an HTTP request | `method`, `url`, `query`, `headers`, `body`, `success_codes` |
| `wait` | Pause for a duration or until a timestamp | `duration` or `until` |
| `noop` | No-op marker, resolved by the orchestrator without touching a worker | `message` (optional) |

Every task supports `depends_on: [other_task_names]` and an optional
per-task `timeout`, overriding the workflow-level default.

## Project layout

```
crates/
├── core                    # domain model. Execution, WorkflowDefinition, no I/O
├── worker                  # task execution. ShellExecutor, HttpExecutor, etc.
├── storage                 # WorkflowStore/TaskQueueStore/RouterStore traits + in-memory impls
├── storage-postgres        # Postgres implementations
├── storage-mysql           # MySQL implementations
├── storage-sqlite          # SQLite implementations
├── engine                  # embedded-mode orchestration
├── control-plane           # distributed-mode coordination (leases, queue, routing)
├── server                  # the miranda-server binary. gRPC + HTTP
└── cli                     # the miranda-cli binary. run/submit/status/db
```

## Status

Miranda is under active development. Embedded execution and distributed
execution (including durable, SQL-backed coordination across all three
supported databases) are built and verified under real concurrent load. See
`MIRANDA_ROADMAP.md` for exactly what's been run for real versus what's
merely compiled, and what's still ahead, including a Kubernetes-kubeadm-
style init/join deployment flow, streaming task notification, and a future
SDK for running arbitrary user code as a task.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in this project by you shall be dual licensed as
above, without any additional terms or conditions.
