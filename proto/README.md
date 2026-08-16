# Miranda proto

```
proto/
└── miranda/
    ├── worker/
    │   └── v1/
    │       └── worker.proto
    └── control_plane/
        └── v1/
            └── control_plane.proto
```

## Layout convention

`proto/<package>/<service-family>/<version>/<file>.proto` — one folder per
service family, versioned, matching each file's own `package
miranda.<service_family>.v1;` declaration. A new service gets a new
sibling folder at the same level as `worker/`/`control_plane/`, not a new
file dropped into an existing one.

Folder and package names use underscores, not hyphens
(`control_plane`, not `control-plane`) — this isn't a style choice, it's
required: protobuf package names and the Rust module paths generated from
them don't accept hyphens as identifier characters. This is also why
`control_plane` here doesn't match the crate name `control-plane` — crate
names follow Cargo's own hyphen convention, proto packages follow
protobuf's.

## `worker/v1/worker.proto` — `WorkerService`

Worker-facing. Everything a worker calls on the control plane it's
connected to:

- `Register` / `Deregister`
- `Heartbeat`
- `PollTask` / `ReportResult`
- `SubscribeToTasks` — server-streaming, deliberately minimal
  (`TaskNotification` carries no payload). A wake-up signal only, telling a
  subscribed worker "something may be ready, go call `PollTask`" — never a
  task assignment itself. This is what keeps the design pull-shaped: direct
  server-initiated push (the control plane handing a worker a task over the
  stream) was considered and rejected. `PollTask` remains the only way a
  task is ever actually claimed.

Every RPC identifies its caller by `worker_id`; `Register` additionally
carries the join token issued by the control plane's `init` step, checked
before registration is allowed to proceed.

## `control_plane/v1/control_plane.proto` — `ControlPlaneService`

Peer-facing, not worker-facing — this is how multiple control-plane
instances sharing one database coordinate with each other for high
availability, not something a worker or the CLI ever calls.

- `NotifyReady` — one RPC, both messages empty. An instance that just
  enqueued a dispatchable task calls this on every peer it currently knows
  about, so a worker subscribed to a *different* instance still gets woken
  promptly. Deliberately storage-agnostic: this never touches
  `WorkflowStore`/`TaskQueueStore`/etc, so adding a new database backend
  later never requires touching this service. Peer discovery itself
  happens through the database (a heartbeated `control_plane_instances`
  table), but the notification is a direct call between processes, not
  something routed through storage.

  A `NotifyReady` handler must only wake its own local subscribers, never
  re-broadcast to its own peers — that distinction is the difference
  between correct fan-out and an infinite loop between two instances (a
  real bug this project hit once; see `MIRANDA_ROADMAP.md`).

## Codegen

Compiled by `crates/server/build.rs` via `tonic-prost-build`, both files
in one `compile_protos([...])` call. Generated code is included at
build time via `tonic::include_proto!("miranda.worker.v1")` /
`tonic::include_proto!("miranda.control_plane.v1")`, each inside its
matching `crates/server/src/grpc/<service>/service.rs` — see
`crates/README.md` for that layout.
