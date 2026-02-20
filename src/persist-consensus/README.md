# persist-consensus

A standalone Raft-backed consensus service for Materialize persist, using
[openraft](https://github.com/databendlabs/openraft).

## Crate layout

| Crate | Description |
|---|---|
| `mz-persist-consensus` | Raft server binary and openraft internals |
| `mz-persist-consensus-client` | gRPC proto definitions and raw client library |
| `mz-persist` (feature `raft`) | `Consensus` trait implementation using the client |

## Building

```bash
# Build the server binary
cargo build -p mz-persist-consensus

# Build environmentd with Raft consensus support
cargo build -p mz-environmentd --features mz-environmentd/raft
```

## Running

### 1. Start the persist-consensus server

```bash
cargo run -p mz-persist-consensus -- \
  --node-id 1 \
  --api-listen-addr 127.0.0.1:6880 \
  --raft-listen-addr 127.0.0.1:6881
```

| Flag | Default | Description |
|---|---|---|
| `--node-id` | *(required)* | Unique node identifier within the Raft cluster |
| `--api-listen-addr` | `0.0.0.0:6880` | External Consensus API (gRPC) |
| `--raft-listen-addr` | `0.0.0.0:6881` | Internal Raft node-to-node RPCs (gRPC) |
| `--peer` | *(none)* | Peer node in format `node_id:raft_addr:api_addr` (repeatable) |

### 2. Start environmentd

```bash
cargo run -p mz-environmentd --features mz-environmentd/raft -- \
  --persist-consensus-url=raft://127.0.0.1:6880 \
  --persist-blob-url=file:///tmp/mz-persist-blob \
  # ... other required environmentd flags
```

The `raft://` URI scheme tells persist to connect to the persist-consensus gRPC
service instead of Postgres/CockroachDB.

### Multi-node cluster example

```bash
# Node 1
cargo run -p mz-persist-consensus -- \
  --node-id 1 \
  --api-listen-addr 127.0.0.1:6880 \
  --raft-listen-addr 127.0.0.1:6881 \
  --peer 2:127.0.0.1:6883:127.0.0.1:6882 \
  --peer 3:127.0.0.1:6885:127.0.0.1:6884

# Node 2
cargo run -p mz-persist-consensus -- \
  --node-id 2 \
  --api-listen-addr 127.0.0.1:6882 \
  --raft-listen-addr 127.0.0.1:6883 \
  --peer 1:127.0.0.1:6881:127.0.0.1:6880 \
  --peer 3:127.0.0.1:6885:127.0.0.1:6884

# Node 3
cargo run -p mz-persist-consensus -- \
  --node-id 3 \
  --api-listen-addr 127.0.0.1:6884 \
  --raft-listen-addr 127.0.0.1:6885 \
  --peer 1:127.0.0.1:6881:127.0.0.1:6880 \
  --peer 2:127.0.0.1:6883:127.0.0.1:6882
```

## Tests

```bash
cargo test -p mz-persist-consensus
cargo test -p mz-persist-consensus-client
```

## Architecture

- **Writes** (`compare_and_set`, `truncate`) go through Raft for linearizable
  consensus.
- **Reads** (`head`, `scan`, `list_keys`) are served directly from the local
  state machine replica without a Raft round-trip.
- **Raft node-to-node communication** uses gRPC with serde JSON payloads in
  opaque proto bytes.
- **Storage** is currently in-memory. The state machine mirrors the logic from
  `MemConsensus` in `mz_persist::mem`.
