# protocol

The wire contract for LEDGERD: the SBE (Simple Binary Encoding) schema and the
generated flyweight codecs that every other module uses to talk to the cluster
and the read replica. This module is the contract only; it holds no business
logic.

## Responsibility

- Define all on-wire and IPC messages in an SBE schema.
- Generate zero-copy flyweight encoders and decoders (no reflection, little
  endian, fixed binary layout with backward-compatible optional fields).
- Provide stream identifiers and shared wire constants.

## Key contents

- [src/main/resources/messages.xml](src/main/resources/messages.xml) - the SBE
  schema (source of truth for the wire format).
- Generated codecs under the module build output (encoders/decoders for
  `CommandEnvelope`, `CommandResult`, snapshot records, domain event journal
  records, and `QueryRequest` / `QueryResponse`).
- [QueryStreams.java](src/main/java/io/justrade/ledgerd/protocol/QueryStreams.java) -
  default channel and stream ids for the read-side query protocol.

## Message families

- Ingress: `CommandEnvelope` (command plus correlation: clientId, clientSeq,
  commandId, assetId) and `TransferBatch` (a group of transfer legs with a
  `linked` flag, ADR 0012).
- Egress: `CommandResult` (deterministic result for exactly one command) and
  `TransferBatchResult` (one result per leg, ADR 0012).
- Snapshot: `SnapshotHeader`, `BalanceEntry`, `AllowanceEntry`, `DedupEntry`,
  `AssetSupplyEntry`, `BatchDedupEntry`, `SnapshotFooter`.
- Domain event journal (ADR 0011): `BalanceChangedEvent`, `ReservedEvent`,
  `CapturedEvent`, `ReleasedEvent`, `TransferEvent`, `AllowanceChangedEvent`,
  `CommandRejectedEvent`.
- Read path: `QueryRequest`, `QueryResponse`.

## Design notes

- Flyweights hold no state: `wrap(buffer, offset, blockLength, version)` per
  message, decode fields in place, never build an intermediate POJO.
- Little endian only, matching SBE default and native x86/ARM order.
- Schema evolution is via optional fields, so newer and older peers interoperate.

## Related

- Wire and snapshot format details: [../docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md).
- Command and status code reference: [../docs/API-REFERENCE.md](../docs/API-REFERENCE.md).
