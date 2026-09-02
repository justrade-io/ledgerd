# write-client

The write-side SDK: submit commands to the cluster and receive deterministic
results. It depends only on `protocol` (no `core`), so it is a thin, embeddable
client.

## Responsibility

- Submit `CommandEnvelope` messages to the cluster ingress.
- Handle leader changes and reconnects transparently.
- Provide idempotent retry via `(clientId, commandId)` correlation, so a retry
  after a timeout or failover cannot double-apply.
- Deliver `CommandResult` messages with explicit backpressure signalling.

## Key classes

- [WriteClient.java](src/main/java/io/justrade/ledgerd/write/client/WriteClient.java) -
  the client: connect, submit, poll.
- [ResultHandler.java](src/main/java/io/justrade/ledgerd/write/client/ResultHandler.java) -
  callback for command results.
- [PendingCommand.java](src/main/java/io/justrade/ledgerd/write/client/PendingCommand.java) -
  in-flight command tracking for correlation and retry.
- [config/ClientConfig.java](src/main/java/io/justrade/ledgerd/write/client/config/ClientConfig.java) -
  immutable client configuration.

## Usage shape

Submit a command and poll for results in a closed loop:

```java
try (WriteClient client = new WriteClient(config, handler)) {
    long commandId = client.submit(CommandType.CREDIT, 100L, 0L, 0L, 500L);
    while (running) {
        client.poll();   // drives result callbacks, correlated by commandId
    }
}
```

See [../examples/](../examples/) for a runnable end-to-end use and
[../docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md) for the retry and correlation
model.

## Related

- Exactly-once semantics: [../docs/decisions/](../docs/decisions/).
- Wire format: [../protocol/README.md](../protocol/README.md).
