# examples

Runnable examples that drive LEDGERD through the client SDK. The quickest way to
see the whole system work end to end.

## Contents

- [QuickStartExample.java](src/main/java/io/justrade/ledgerd/examples/QuickStartExample.java) -
  boots an in-process single-node cluster and drives commands through the write
  client.
- [BatchTransferExample.java](src/main/java/io/justrade/ledgerd/examples/BatchTransferExample.java) -
  drives transfer batches: independent legs, an atomic linked chain, and a
  linked chain that rolls back (ADR 0012).
- [RemoteClientExample.java](src/main/java/io/justrade/ledgerd/examples/RemoteClientExample.java) -
  connects to a running cluster and submits commands plus reads results.
- [ReadClientExample.java](src/main/java/io/justrade/ledgerd/examples/ReadClientExample.java) -
  boots a write cluster plus a read replica in-process, then reads balances,
  allowances, and supply through the read-client SDK.

## Run

```bash
./gradlew :examples:run                                                  # QuickStartExample (default)
./gradlew :examples:run -PmainClass=io.justrade.ledgerd.examples.BatchTransferExample
./gradlew :examples:run -PmainClass=io.justrade.ledgerd.examples.RemoteClientExample
./gradlew :examples:run -PmainClass=io.justrade.ledgerd.examples.ReadClientExample
```

The example configuration sets the required `--add-opens` JVM flags
automatically.

## Related

- Step-by-step setup: [../README.md](../README.md).
- Write SDK: [../write-client/README.md](../write-client/README.md).
- Read SDK: [../read-client/README.md](../read-client/README.md).
