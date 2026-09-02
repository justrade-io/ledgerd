# examples

Runnable examples that drive LEDGERD through the client SDK. The quickest way to
see the whole system work end to end.

## Contents

- [QuickStartExample.java](src/main/java/io/justrade/ledgerd/examples/QuickStartExample.java) -
  boots an in-process single-node cluster and drives commands through the write
  client.
- [RemoteClientExample.java](src/main/java/io/justrade/ledgerd/examples/RemoteClientExample.java) -
  connects to a running cluster and submits commands plus reads results.

## Run

```bash
./gradlew :examples:run
```

The example configuration sets the required `--add-opens` JVM flags
automatically.

## Related

- Step-by-step setup: [../README.md](../README.md).
- Write SDK: [../write-client/README.md](../write-client/README.md).
