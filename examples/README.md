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

## Deployment verification tools

These are not walkthrough demos; they are driven by the Docker and AWS deploy
harnesses (`docker/`, `deploy/aws/`) rather than `:examples:run`.

- [LoadGenerator.java](src/main/java/io/justrade/ledgerd/examples/LoadGenerator.java) -
  single-command load driver for the Docker smoke/fault harness (fixed
  account-100 -> account-200 scenario).
- [BatchLoadGenerator.java](src/main/java/io/justrade/ledgerd/examples/BatchLoadGenerator.java) -
  transfer-batch load driver for the Docker harness (fixed two-account scenario).
- [ReadCheck.java](src/main/java/io/justrade/ledgerd/examples/ReadCheck.java) -
  read-replica convergence check. Verifies the fixed 100/200 balances by
  default; `LEDGERD_SKIP_BALANCES=1` verifies total supply only, which is the
  scale-independent check used by the multi-account drivers.
- [BenchAggregator.java](src/main/java/io/justrade/ledgerd/examples/BenchAggregator.java) +
  [BenchHistogram.java](src/main/java/io/justrade/ledgerd/examples/BenchHistogram.java) -
  merge per-client HdrHistogram logs into one combined percentile set.
- [CapacityFillDriver.java](src/main/java/io/justrade/ledgerd/examples/CapacityFillDriver.java) -
  fills the balance maps with a hot/cold account distribution across assets, to
  validate the memory model in `docs/CAPACITY-PLANNING.md` (driven by
  `ledgerd-fill`).
- [ParetoSampler.java](src/main/java/io/justrade/ledgerd/examples/ParetoSampler.java) -
  deterministic two-bucket hot/cold account sampler shared by the Pareto drivers.
- [ParetoLoadGenerator.java](src/main/java/io/justrade/ledgerd/examples/ParetoLoadGenerator.java) -
  multi-account Pareto single-command bench driver (driven by `ledgerd-bench` on
  AWS). Funds every account on every asset, then drives one-unit transfers
  between Pareto-sampled accounts.
- [ParetoBatchGenerator.java](src/main/java/io/justrade/ledgerd/examples/ParetoBatchGenerator.java) -
  multi-account Pareto transfer-batch bench driver (driven by `ledgerd-batch` on
  AWS).

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
