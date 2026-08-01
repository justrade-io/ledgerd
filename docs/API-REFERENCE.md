# ADBE API Reference

ADBE (Aeron Deterministic Balance Engine) is a replicated, allocation-free balance engine built on Aeron
Cluster. Every command is committed via Raft, applied in total order on a single thread, and acknowledged
with a deterministic `StatusCode`. This document covers the complete client integration surface.

---

## Table of Contents

1. [Quick Start](#1-quick-start)
2. [Connecting to a Cluster](#2-connecting-to-a-cluster)
3. [ClientConfig Reference](#3-clientconfig-reference)
4. [ResultHandler Contract](#4-resulthandler-contract)
5. [Backpressure Handling](#5-backpressure-handling)
6. [Submit / Poll Event Loop Patterns](#6-submit--poll-event-loop-patterns)
7. [Use Cases](#7-use-cases)
8. [Commands Reference](#8-commands-reference)
9. [Status Codes](#9-status-codes)
10. [Observability](#10-observability)
11. [Direct Engine Usage (Headless)](#11-direct-engine-usage-headless)
12. [Numeric Conventions](#12-numeric-conventions)
13. [Read API (HTTP)](#13-read-api-http)

For cluster deployment, snapshot management, and observability see [OPERATIONS.md](OPERATIONS.md).

---

## 1. Quick Start

The fastest path: boot an in-process single-node cluster, connect a client, submit a credit and a
transfer, and drain results.

```java
import com.adbe.client.AdbeClient;
import com.adbe.client.config.ClientConfig;
import com.adbe.config.CoreConfig;
import com.adbe.launcher.ClusterConfig;
import com.adbe.launcher.ClusterNode;
import com.adbe.protocol.CommandType;

import java.nio.file.Files;
import java.nio.file.Path;

Path baseDir = Files.createTempDirectory("adbe-");
ClusterConfig clusterConfig = ClusterConfig.singleNodeLocalhost(0, baseDir);

try (ClusterNode node = new ClusterNode(clusterConfig, CoreConfig.defaults())) {

    long[] lastIdLo = {-1L};
    long[] lastBalance = {Long.MIN_VALUE};

    ClientConfig config = ClientConfig
            .builder(1L, ClusterConfig.ingressEndpoints(1))
            .build();

    try (AdbeClient client = new AdbeClient(config,
            (idHi, idLo, status, balance, hasBalance, allowance, hasAllowance) -> {
                lastIdLo[0] = idLo;
                if (hasBalance) lastBalance[0] = balance;
                System.out.println(status + " balance=" + (hasBalance ? balance : "n/a"));
            })) {

        // CREDIT account 100 with 500 units.
        long creditId = client.submit(CommandType.CREDIT, 100L, 0L, 0L, 500L);
        awaitResult(client, creditId, lastIdLo);   // resultBalance = 500

        // TRANSFER 150 from account 100 to account 200.
        long transferId = client.submit(CommandType.TRANSFER, 100L, 200L, 0L, 150L);
        awaitResult(client, transferId, lastIdLo); // resultBalance = 350 (sender)
    }
}

static void awaitResult(AdbeClient client, long commandIdLo, long[] lastIdLo) {
    long deadline = System.currentTimeMillis() + 15_000L;
    while (System.currentTimeMillis() < deadline) {
        client.poll();
        if (lastIdLo[0] == commandIdLo) return;
        Thread.onSpinWait();
    }
    throw new IllegalStateException("timeout waiting for commandIdLo=" + commandIdLo);
}
```

Run the full version directly:

```bash
./gradlew :adbe-examples:run
```

---

## 2. Connecting to a Cluster

### Embedded media driver (default)

When `aeronDirectoryName` is not set, `AdbeClient` launches its own embedded `MediaDriver`. This is
correct for standalone processes and the examples. The driver is shut down when the client is closed.

```java
ClientConfig config = ClientConfig
        .builder(clientId, "0=localhost:20100,1=localhost:20200,2=localhost:20300")
        .build(); // aeronDirectoryName = null -> embedded driver
```

### Attach to an existing media driver

If your process already runs a `MediaDriver` (e.g. via `MediaDriver.launchEmbedded`), pass its
directory to avoid launching a second driver on the same machine.

```java
MediaDriver driver = MediaDriver.launchEmbedded(...);

ClientConfig config = ClientConfig
        .builder(clientId, ingressEndpoints)
        .aeronDirectoryName(driver.aeronDirectoryName())
        .build();
```

### Remote / Docker

When the client and cluster nodes are on different hosts (separate containers), the cluster needs a
routable address to send results back. Override `egressChannel` with the client's own host address.

```bash
# docker-compose environment variables
ADBE_INGRESS_ENDPOINTS=0=adbe-node-0:20100,1=adbe-node-1:20200,2=adbe-node-2:20300
ADBE_EGRESS_ENDPOINT=client-host:0          # ephemeral port; host must be reachable by nodes
ADBE_CLIENT_ID=1
```

```java
String ingressEndpoints = System.getenv("ADBE_INGRESS_ENDPOINTS");
String egressEndpoint   = System.getenv("ADBE_EGRESS_ENDPOINT");  // may be null for co-located

ClientConfig.Builder builder = ClientConfig.builder(clientId, ingressEndpoints);
if (egressEndpoint != null && !egressEndpoint.isBlank()) {
    builder.egressChannel("aeron:udp?endpoint=" + egressEndpoint);
}
ClientConfig config = builder.build();
```

See `adbe-examples/RemoteClientExample.java` and `docker/client-entrypoint.sh` for a full example
driving the three-node `docker-compose.yml` cluster.

---

## 3. ClientConfig Reference

All configuration is set via `ClientConfig.builder(clientId, ingressEndpoints)`.

| Option                  | Type     | Default                             | Notes                                                                                       |
|-------------------------|----------|-------------------------------------|---------------------------------------------------------------------------------------------|
| `clientId`              | `long`   | required                            | Session identity; minted by Edge after authentication. Used as `commandIdHi`.               |
| `ingressEndpoints`      | `String` | required                            | Aeron cluster client form: `0=host:port,1=host:port,...`                                    |
| `aeronDirectoryName`    | `String` | `null`                              | `null` launches an embedded `MediaDriver`. Set to attach to a pre-existing driver.          |
| `egressChannel`         | `String` | `aeron:udp?endpoint=localhost:0`    | Result channel the client binds. Override with a routable address when client is off-host.  |
| `messageTimeoutNs`      | `long`   | `30_000_000_000` (30 s)             | Aeron cluster session message timeout.                                                      |
| `retryBackoffNs`        | `long`   | `250_000_000` (250 ms)              | Delay before retransmitting an unacknowledged command.                                      |
| `maxRetries`            | `int`    | `0`                                 | `0` = retry indefinitely. Any positive value caps retransmit attempts and then drops.       |
| `maxInFlight`           | `int`    | `1024`                              | In-flight command window size. `submit()` throws `BackpressureException` when full.         |

---

## 4. ResultHandler Contract

`ResultHandler` is a `@FunctionalInterface` invoked on the polling thread when a `CommandResult`
arrives and is matched to its pending command by `commandIdLo`.

```java
@FunctionalInterface
public interface ResultHandler {
    void onResult(
            long commandIdHi,     // high 64-bit word of the 128-bit command id
            long commandIdLo,     // low 64-bit word; matches the value returned by submit()
            StatusCode status,    // deterministic result; never null
            long resultBalance,   // only valid when hasBalance == true
            boolean hasBalance,
            long resultAllowance, // only valid when hasAllowance == true
            boolean hasAllowance);
}
```

**Threading rule**: `onResult` is called from the same thread that calls `poll()`. Implementations
must not block or perform I/O. Hand off to an off-heap queue or ring buffer if downstream work is
needed.

**Field presence by command**:

| Command               | `hasBalance` | `hasAllowance` | Value when present                        |
|-----------------------|:------------:|:--------------:|-------------------------------------------|
| `CREDIT`              | yes          | no             | `resultBalance` = new account balance     |
| `DEBIT`               | yes          | no             | `resultBalance` = new account balance     |
| `TRANSFER`            | yes          | no             | `resultBalance` = sender's new balance    |
| `APPROVE`             | no           | yes            | `resultAllowance` = new allowance         |
| `INCREASE_ALLOWANCE`  | no           | yes            | `resultAllowance` = updated allowance     |
| `DECREASE_ALLOWANCE`  | no           | yes            | `resultAllowance` = updated allowance     |
| `DELEGATED_TRANSFER`  | yes          | yes            | owner's new balance; remaining allowance  |
| Any error status      | no           | no             | both fields absent                        |

---

## 5. Backpressure Handling

`submit()` throws `BackpressureException` when the in-flight window (`maxInFlight`) is full. This is
an explicit signal - the command is never silently dropped. The correct response is to drain
acknowledgements via `poll()` and retry.

```java
// Safe, allocation-free submit loop used under sustained load.
void submitWithBackpressure(AdbeClient client, CommandType type,
        long a, long b, long c, long amount) {
    while (true) {
        try {
            client.submit(type, a, b, c, amount);
            return;
        } catch (BackpressureException e) {
            client.poll(); // drain acks to free window slots
        }
    }
}
```

**When to increase `maxInFlight`**: if `client.backpressureEvents()` grows under normal load,
increase `maxInFlight` in `ClientConfig`. The pool is pre-allocated at construction so there is no
runtime allocation cost.

---

## 6. Submit / Poll Event Loop Patterns

`AdbeClient` is not thread-safe. `submit()` and `poll()` must be called from the same thread.

### Single-threaded drain loop

Suitable for request/response scenarios and examples.

```java
long id = client.submit(CommandType.TRANSFER, 100L, 200L, 0L, 50L);

// Block until this specific command completes.
long deadline = System.currentTimeMillis() + 15_000L;
while (System.currentTimeMillis() < deadline) {
    client.poll();
    if (resultReceived) break; // set by ResultHandler
    Thread.onSpinWait();
}
```

### Agrona Agent integration

For hot-path services, embed the client inside an `Agent.doWork()` loop pinned to a dedicated core.
Submit is called when new commands are available from an upstream ring buffer; `poll()` runs every
work cycle to drive egress and retransmission.

```java
public int doWork() {
    int work = client.poll();           // drives egress delivery and timed retransmit

    CommandSlot slot;
    while ((slot = inboundRing.poll()) != null) {
        submitWithBackpressure(client,
                slot.type, slot.accountA, slot.accountB, slot.accountC, slot.amount);
        work++;
    }
    return work;
}
```

**Idle strategy**: use `BusySpinIdleStrategy` on dedicated isolated cores, `YieldingIdleStrategy`
for adjacent paths, `BackoffIdleStrategy` for background agents.

---

## 7. Use Cases

### A. Onboard an account (CREDIT)

```
CREDIT creates the account automatically on first call.
No separate "create account" command exists.
```

```java
// First CREDIT: account 100 is created with balance 500.
long id = client.submit(CommandType.CREDIT, 100L, 0L, 0L, 500L);
// ResultHandler: status=SUCCESS, hasBalance=true, resultBalance=500

// Subsequent CREDIT adds to existing balance.
long id2 = client.submit(CommandType.CREDIT, 100L, 0L, 0L, 200L);
// ResultHandler: status=SUCCESS, hasBalance=true, resultBalance=700
```

Error cases: `INVALID_AMOUNT` (amount < 0), `OVERFLOW` (balance would exceed `Long.MAX_VALUE`).

---

### B. Move funds (TRANSFER)

```
Sender must exist. Recipient is created automatically if absent.
Total supply is conserved: sender loses amount, recipient gains amount.
```

```java
// Credit sender first (account 100 = 500).
client.submit(CommandType.CREDIT, 100L, 0L, 0L, 500L);

// Transfer 150 from 100 to 200; account 200 is created if new.
long id = client.submit(CommandType.TRANSFER, 100L, 200L, 0L, 150L);
// ResultHandler: status=SUCCESS, hasBalance=true, resultBalance=350 (sender's new balance)
// Account 200 now has balance 150 (not reflected in this result).
```

Self-transfer (`accountA == accountB`) is a no-op: returns `SUCCESS` with `resultBalance` = current
balance, total supply unchanged.

Error cases: `INVALID_AMOUNT`, `INVALID_ACCOUNT` (sender does not exist),
`INSUFFICIENT_BALANCE` (sender balance < amount), `OVERFLOW` (recipient would overflow).

---

### C. Spend on behalf - ERC-20 allowance lifecycle

```
Owner grants a delegate permission to spend up to a limit.
APPROVE sets an absolute cap. DELEGATED_TRANSFER consumes from that cap.
```

```java
// 1. Credit owner.
client.submit(CommandType.CREDIT, /*owner*/ 1L, 0L, 0L, 1_000L);

// 2. Owner approves delegate 9 to spend up to 200.
long approveId = client.submit(CommandType.APPROVE, /*owner*/ 1L, /*delegate*/ 9L, 0L, 200L);
// ResultHandler: status=SUCCESS, hasAllowance=true, resultAllowance=200

// 3. Delegate spends 75 from owner's account into recipient 2.
long dtId = client.submit(CommandType.DELEGATED_TRANSFER,
        /*delegate*/ 9L, /*owner*/ 1L, /*recipient*/ 2L, /*amount*/ 75L);
// ResultHandler: status=SUCCESS,
//   hasBalance=true,   resultBalance=925   (owner's new balance)
//   hasAllowance=true, resultAllowance=125  (remaining allowance)

// 4. Increase allowance by 50 (125 -> 175).
client.submit(CommandType.INCREASE_ALLOWANCE, 1L, 9L, 0L, 50L);
// ResultHandler: status=SUCCESS, hasAllowance=true, resultAllowance=175

// 5. Reduce allowance by 25 (175 -> 150).
client.submit(CommandType.DECREASE_ALLOWANCE, 1L, 9L, 0L, 25L);
// ResultHandler: status=SUCCESS, hasAllowance=true, resultAllowance=150
```

> **Allowance default**: an owner/delegate pair that has never been approved has an implicit
> allowance of `0`. `INCREASE_ALLOWANCE` can be called before `APPROVE` - it starts from `0`.

`DELEGATED_TRANSFER` validation order (stops at first failure):
`INVALID_AMOUNT` -> `INSUFFICIENT_ALLOWANCE` -> `INVALID_ACCOUNT` (owner) ->
`INSUFFICIENT_BALANCE` -> `OVERFLOW`

---

### D. Idempotent retry

```
Submitting the same (clientId, clientSeq, commandId) a second time returns the cached
result verbatim. The command is NOT re-applied. Status will be DUPLICATE.
```

```java
// First submission: applied normally.
long seq1 = client.submit(CommandType.TRANSFER, 100L, 200L, 0L, 150L);
// ResultHandler: status=SUCCESS, resultBalance=350

// Network hiccup: retransmit the same logical command.
// AdbeClient does this automatically by reusing the same commandIdLo.
// If you drive the raw wire yourself:
//   send same (clientId=1, clientSeq=1, commandIdHi=1, commandIdLo=seq1)
// ResultHandler: status=DUPLICATE, resultBalance=350 (cached, not recomputed)
```

`AdbeClient` handles retransmission automatically on leader change and timeout, reusing the original
`commandIdLo`, which is the prerequisite for this guarantee. The application layer does not need to
track duplicates.

> **Idempotency survives node restart**: the dedup table is included in every snapshot. A node
> restarted from a snapshot returns `DUPLICATE` for any command applied before the snapshot was
> taken. See [OPERATIONS.md - Snapshot Management](OPERATIONS.md#4-snapshot-management).

---

## 8. Commands Reference

### Command envelope fields

Every command is wrapped in a `CommandEnvelope` (SBE-encoded) with the following fields:

| Field         | Type   | Assigned by | Purpose                                                       |
|---------------|--------|-------------|---------------------------------------------------------------|
| `clientId`    | `long` | Edge        | Session identity after authentication. Scopes the dedup ring. |
| `clientSeq`   | `long` | Edge        | Monotonic per-client counter. Drives the O(1) dedup slot.     |
| `commandIdHi` | `long` | Edge        | High word of 128-bit globally unique command id.              |
| `commandIdLo` | `long` | Edge        | Low word; returned by `submit()` for result correlation.       |
| `commandType` | enum   | Edge        | One of the seven command types below.                         |
| `accountA`    | `long` | caller      | Primary account operand (sender, owner, or delegate).         |
| `accountB`    | `long` | caller      | Secondary operand (recipient, delegate).                      |
| `accountC`    | `long` | caller      | Tertiary operand (recipient in DELEGATED_TRANSFER; else `0`). |
| `amount`      | `long` | caller      | Non-negative 64-bit fixed-scale value.                        |

`submit(type, accountA, accountB, accountC, amount)` - argument order matches this table.

---

### Command semantics table

| Command               | accountA     | accountB    | accountC    | Success result                         | Possible errors                                                   |
|-----------------------|--------------|-------------|-------------|----------------------------------------|-------------------------------------------------------------------|
| `CREDIT`              | recipient    | (unused)    | (unused)    | `resultBalance` = new balance          | `INVALID_AMOUNT`, `OVERFLOW`                                      |
| `DEBIT`               | account      | (unused)    | (unused)    | `resultBalance` = new balance          | `INVALID_AMOUNT`, `INVALID_ACCOUNT`, `INSUFFICIENT_BALANCE`      |
| `TRANSFER`            | sender       | recipient   | (unused)    | `resultBalance` = sender's new balance | `INVALID_AMOUNT`, `INVALID_ACCOUNT`, `INSUFFICIENT_BALANCE`, `OVERFLOW` |
| `APPROVE`             | owner        | delegate    | (unused)    | `resultAllowance` = set allowance      | `INVALID_AMOUNT`                                                  |
| `INCREASE_ALLOWANCE`  | owner        | delegate    | (unused)    | `resultAllowance` = new allowance      | `INVALID_AMOUNT`, `OVERFLOW`                                      |
| `DECREASE_ALLOWANCE`  | owner        | delegate    | (unused)    | `resultAllowance` = new allowance      | `INVALID_AMOUNT`, `INSUFFICIENT_ALLOWANCE`                        |
| `DELEGATED_TRANSFER`  | delegate     | owner       | recipient   | `resultBalance` + `resultAllowance`    | `INVALID_AMOUNT`, `INSUFFICIENT_ALLOWANCE`, `INVALID_ACCOUNT`, `INSUFFICIENT_BALANCE`, `OVERFLOW` |

---

### CREDIT

Creates the account if it does not exist. Adds `amount` to the current balance.

```
submit(CREDIT, recipient, 0, 0, amount)
```

- `amount` must be >= 0; negative values return `INVALID_AMOUNT`.
- There is no separate account-creation command; the first `CREDIT` is the registration.

---

### DEBIT

Subtracts `amount` from an existing account balance.

```
submit(DEBIT, account, 0, 0, amount)
```

- Account must exist; missing account returns `INVALID_ACCOUNT`.
- Balance after debit must be >= 0; shortfall returns `INSUFFICIENT_BALANCE`.

---

### TRANSFER

Moves `amount` from sender to recipient atomically. Total supply is conserved.

```
submit(TRANSFER, sender, recipient, 0, amount)
```

- Sender must exist; recipient is created with balance 0 if absent.
- Self-transfer (`sender == recipient`): no-op, returns `SUCCESS`, `resultBalance` = current balance.

---

### APPROVE

Sets an absolute allowance for `delegate` to spend from `owner`'s balance.

```
submit(APPROVE, owner, delegate, 0, limit)
```

- Overwrites any previous allowance. Use `INCREASE_ALLOWANCE` / `DECREASE_ALLOWANCE` for deltas.
- Setting limit to `0` revokes the allowance.

---

### INCREASE_ALLOWANCE

Adds `delta` to the current allowance for `(owner, delegate)`.

```
submit(INCREASE_ALLOWANCE, owner, delegate, 0, delta)
```

- If no prior allowance exists, it defaults to `0` before adding.

---

### DECREASE_ALLOWANCE

Subtracts `delta` from the current allowance for `(owner, delegate)`.

```
submit(DECREASE_ALLOWANCE, owner, delegate, 0, delta)
```

- Returns `INSUFFICIENT_ALLOWANCE` if current allowance < delta.

---

### DELEGATED_TRANSFER

Delegate transfers `amount` from owner's balance to recipient, consuming from the allowance.

```
submit(DELEGATED_TRANSFER, delegate, owner, recipient, amount)
```

- Validation order (stops at first failure): `INVALID_AMOUNT` -> `INSUFFICIENT_ALLOWANCE` ->
  `INVALID_ACCOUNT` (owner must exist) -> `INSUFFICIENT_BALANCE` -> `OVERFLOW`.
- `owner == recipient`: allowance is consumed, owner's balance is unchanged.
- Both `resultBalance` (owner's new balance) and `resultAllowance` (remaining allowance) are
  present on `SUCCESS`.

---

## 9. Status Codes

| Code                    | Value | Meaning                                                             | When to expect                                                  |
|-------------------------|------:|---------------------------------------------------------------------|-----------------------------------------------------------------|
| `SUCCESS`               | 0     | Command applied; result fields are valid.                           | Normal case.                                                    |
| `INSUFFICIENT_BALANCE`  | 1     | Account balance is less than the requested amount.                  | DEBIT, TRANSFER sender, DELEGATED_TRANSFER owner.               |
| `INSUFFICIENT_ALLOWANCE`| 2     | Allowance is less than the requested spend.                         | DELEGATED_TRANSFER; DECREASE_ALLOWANCE (current < delta).       |
| `INVALID_ACCOUNT`       | 3     | Account does not exist.                                             | DEBIT (account missing), TRANSFER (sender missing), DELEGATED_TRANSFER (owner missing). |
| `DUPLICATE`             | 4     | Cached result returned; command was not re-applied.                 | Retransmit of same `(clientId, clientSeq, commandId)`.          |
| `OVERFLOW`              | 5     | Operation would push balance or allowance above `Long.MAX_VALUE`.   | CREDIT large amount, TRANSFER recipient, INCREASE_ALLOWANCE, DELEGATED_TRANSFER recipient. |
| `INVALID_AMOUNT`        | 6     | `amount` is negative.                                               | Any command submitted with `amount < 0`.                        |
| `NULL_VAL`              | -1    | Uninitialized sentinel; never returned by the engine.               | Check `hasBalance`/`hasAllowance` rather than comparing to this value. |

On any non-`SUCCESS` status (including `DUPLICATE`), `hasBalance` and `hasAllowance` are both
`false`. The cached result on `DUPLICATE` carries the original `resultBalance`/`resultAllowance`
from when the command first succeeded.

---

## 10. Observability

### AdbeClient metrics

All counters are read from the poll thread. There is no synchronization cost.

| Method                    | Type        | Description                                                         |
|---------------------------|-------------|---------------------------------------------------------------------|
| `submitted()`             | `long`      | Total commands handed to the network layer (including retransmits). |
| `completed()`             | `long`      | Commands for which a result has been received and correlated.       |
| `pendingCount()`          | `int`       | Commands currently in-flight (submitted but not yet acknowledged).  |
| `backpressureEvents()`    | `long`      | Number of times `BackpressureException` was thrown or offer failed. |
| `leaderChanges()`         | `int`       | Number of leader elections observed since construction.             |
| `leaderMemberId()`        | `int`       | Current cluster leader member id (-1 if unknown).                   |
| `latencyHistogram()`      | `Histogram` | HdrHistogram of end-to-end submit-to-result latency in nanoseconds. |

### HdrHistogram - percentile reporting

```java
Histogram h = client.latencyHistogram();
System.out.printf("p50=%dus p99=%dus p99.9=%dus max=%dus%n",
        TimeUnit.NANOSECONDS.toMicros(h.getValueAtPercentile(50.0)),
        TimeUnit.NANOSECONDS.toMicros(h.getValueAtPercentile(99.0)),
        TimeUnit.NANOSECONDS.toMicros(h.getValueAtPercentile(99.9)),
        TimeUnit.NANOSECONDS.toMicros(h.getMaxValue()));
```

The histogram covers a 1-hour range at 3 significant figures. Reset with `h.reset()` between
measurement windows. Mean latency is not a contract metric; use p99.9 as the tail budget.

### Core metrics HTTP endpoint

The launcher can expose off-heap counters over HTTP for Prometheus scraping:

```bash
./gradlew :adbe-launcher:run -Dadbe.metricsPort=9100
curl http://localhost:9100/metrics   # adbe_commands_processed, adbe_duplicates_detected, ...
curl http://localhost:9100/healthz   # ok
```

---

## 11. Direct Engine Usage (Headless)

For unit tests, property tests, and deterministic replay, drive `BalanceEngine` directly without a
cluster or network stack. The SBE flyweight encodes into a reusable `UnsafeBuffer`; no heap
allocation occurs in steady state.

```java
import com.adbe.config.CoreConfig;
import com.adbe.core.BalanceEngine;
import com.adbe.core.CommandOutcome;
import com.adbe.protocol.*;
import com.adbe.telemetry.CoreMetrics;
import org.agrona.concurrent.UnsafeBuffer;

// Build engine with preallocated power-of-two capacities.
BalanceEngine engine = new BalanceEngine(CoreConfig.defaults(), new CoreMetrics());
CommandOutcome outcome = new CommandOutcome();

// Reusable encode/decode flyweights - one set per thread.
UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
CommandEnvelopeEncoder encoder = new CommandEnvelopeEncoder();
CommandEnvelopeDecoder decoder = new CommandEnvelopeDecoder();

// Encode a CREDIT.
encoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
        .clientId(1L).clientSeq(0L)
        .commandIdHi(0L).commandIdLo(42L)
        .commandType(CommandType.CREDIT)
        .accountA(100L).accountB(0L)
        .amount(500L)
        .correlationId(CommandEnvelopeEncoder.correlationIdNullValue())
        .accountC(0L);

// Decode and process.
headerDecoder.wrap(buffer, 0);
decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH,
        headerDecoder.blockLength(), headerDecoder.version());

boolean duplicate = engine.process(decoder, outcome);
// outcome.status()        -> SUCCESS
// outcome.resultBalance() -> 500
// duplicate               -> false

// Replay idempotency: same (clientId, clientSeq) returns cached result.
boolean dup2 = engine.process(decoder, outcome); // dup2 = true
```

In tests, use `CommandFixtures` from `adbe-tests` (`testFixtures` source set):

```java
CommandFixtures fixtures = new CommandFixtures();
CommandEnvelopeDecoder cmd = fixtures.encode(
        /*clientId*/ 1L, /*clientSeq*/ 0L,
        /*cmdIdHi*/ 0L,  /*cmdIdLo*/ 1L,
        CommandType.TRANSFER,
        /*accountA*/ 1L, /*accountB*/ 2L, /*accountC*/ 0L, /*amount*/ 40L);

engine.process(cmd, outcome);
```

`CommandFixtures` is a test-only helper; it is not part of the shipped `adbe-client` SDK.

---

## 12. Numeric Conventions

| Convention              | Rule                                                                                  |
|-------------------------|---------------------------------------------------------------------------------------|
| Balance / allowance type| `long` (64-bit signed integer). Scale is fixed and defined per deployment.            |
| Amount constraints      | `amount >= 0`; negative values always return `INVALID_AMOUNT`.                        |
| Max value               | `Long.MAX_VALUE` = `9_223_372_036_854_775_807`. Operations that would exceed this return `OVERFLOW`. |
| Prohibited types        | Never use `double`, `float`, or `java.math.BigDecimal` for monetary or fixed-scale values. |
| Null sentinel           | Result fields use a wire-level `NULL_VAL` (-1 for status, specific sentinel per field). Always check `hasBalance` / `hasAllowance` rather than comparing raw values to null sentinels. |
| Account IDs             | Arbitrary `long`; zero is valid but conventionally unused.                            |
| Client / command IDs    | 128-bit composite (`commandIdHi` + `commandIdLo`). The low word is returned by `submit()` and is sufficient for single-client correlation. |

---

## 13. Read API (HTTP)

The command path (sections 1 - 12) is write-only: every result is a deterministic `CommandResult`
committed through Raft. Reads are served separately by the `adbe-read` module, a CQRS read side.
Reads are served by a standby read node:

- **Standby mode** (the only mode): `StandbyReadNode` runs as a standalone
  process with its own embedded Media Driver. It connects to a cluster member's
  Aeron Archive and follows the consensus log recording from the last loaded
  snapshot position - or from position 0 when no snapshot has loaded yet, so it
  builds state immediately on a fresh cluster - and loads service snapshots as
  they appear. It applies the committed log through the same `BalanceEngine`, so
  it holds a complete copy of engine state, including both sides of every
  `TRANSFER`, which the egress stream never carries. Standby nodes are NOT
  cluster members: they do not vote, do not affect quorum, and can be added,
  removed, or restarted independently. See
  [ADR 0006](decisions/0006-standby-snapshot-read-nodes.md) and
  [ADR 0007](decisions/0007-standby-only-read-side.md).

**Consistency**: reads are eventually consistent with bounded staleness. With
live log following (the default), staleness is the live log replay delay
(milliseconds); a snapshot, when present, bounds the replay. Do not use these
endpoints where linearizable reads are required.

**Threading**: queries are answered on the single agent thread (an Agrona
`Agent` driven by an `AgentRunner`), reached over a lock-free ring buffer from
the Netty HTTP boundary. Reads never touch the stores concurrently, so the
single-writer discipline is preserved.

### Running a read node

```bash
# Gradle (development): standalone standby, connects to localhost:20104 archive.
ADBE_ARCHIVE_CHANNEL=aeron:udp?endpoint=localhost:20104 ./gradlew :adbe-read:run
```

```java
import com.adbe.config.CoreConfig;
import com.adbe.read.StandbyReadNode;
import com.adbe.read.config.ReadServiceConfig;
import com.adbe.read.config.StandbyConfig;

StandbyConfig standbyConfig = StandbyConfig.builder()
        .archiveControlChannel("aeron:udp?endpoint=localhost:20104")
        .build();
ReadServiceConfig readConfig = ReadServiceConfig.builder().httpPort(8080).build();

try (StandbyReadNode node = new StandbyReadNode(standbyConfig, CoreConfig.defaults(), readConfig)) {
    // Serves reads on http://localhost:8080 while following the cluster log.
}
```

Configured by environment variables via `com.adbe.read.ReadServiceLauncher`:
`ADBE_ARCHIVE_CHANNEL`, `ADBE_LOCAL_HOST` (routable host for Archive call-backs,
default `localhost`; set to the container address in Docker), `ADBE_HTTP_PORT`
(default `8080`), `ADBE_SNAPSHOT_POLL_MS` (default `5000`), `ADBE_LIVE_LOG`
(default `true`). See
[OPERATIONS.md - Read Service](OPERATIONS.md#8-read-service-http-query-api)
for the full reference.

### Endpoints

| Method | Path                          | Description                          | Success body                                                        |
|--------|-------------------------------|--------------------------------------|---------------------------------------------------------------------|
| GET    | `/balance/{id}`               | One account balance                  | `{"account":100,"exists":true,"balance":350}`                       |
| POST   | `/balances`                   | Batch balances (ids in request body) | `{"balances":[{"account":100,"exists":true,"balance":350}, ...]}`   |
| GET    | `/allowance/{owner}/{delegate}` | Allowance for a pair               | `{"owner":1,"delegate":9,"allowance":200}`                          |
| GET    | `/supply`                     | Engine-wide total supply             | `{"totalSupply":500}`                                               |
| GET    | `/healthz`                    | Liveness probe                       | `{"status":"ok"}`                                                   |
| GET    | `/metrics`                    | Gateway counters                     | `{"submitted":...,"completed":...,"pending":...}`                   |

- A missing account returns HTTP 200 with `{"exists":false}` (not 404), so batch responses stay
  uniform. The `balance` field is omitted when `exists` is false.
- `POST /balances` accepts any JSON or text body containing the account ids; every signed decimal
  integer in the body is treated as an id, up to `maxBatchSize` (default 512). Example body:
  `{"ids":[100,200,999]}`.
- An overloaded request ring returns HTTP 503 `{"error":"read service overloaded"}`; a read that is
  not answered within `requestTimeoutMs` (default 5000) returns HTTP 504.

### Examples

```bash
curl http://localhost:8080/balance/100
# {"account":100,"exists":true,"balance":350}

curl -X POST http://localhost:8080/balances -d '{"ids":[100,200,999]}'
# {"balances":[{"account":100,"exists":true,"balance":350},
#              {"account":200,"exists":true,"balance":150},
#              {"account":999,"exists":false}]}

curl http://localhost:8080/allowance/1/9
# {"owner":1,"delegate":9,"allowance":200}

curl http://localhost:8080/supply
# {"totalSupply":500}
```

> **Authentication and rate limiting are out of scope** for the read module and must be added at the
> Edge before any production exposure (see ADR 0005).
