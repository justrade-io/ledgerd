# read-client

The read-side SDK: query a read replica over plain Aeron request/response
streams. Like `write-client`, it depends only on `protocol`.

## Responsibility

- Send `QueryRequest` messages and correlate `QueryResponse` messages by
  request id.
- Provide idempotent retry with a bounded in-flight window.
- Expose typed results for balances, batch balances, allowances, and total
  supply.

## Key classes

- [ReadClient.java](src/main/java/io/justrade/ledgerd/read/client/ReadClient.java) -
  the client: connect, query, poll.
- [QueryListener.java](src/main/java/io/justrade/ledgerd/read/client/QueryListener.java) -
  callback sink for asynchronous query delivery.
- [config/ReadClientConfig.java](src/main/java/io/justrade/ledgerd/read/client/config/ReadClientConfig.java) -
  immutable client configuration.
- `BalanceResult`, `AllowanceResult`, `TotalSupplyResult` - typed query results.

## Usage shape

```java
try (ReadClient client = new ReadClient(ReadClientConfig.builder().build())) {
    long requestId = client.submitBalance(0L, 42L);
    while (running) {
        client.poll();   // drives response callbacks, correlated by requestId
    }
}

// Or synchronous:
try (ReadClient client = new ReadClient(ReadClientConfig.builder().build())) {
    BalanceResult result = client.balance(0L, 42L);
    TotalSupplyResult supply = client.totalSupply(0L);
}
```

## Related

- The read replica: [../read/README.md](../read/README.md).
- Read API reference: [../docs/API-REFERENCE.md](../docs/API-REFERENCE.md).
- Wire format: [../protocol/README.md](../protocol/README.md).
