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
- `BalanceResult` - a decoded single balance response.
- `AllowanceResult` - a decoded allowance response.
- `TotalSupplyResult` - a decoded total-supply response.

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
}
```

## Related

- The read replica: [../read](../read).
