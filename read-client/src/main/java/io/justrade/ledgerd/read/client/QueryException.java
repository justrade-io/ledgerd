package io.justrade.ledgerd.read.client;

import io.justrade.ledgerd.protocol.QueryStatusCode;

/** Thrown when the read service rejected a query (e.g. an unsupported query type). */
public final class QueryException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final QueryStatusCode status;

    public QueryException(final QueryStatusCode status, final String message) {
        super(message);
        this.status = status;
    }

    public QueryStatusCode status() {
        return status;
    }
}
