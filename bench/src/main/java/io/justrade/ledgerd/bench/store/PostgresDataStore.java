package io.justrade.ledgerd.bench.store;

import io.justrade.ledgerd.bench.Op;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * PostgreSQL backend: one row per account, mutated with synchronous JDBC. Credit
 * and debit are single {@code UPDATE}s under autocommit; a transfer runs a
 * transaction that locks both rows with {@code SELECT ... FOR UPDATE} in ascending
 * id order (so concurrent transfers cannot deadlock), applies the two updates, and
 * commits. Each op therefore pays a WAL fsync, which is the point of comparison.
 *
 * <p>The container is provisioned on demand via Testcontainers; a run requires a
 * reachable Docker daemon.
 */
public final class PostgresDataStore extends ThreadedDataStore {

    private static final String IMAGE = "postgres:16-alpine";
    private static final int INSERT_BATCH = 1000;

    private final PostgreSQLContainer<?> container;

    public PostgresDataStore(final int workers) {
        super("postgres", workers);
        this.container = new PostgreSQLContainer<>(IMAGE);
        container.start();
    }

    @Override
    public void setup(final int accounts, final long initialBalance) throws SQLException {
        try (Connection conn = connect()) {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS accounts");
                st.execute("CREATE TABLE accounts (id BIGINT PRIMARY KEY, balance BIGINT NOT NULL)");
            }
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO accounts (id, balance) VALUES (?, ?)")) {
                for (int i = 1; i <= accounts; i++) {
                    ps.setLong(1, i);
                    ps.setLong(2, initialBalance);
                    ps.addBatch();
                    if (i % INSERT_BATCH == 0) {
                        ps.executeBatch();
                    }
                }
                ps.executeBatch();
            }
            conn.commit();
        }
    }

    @Override
    protected Worker createWorker() throws SQLException {
        return new PostgresWorker(connect());
    }

    @Override
    public void verify(final long expectedSupply) throws SQLException {
        try (Connection conn = connect();
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT COALESCE(SUM(balance), 0) FROM accounts")) {
            rs.next();
            final long actual = rs.getLong(1);
            if (actual != expectedSupply) {
                throw new IllegalStateException("postgres supply " + actual + " != expected " + expectedSupply);
            }
        }
    }

    @Override
    public void close() {
        container.stop();
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(container.getJdbcUrl(), container.getUsername(), container.getPassword());
    }

    /** Per-thread JDBC connection with prepared statements for the three ops. */
    private static final class PostgresWorker implements Worker {
        private final Connection conn;
        private final PreparedStatement credit;
        private final PreparedStatement debit;
        private final PreparedStatement lock;

        PostgresWorker(final Connection conn) throws SQLException {
            this.conn = conn;
            this.credit = conn.prepareStatement("UPDATE accounts SET balance = balance + ? WHERE id = ?");
            this.debit = conn.prepareStatement("UPDATE accounts SET balance = balance - ? WHERE id = ?");
            this.lock = conn.prepareStatement("SELECT id FROM accounts WHERE id IN (?, ?) ORDER BY id FOR UPDATE");
        }

        @Override
        public void execute(final Op op) throws SQLException {
            switch (op.type()) {
                case CREDIT -> update(credit, op.amount(), op.accountA());
                case DEBIT -> update(debit, op.amount(), op.accountA());
                case TRANSFER -> transfer(op.accountA(), op.accountB(), op.amount());
                default -> throw new IllegalArgumentException("unknown op type: " + op.type());
            }
        }

        private static void update(final PreparedStatement ps, final long amount, final long id) throws SQLException {
            ps.setLong(1, amount);
            ps.setLong(2, id);
            ps.executeUpdate();
        }

        private void transfer(final long from, final long to, final long amount) throws SQLException {
            conn.setAutoCommit(false);
            try {
                lock.setLong(1, Math.min(from, to));
                lock.setLong(2, Math.max(from, to));
                try (ResultSet rs = lock.executeQuery()) {
                    while (rs.next()) {
                        // Drain the locked rows; the FOR UPDATE lock is the point.
                    }
                }
                update(debit, amount, from);
                update(credit, amount, to);
                conn.commit();
            } catch (final SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        }

        @Override
        public void close() throws SQLException {
            conn.close();
        }
    }
}
