#!/usr/bin/env python3
"""Expose LEDGERD bench results as Prometheus gauge metrics.

Reads the result files produced by the bench harnesses:

- results.csv              (single-command client rows)           -> ledgerd_bench_*_max_us + counters
- merged-results.csv       (single-command merged percentiles)    -> ledgerd_bench_*_us
- batch-results.csv        (batch client rows)                    -> ledgerd_batch_*_max_us + throughput
- batch-merged-results.csv (batch merged percentiles)             -> ledgerd_batch_*_us
- reconcile.csv            (single-command server reconciliation) -> ledgerd_reconcile_*
- batch-reconcile.csv      (batch server reconciliation)          -> ledgerd_batch_reconcile_*

Each file is grouped by its sweep dimension (max_in_flight or legs_per_batch),
keeping only the latest timestamp group. Per-client percentile fields are
aggregated as max across concurrent clients; merged files carry the true
combined percentile produced by BenchAggregator.

Scraped by Prometheus on :9200.
"""
import csv
from http.server import BaseHTTPRequestHandler, HTTPServer

RESULTS_CSV = "/var/lib/ledgerd/bench/results.csv"
MERGED_RESULTS_CSV = "/var/lib/ledgerd/bench/merged-results.csv"
RECONCILE_CSV = "/var/lib/ledgerd/bench/reconcile.csv"
BATCH_RESULTS_CSV = "/var/lib/ledgerd/bench/batch-results.csv"
BATCH_MERGED_RESULTS_CSV = "/var/lib/ledgerd/bench/batch-merged-results.csv"
BATCH_RECONCILE_CSV = "/var/lib/ledgerd/bench/batch-reconcile.csv"
PORT = 9200

PERCENTILES = ["p50_us", "p99_us", "p999_us", "max_us"]


def read_aggregates(path, group_key, max_fields, sum_fields):
    """Aggregate CSV rows grouped by group_key, keeping the latest timestamp group.

    max_fields are aggregated as max across the group rows, sum_fields as sum.
    """
    rows = []
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                rows.append(row)
    except (FileNotFoundError, KeyError, ValueError):
        return {}

    by_key = {}
    for row in rows:
        key = row[group_key]
        ts = row["timestamp"]
        if key not in by_key or ts > by_key[key]["ts"]:
            by_key[key] = {"ts": ts, "rows": [row]}
        elif ts == by_key[key]["ts"]:
            by_key[key]["rows"].append(row)

    agg = {}
    for key, group in by_key.items():
        group_rows = group["rows"]
        entry = {}
        for field in max_fields:
            entry[field] = max(int(r[field]) for r in group_rows)
        for field in sum_fields:
            entry[field] = sum(int(r[field]) for r in group_rows)
        agg[key] = entry
    return agg


def read_latest(path, group_key, fields):
    """Read the latest row per group_key, returning only the requested fields."""
    rows = []
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                rows.append(row)
    except (FileNotFoundError, KeyError, ValueError):
        return {}

    by_key = {}
    for row in rows:
        key = row[group_key]
        ts = row["timestamp"]
        if key not in by_key or ts >= by_key[key]["timestamp"]:
            by_key[key] = row

    agg = {}
    for key, row in by_key.items():
        agg[key] = {field: int(row[field]) for field in fields}
    return agg


def single_client_metrics():
    return read_aggregates(
        RESULTS_CSV,
        "max_in_flight",
        PERCENTILES,
        ["submitted", "completed", "backpressure", "leader_changes"],
    )


def single_merged_metrics():
    return read_latest(MERGED_RESULTS_CSV, "max_in_flight", ["samples"] + PERCENTILES)


def single_reconcile_metrics():
    return read_latest(
        RECONCILE_CSV,
        "max_in_flight",
        [
            "expected_commands",
            "server_commands_delta",
            "server_duplicates",
            "server_leader_elections",
            "server_backpressure",
            "read_applied_position",
            "read_integrity_failures",
        ],
    )


def batch_client_metrics():
    return read_aggregates(
        BATCH_RESULTS_CSV,
        "legs_per_batch",
        PERCENTILES,
        ["batches", "legs", "legs_per_sec", "batches_per_sec", "backpressure", "leader_changes"],
    )


def batch_merged_metrics():
    return read_latest(BATCH_MERGED_RESULTS_CSV, "legs_per_batch", ["samples"] + PERCENTILES)


def batch_reconcile_metrics():
    return read_latest(
        BATCH_RECONCILE_CSV,
        "legs_per_batch",
        [
            "expected_commands",
            "server_commands_delta",
            "server_duplicates",
            "server_leader_elections",
            "server_backpressure",
            "read_applied_position",
            "read_integrity_failures",
        ],
    )


def emit(lines, name, label_key, label_value, value):
    lines.append('%s{%s="%s"} %d' % (name, label_key, label_value, value))


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/metrics":
            self.send_error(404)
            return

        lines = []

        for mif, e in single_client_metrics().items():
            emit(lines, "ledgerd_bench_submitted", "max_in_flight", mif, e["submitted"])
            emit(lines, "ledgerd_bench_completed", "max_in_flight", mif, e["completed"])
            emit(lines, "ledgerd_bench_backpressure", "max_in_flight", mif, e["backpressure"])
            emit(lines, "ledgerd_bench_leader_changes", "max_in_flight", mif, e["leader_changes"])
            emit(lines, "ledgerd_bench_p50_max_us", "max_in_flight", mif, e["p50_us"])
            emit(lines, "ledgerd_bench_p99_max_us", "max_in_flight", mif, e["p99_us"])
            emit(lines, "ledgerd_bench_p999_max_us", "max_in_flight", mif, e["p999_us"])
            emit(lines, "ledgerd_bench_max_max_us", "max_in_flight", mif, e["max_us"])

        for mif, e in single_merged_metrics().items():
            emit(lines, "ledgerd_bench_samples", "max_in_flight", mif, e["samples"])
            emit(lines, "ledgerd_bench_p50_us", "max_in_flight", mif, e["p50_us"])
            emit(lines, "ledgerd_bench_p99_us", "max_in_flight", mif, e["p99_us"])
            emit(lines, "ledgerd_bench_p999_us", "max_in_flight", mif, e["p999_us"])
            emit(lines, "ledgerd_bench_max_us", "max_in_flight", mif, e["max_us"])

        for mif, e in single_reconcile_metrics().items():
            emit(lines, "ledgerd_reconcile_expected_commands", "max_in_flight", mif, e["expected_commands"])
            emit(lines, "ledgerd_reconcile_server_commands_delta", "max_in_flight", mif, e["server_commands_delta"])
            emit(lines, "ledgerd_reconcile_server_duplicates", "max_in_flight", mif, e["server_duplicates"])
            emit(lines, "ledgerd_reconcile_server_leader_elections", "max_in_flight", mif, e["server_leader_elections"])
            emit(lines, "ledgerd_reconcile_server_backpressure", "max_in_flight", mif, e["server_backpressure"])
            emit(lines, "ledgerd_reconcile_read_applied_position", "max_in_flight", mif, e["read_applied_position"])
            emit(lines, "ledgerd_reconcile_read_integrity_failures", "max_in_flight", mif, e["read_integrity_failures"])

        for lpb, e in batch_client_metrics().items():
            emit(lines, "ledgerd_batch_legs_per_sec", "legs_per_batch", lpb, e["legs_per_sec"])
            emit(lines, "ledgerd_batch_batches_per_sec", "legs_per_batch", lpb, e["batches_per_sec"])
            emit(lines, "ledgerd_batch_batches", "legs_per_batch", lpb, e["batches"])
            emit(lines, "ledgerd_batch_legs", "legs_per_batch", lpb, e["legs"])
            emit(lines, "ledgerd_batch_backpressure", "legs_per_batch", lpb, e["backpressure"])
            emit(lines, "ledgerd_batch_leader_changes", "legs_per_batch", lpb, e["leader_changes"])
            emit(lines, "ledgerd_batch_p50_max_us", "legs_per_batch", lpb, e["p50_us"])
            emit(lines, "ledgerd_batch_p99_max_us", "legs_per_batch", lpb, e["p99_us"])
            emit(lines, "ledgerd_batch_p999_max_us", "legs_per_batch", lpb, e["p999_us"])
            emit(lines, "ledgerd_batch_max_max_us", "legs_per_batch", lpb, e["max_us"])

        for lpb, e in batch_merged_metrics().items():
            emit(lines, "ledgerd_batch_samples", "legs_per_batch", lpb, e["samples"])
            emit(lines, "ledgerd_batch_p50_us", "legs_per_batch", lpb, e["p50_us"])
            emit(lines, "ledgerd_batch_p99_us", "legs_per_batch", lpb, e["p99_us"])
            emit(lines, "ledgerd_batch_p999_us", "legs_per_batch", lpb, e["p999_us"])
            emit(lines, "ledgerd_batch_max_us", "legs_per_batch", lpb, e["max_us"])

        for lpb, e in batch_reconcile_metrics().items():
            emit(lines, "ledgerd_batch_reconcile_expected_commands", "legs_per_batch", lpb, e["expected_commands"])
            emit(lines, "ledgerd_batch_reconcile_server_commands_delta", "legs_per_batch", lpb, e["server_commands_delta"])
            emit(lines, "ledgerd_batch_reconcile_server_duplicates", "legs_per_batch", lpb, e["server_duplicates"])
            emit(lines, "ledgerd_batch_reconcile_server_leader_elections", "legs_per_batch", lpb, e["server_leader_elections"])
            emit(lines, "ledgerd_batch_reconcile_server_backpressure", "legs_per_batch", lpb, e["server_backpressure"])
            emit(lines, "ledgerd_batch_reconcile_read_applied_position", "legs_per_batch", lpb, e["read_applied_position"])
            emit(lines, "ledgerd_batch_reconcile_read_integrity_failures", "legs_per_batch", lpb, e["read_integrity_failures"])

        body = "\n".join(lines) + "\n"
        payload = body.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
