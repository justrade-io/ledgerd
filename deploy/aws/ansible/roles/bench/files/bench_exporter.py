#!/usr/bin/env python3
"""Expose LEDGERD bench results as Prometheus gauge metrics.

Reads both result files produced by the bench harnesses:

- results.csv        (single-command in-flight sweep)  -> ledgerd_bench_*
- batch-results.csv  (batch legs-per-batch sweep)      -> ledgerd_batch_*

Each file is grouped by its sweep dimension (max_in_flight or legs_per_batch),
keeping only the latest timestamp group; latency percentiles are aggregated as
max across the concurrent clients and counters as sum.

Scraped by Prometheus on :9200.
"""
import csv
from http.server import BaseHTTPRequestHandler, HTTPServer

RESULTS_CSV = "/var/lib/ledgerd/bench/results.csv"
BATCH_RESULTS_CSV = "/var/lib/ledgerd/bench/batch-results.csv"
PORT = 9200


def read_aggregates(path, group_key, max_fields, sum_fields):
    """Aggregate CSV rows grouped by group_key, keeping the latest timestamp group."""
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


def single_command_metrics():
    return read_aggregates(
        RESULTS_CSV,
        "max_in_flight",
        ["p50_us", "p99_us", "p999_us", "max_us"],
        ["submitted", "completed", "backpressure", "leader_changes"],
    )


def batch_metrics():
    return read_aggregates(
        BATCH_RESULTS_CSV,
        "legs_per_batch",
        ["p50_us", "p99_us", "p999_us", "max_us"],
        ["batches", "legs", "legs_per_sec", "batches_per_sec", "backpressure", "leader_changes"],
    )


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/metrics":
            self.send_error(404)
            return

        lines = []

        for mif, e in single_command_metrics().items():
            labels = 'max_in_flight="%s"' % mif
            lines.append("ledgerd_bench_p50_us{%s} %d" % (labels, e["p50_us"]))
            lines.append("ledgerd_bench_p99_us{%s} %d" % (labels, e["p99_us"]))
            lines.append("ledgerd_bench_p999_us{%s} %d" % (labels, e["p999_us"]))
            lines.append("ledgerd_bench_max_us{%s} %d" % (labels, e["max_us"]))
            lines.append("ledgerd_bench_backpressure{%s} %d" % (labels, e["backpressure"]))
            lines.append("ledgerd_bench_submitted{%s} %d" % (labels, e["submitted"]))
            lines.append("ledgerd_bench_completed{%s} %d" % (labels, e["completed"]))

        for lpb, e in batch_metrics().items():
            labels = 'legs_per_batch="%s"' % lpb
            lines.append("ledgerd_batch_legs_per_sec{%s} %d" % (labels, e["legs_per_sec"]))
            lines.append("ledgerd_batch_batches_per_sec{%s} %d" % (labels, e["batches_per_sec"]))
            lines.append("ledgerd_batch_p50_us{%s} %d" % (labels, e["p50_us"]))
            lines.append("ledgerd_batch_p99_us{%s} %d" % (labels, e["p99_us"]))
            lines.append("ledgerd_batch_p999_us{%s} %d" % (labels, e["p999_us"]))
            lines.append("ledgerd_batch_max_us{%s} %d" % (labels, e["max_us"]))
            lines.append("ledgerd_batch_backpressure{%s} %d" % (labels, e["backpressure"]))

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
