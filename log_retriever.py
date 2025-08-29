#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Concurrent OCI Logging retriever:
- Resolves compartment/log group/log names -> OCIDs
- Splits timeframe into slices and processes them in parallel (workers)
- Robust retry/backoff with jitter and Retry-After handling
- Writes NDJSON (one event per line) via a single writer thread
- Shows a lightweight progress bar when --debug is not used
"""

import argparse
import json
import os
import queue
import random
import re
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import oci
from oci.identity import IdentityClient
from oci.logging import LoggingManagementClient
from oci.loggingsearch import LogSearchClient
from oci.loggingsearch.models import SearchLogsDetails
from payroll_metrics import parse_timeframe

# ───────────────────────── Tiempo ─────────────────────────


def clamp_14_days(start_dt: datetime, end_dt: datetime, debug=False):
    max_span = timedelta(days=14)
    span = end_dt - start_dt
    if span > max_span:
        new_start = end_dt - max_span
        if debug:
            print(f"[WARN] Window {span} > 14 days; clamped to {new_start} .. {end_dt}", file=sys.stderr)
        return new_start, end_dt, True
    return start_dt, end_dt, False

def parse_duration(dur: str) -> timedelta:
    m = re.match(r'^(\d+)\s*(s|m|h|d|w)$', dur.strip(), re.IGNORECASE)
    if not m:
        raise ValueError("Invalid --slice. Use Ns/Nm/Nh/Nd/Nw (e.g., 30m, 1h).")
    qty = int(m.group(1)); unit = m.group(2).lower()
    return {
        "s": timedelta(seconds=qty),
        "m": timedelta(minutes=qty),
        "h": timedelta(hours=qty),
        "d": timedelta(days=qty),
        "w": timedelta(weeks=qty),
    }[unit]

# ────────────────────── Resolvers (translator) ──────────────────────

OCID_COMP_RX = re.compile(r"^ocid1\.compartment\..+$")
OCID_LG_RX   = re.compile(r"^ocid1\.loggroup\..+$")
OCID_LOG_RX  = re.compile(r"^ocid1\.log\..+$")

def resolve_compartment_id(identity: IdentityClient, tenancy_id: str, arg: str, debug=False) -> str:
    if OCID_COMP_RX.match(arg):
        return arg
    ten = identity.get_tenancy(tenancy_id).data
    if ten.name == arg:
        if debug: print(f"[DEBUG] Compartment '{arg}' matched tenancy name → root OCID", file=sys.stderr)
        return tenancy_id
    comps = oci.pagination.list_call_get_all_results(
        identity.list_compartments, tenancy_id,
        access_level="ACCESSIBLE", compartment_id_in_subtree=True).data
    exact = [c for c in comps if c.name == arg]
    if len(exact) == 1:
        if debug: print(f"[DEBUG] Compartment '{arg}' → {exact[0].id}", file=sys.stderr)
        return exact[0].id
    if len(exact) > 1:
        ids = ", ".join(c.id for c in exact)
        raise ValueError(f"Compartment name '{arg}' is not unique. Matches: {ids}")
    ci = [c for c in comps if c.name.lower() == arg.lower()]
    if len(ci) == 1:
        if debug: print(f"[DEBUG] Compartment (ci) '{arg}' → {ci[0].id}", file=sys.stderr)
        return ci[0].id
    raise ValueError(f"Compartment '{arg}' not found.")

def resolve_log_group_id(lm: LoggingManagementClient, compartment_id: str, arg: str, debug=False) -> str:
    if OCID_LG_RX.match(arg):
        return arg
    groups = oci.pagination.list_call_get_all_results(lm.list_log_groups, compartment_id).data
    exact = [g for g in groups if g.display_name == arg and getattr(g, "lifecycle_state", "ACTIVE") == "ACTIVE"]
    if len(exact) == 1:
        if debug: print(f"[DEBUG] Log Group '{arg}' → {exact[0].id}", file=sys.stderr)
        return exact[0].id
    if len(exact) > 1:
        ids = ", ".join(g.id for g in exact)
        raise ValueError(f"Log Group name '{arg}' is not unique (ACTIVE). Matches: {ids}")
    exact_any = [g for g in groups if g.display_name == arg]
    if len(exact_any) == 1:
        if debug: print(f"[DEBUG] Log Group (any) '{arg}' → {exact_any[0].id}", file=sys.stderr)
        return exact_any[0].id
    ci = [g for g in groups if g.display_name.lower() == arg.lower() and getattr(g, "lifecycle_state", "ACTIVE") == "ACTIVE"]
    if len(ci) == 1:
        if debug: print(f"[DEBUG] Log Group (ci) '{arg}' → {ci[0].id}", file=sys.stderr)
        return ci[0].id
    names = sorted(set(getattr(g, "display_name", "") for g in groups))
    raise ValueError(f"Log Group '{arg}' not found/unique in {compartment_id}. "
                     f"Available: {', '.join(names[:20])}{'...' if len(names)>20 else ''}")

def resolve_log_id(lm: LoggingManagementClient, log_group_id: str, arg: str, debug=False) -> str:
    if OCID_LOG_RX.match(arg):
        return arg
    logs = oci.pagination.list_call_get_all_results(lm.list_logs, log_group_id).data
    exact = [l for l in logs if l.display_name == arg and getattr(l, "lifecycle_state", "ACTIVE") == "ACTIVE"]
    if len(exact) == 1:
        if debug: print(f"[DEBUG] Log '{arg}' → {exact[0].id}", file=sys.stderr)
        return exact[0].id
    if len(exact) > 1:
        ids = ", ".join(l.id for l in exact)
        raise ValueError(f"Log name '{arg}' is not unique (ACTIVE). Matches: {ids}")
    exact_any = [l for l in logs if l.display_name == arg]
    if len(exact_any) == 1:
        if debug: print(f"[DEBUG] Log (any) '{arg}' → {exact_any[0].id}", file=sys.stderr)
        return exact_any[0].id
    ci = [l for l in logs if l.display_name.lower() == arg.lower() and getattr(l, "lifecycle_state", "ACTIVE") == "ACTIVE"]
    if len(ci) == 1:
        if debug: print(f"[DEBUG] Log (ci) '{arg}' → {ci[0].id}", file=sys.stderr)
        return ci[0].id
    names = sorted(set(getattr(l, "display_name", "") for l in logs))
    raise ValueError(f"Log '{arg}' not found/unique in {log_group_id}. "
                     f"Available: {', '.join(names[:20])}{'...' if len(names)>20 else ''}")

# ───────────────────────── CLI ─────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Fetch OCI logs in parallel and export NDJSON (with progress bar if --debug not set).",
        fromfile_prefix_chars='@'
    )
    p.add_argument("--config-file", default="auth/config", help="OCI config path (default: auth/config)")
    p.add_argument("--profile", default="DEFAULT", help="OCI config profile (default: DEFAULT)")
    p.add_argument("--compartment-name", required=True, help="Compartment name or OCID")
    p.add_argument("--log-group", required=True, help="Log Group name or OCID")
    p.add_argument("--log-name", required=True, help="Log name or OCID")
    p.add_argument("--region", required=True, help="Region (e.g., us-phoenix-1)")
    p.add_argument("--timeframe", required=True, help="[MM-DD-YYYY:HH:MM:SS]-[...] or 15m/6h/2d/1w/3mo")

    # Defaults (your requested profile)
    p.add_argument("--slice", default="30m", help="Slice size (default: 30m). Use Ns/Nm/Nh/Nd/Nw.")
    p.add_argument("--limit", type=int, default=1000, help="Max items per page (default: 1000)")
    p.add_argument("--no-sort", dest="no_sort", action="store_true")
    p.add_argument("--sort", dest="no_sort", action="store_false", help="Enable sorting by datetime desc")
    p.set_defaults(no_sort=True)
    p.add_argument("--workers", type=int, default=4, help="Number of concurrent workers (default: 4)")
    p.add_argument("--max-events", type=int, default=None, help="Stop after this many events (optional)")

    p.add_argument("--connect-timeout", type=float, default=10.0, help="Connect timeout seconds (default: 10)")
    p.add_argument("--read-timeout", type=float, default=180.0, help="Read timeout seconds (default: 180)")

    p.add_argument("--max-retries", type=int, default=6, help="Max retries per page (default: 6)")
    p.add_argument("--backoff-initial", type=float, default=1.0, help="Initial backoff seconds (default: 1)")
    p.add_argument("--backoff-max", type=float, default=40.0, help="Max backoff seconds (default: 40)")
    p.add_argument("--jitter", type=float, default=0.5, help="Jitter factor 0..1 (default: 0.5)")

    p.add_argument("--where", help="Extra filter appended as '| where <expr>'")
    p.add_argument("--out", required=True, help="Output NDJSON file (append)")
    p.add_argument("--debug", action="store_true", help="Verbose diagnostics (disables progress bar)")
    return p.parse_args()

# ─────────────────────── Progress Bar ───────────────────────

class ProgressBar:
    def __init__(self, total_slices: int, enabled: bool, bar_len: int = 30, min_interval: float = 0.2):
        self.total = max(1, total_slices)
        self.done = 0
        self.start = time.time()
        self.bar_len = bar_len
        self.enabled = enabled
        self.lock = threading.Lock()
        self.spinner_seq = "|/-\\"
        self.spin_idx = 0
        self.last_render = 0.0
        self.min_interval = min_interval
        self._finished = False

    @staticmethod
    def _fmt_secs(s: float) -> str:
        if s != s or s is None:  # NaN
            return "--:--"
        s = int(max(0, s))
        h, rem = divmod(s, 3600)
        m, ss = divmod(rem, 60)
        if h > 0:
            return f"{h:d}h{m:02d}m{ss:02d}s"
        return f"{m:02d}m{ss:02d}s"

    def _render(self, spinner: str):
        pct = self.done / self.total
        filled = int(self.bar_len * pct)
        bar = "#" * filled + "-" * (self.bar_len - filled)
        elapsed = time.time() - self.start
        eta = (elapsed / self.done) * (self.total - self.done) if self.done > 0 else float("nan")
        msg = f"\r[{bar}] {pct * 100:5.1f}% {spinner} ETA {self._fmt_secs(eta)}  ({self.done}/{self.total} slices)"
        sys.stderr.write(msg)
        sys.stderr.flush()

    def tick_page(self):
        if not self.enabled or self._finished:
            return
        now = time.time()
        if now - self.last_render < self.min_interval:
            return
        with self.lock:
            if self._finished:
                return
            self.spin_idx = (self.spin_idx + 1) % len(self.spinner_seq)
            self._render(self.spinner_seq[self.spin_idx])
            self.last_render = now

    def done_slice(self):
        if not self.enabled or self._finished:
            return
        with self.lock:
            if self._finished:
                return
            self.done += 1
            # Force an immediate render on slice completion
            self.last_render = 0.0
            self._render(self.spinner_seq[self.spin_idx])

    def finish(self):
        if not self.enabled:
            return
        with self.lock:
            if self._finished:
                return
            self._finished = True
            bar = "#" * self.bar_len
            elapsed = time.time() - self.start
            sys.stderr.write(f"\r[{bar}] 100.0% ✓ done in {self._fmt_secs(elapsed)} ({self.done}/{self.total} slices)\n")
            sys.stderr.flush()

# ─────────────────────── Worker + Helpers ───────────────────────

def make_logsearch_client(base_config, region, connect_timeout, read_timeout):
    cfg = dict(base_config)
    cfg["region"] = region
    client = LogSearchClient(cfg, retry_strategy=None)  # custom retries
    client.base_client.timeout = (connect_timeout, read_timeout)
    return client

def respectful_sleep(delay, debug=False, reason=""):
    if debug:
        print(f"[DEBUG] sleep {delay:.2f}s {reason}", file=sys.stderr)
    time.sleep(delay)

def handle_pushback_and_backoff(e, attempt, backoff_initial, backoff_max, jitter, debug=False):
    retry_after = None
    if isinstance(e, oci.exceptions.ServiceError):
        headers = getattr(e, "headers", {}) or {}
        ra = headers.get("retry-after") or headers.get("Retry-After")
        if ra:
            try:
                retry_after = float(ra)
            except Exception:
                pass

    if retry_after is not None:
        delay = min(backoff_max, max(0.0, retry_after))
        respectful_sleep(delay, debug, reason="[Retry-After]")
        return

    base = min(backoff_max, backoff_initial * (2 ** (attempt - 1)))
    factor = 1.0 + random.uniform(-jitter, jitter)
    delay = max(0.2, base * factor)
    respectful_sleep(delay, debug, reason=f"[backoff attempt={attempt}]")

def fetch_slice(slice_idx, w_start, w_end, path, where, no_sort,
                base_config, region, limit, max_events_global,
                connect_timeout, read_timeout, max_retries,
                backoff_initial, backoff_max, jitter, debug, out_queue,
                progress_bar: ProgressBar, progress_lock: threading.Lock):
    """
    Executes paginated search for one slice and pushes NDJSON lines into out_queue.
    Returns number of events for the slice.
    """
    client = make_logsearch_client(base_config, region, connect_timeout, read_timeout)
    query = f'search "{path}"'
    if where:
        query += f' | where {where}'
    if not no_sort:
        query += ' | sort by datetime desc'

    page = None
    page_idx = 0
    total = 0

    while True:
        details = SearchLogsDetails(
            search_query=query,
            time_start=w_start,
            time_end=w_end,
            is_return_field_info=False
        )

        attempt = 0
        t0 = time.time()
        while True:
            try:
                resp = client.search_logs(
                    search_logs_details=details,
                    limit=limit,
                    page=page
                )
                break
            except (oci.exceptions.RequestException, oci.exceptions.ServiceError) as e:
                attempt += 1
                if isinstance(e, oci.exceptions.ServiceError):
                    if e.status and e.status not in (429, 500, 502, 503, 504):
                        raise
                if attempt > max_retries:
                    raise
                handle_pushback_and_backoff(e, attempt, backoff_initial, backoff_max, jitter, debug)

        elapsed = time.time() - t0
        body = resp.data
        results = body.results or []
        batch = 0
        for r in results:
            out_queue.put(json.dumps(r.data, ensure_ascii=False))
            total += 1
            batch += 1
            if max_events_global is not None and total >= max_events_global:
                break

        next_page = resp.headers.get("opc-next-page")

        if debug:
            with progress_lock:
                print(f"[DEBUG] slice#{slice_idx} {w_start.isoformat()}..{w_end.isoformat()} | "
                      f"page {page_idx} | {batch} results | elapsed {elapsed:.2f}s | "
                      f"next_page={'yes' if next_page else 'no'}", file=sys.stderr)
        else:
            progress_bar.tick_page()

        if (max_events_global is not None and total >= max_events_global) or not next_page:
            break

        page = next_page
        page_idx += 1

    # slice finished
    if not debug:
        progress_bar.done_slice()
    return total

# ───────────────────────── Main ─────────────────────────

def main():
    args = parse_args()

    base_config = oci.config.from_file(args.config_file, args.profile)
    base_config["region"] = args.region

    # Non-concurrent resolvers
    identity = IdentityClient(base_config)
    lm = LoggingManagementClient(base_config)

    # Resolve IDs
    compartment_id = resolve_compartment_id(identity, base_config["tenancy"], args.compartment_name, debug=args.debug)
    log_group_id   = resolve_log_group_id(lm, compartment_id, args.log_group, debug=args.debug)
    log_id         = resolve_log_id(lm, log_group_id, args.log_name, debug=args.debug)

    # Time planning
    start_dt, end_dt, label = parse_timeframe(args.timeframe)
    start_dt, end_dt, clamped = clamp_14_days(start_dt, end_dt, debug=args.debug)

    # Slicing
    slice_td = parse_duration(args.slice) if args.slice else None

    # Build path with OCIDs (fast)
    path = f"{compartment_id}/{log_group_id}/{log_id}"

    # Build slice plan
    slices = []
    if slice_td:
        cursor = start_dt
        idx = 0
        while cursor < end_dt:
            w_end = min(cursor + slice_td, end_dt)
            slices.append((idx, cursor, w_end))
            cursor = w_end
            idx += 1
    else:
        slices = [(0, start_dt, end_dt)]

    # Progress bar enabled when not in debug
    pb = ProgressBar(total_slices=len(slices), enabled=not args.debug)

    if args.debug:
        print(f"[DEBUG] region              = {args.region}", file=sys.stderr)
        print(f"[DEBUG] compartment_id      = {compartment_id}", file=sys.stderr)
        print(f"[DEBUG] log_group_id        = {log_group_id}", file=sys.stderr)
        print(f"[DEBUG] log_id              = {log_id}", file=sys.stderr)
        print(f"[DEBUG] timeframe_label     = {label}", file=sys.stderr)
        print(f"[DEBUG] start_time_utc      = {start_dt.isoformat()}", file=sys.stderr)
        print(f"[DEBUG] end_time_utc        = {end_dt.isoformat()}", file=sys.stderr)
        print(f"[DEBUG] clamped_to_14_days  = {clamped}", file=sys.stderr)
        print(f"[DEBUG] slices planned       = {len(slices)} (size={args.slice})", file=sys.stderr)
        print(f"[DEBUG] search path         = {path}", file=sys.stderr)
        print(f"[DEBUG] sort enabled        = {not args.no_sort}", file=sys.stderr)
        if args.where:
            print(f"[DEBUG] where               = {args.where}", file=sys.stderr)

    # Writer thread
    out_q = queue.Queue(maxsize=10000)
    stop_sentinel = object()

    def writer():
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "a", encoding="utf-8") as f:
            while True:
                item = out_q.get()
                if item is stop_sentinel:
                    break
                f.write(item + "\n")
                out_q.task_done()

    wt = threading.Thread(target=writer, daemon=True)
    wt.start()

    # Launch workers
    progress_lock = threading.Lock()
    total_events = 0
    futures = []

    try:
        with ThreadPoolExecutor(max_workers=max(1, min(args.workers, len(slices)))) as ex:
            for (idx, s, e) in slices:
                fut = ex.submit(
                    fetch_slice,
                    idx, s, e, path, args.where, args.no_sort,
                    base_config, args.region, args.limit, args.max_events,
                    args.connect_timeout, args.read_timeout, args.max_retries,
                    args.backoff_initial, args.backoff_max, args.jitter,
                    args.debug, out_q, pb, progress_lock
                )
                futures.append(fut)

            for fut in as_completed(futures):
                try:
                    total_events += fut.result()
                except Exception as e:
                    # Make sure the bar doesn't leave the cursor mid-line
                    if not args.debug:
                        sys.stderr.write("\n")
                        sys.stderr.flush()
                    print(f"[ERROR] Worker failed: {type(e).__name__}: {e}", file=sys.stderr)
    finally:
        # Close writer
        out_q.put(stop_sentinel)
        wt.join()
        # Finish progress bar
        if not args.debug:
            pb.finish()

    print(f"[INFO] {total_events} events written to {args.out}", file=sys.stderr)

if __name__ == "__main__":
    main()
