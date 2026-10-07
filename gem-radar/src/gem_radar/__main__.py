"""Command line: python -m gem_radar <command> ...

  scan <contract> [--chain C] [--refresh] [--deep auto|force|off] [--json]
  recheck <contract> [--chain C] [--json]
  watch add|remove|list [<contract>] [--chain C]
  history <contract> [--chain C] [--limit N] [--json]
  finalize <scan_id> --model M      (stdin: result JSON with analysis.interpretation)
  status | panel-data               (local reads only; network disabled)
  probe                             (checks provider reachability; makes requests)
"""
from __future__ import annotations

import argparse
import json
import sys

from .commands import gem, radar, recheck, watch
from .core.errors import GemRadarError
from .storage import db, history
from .ui import formatters


def _emit(obj, as_json: bool, text: str) -> None:
    if as_json:
        print(json.dumps({"result": obj, "text": text}, default=str))
    else:
        print(text)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="gem-radar", description="Heuristic memecoin risk scoring")
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("scan")
    s.add_argument("contract")
    s.add_argument("--chain")
    s.add_argument("--refresh", action="store_true")
    s.add_argument("--deep", choices=["auto", "force", "off"], default="auto")
    s.add_argument("--json", action="store_true")
    s.add_argument("--no-save", action="store_true")
    s.add_argument("--for-mod", action="store_true", help=argparse.SUPPRESS)
    r = sub.add_parser("recheck")
    r.add_argument("contract")
    r.add_argument("--chain")
    r.add_argument("--json", action="store_true")
    r.add_argument("--for-mod", action="store_true", help=argparse.SUPPRESS)
    w = sub.add_parser("watch")
    w.add_argument("action", choices=["add", "remove", "list"])
    w.add_argument("contract", nargs="?")
    w.add_argument("--chain")
    w.add_argument("--json", action="store_true")
    h = sub.add_parser("history")
    h.add_argument("contract")
    h.add_argument("--chain")
    h.add_argument("--limit", type=int, default=10)
    h.add_argument("--json", action="store_true")
    f = sub.add_parser("finalize")
    f.add_argument("scan_id", type=int)
    f.add_argument("--model", required=True)
    f.add_argument("--json", action="store_true")
    sub.add_parser("status")
    sub.add_parser("panel-data")
    sub.add_parser("probe")
    a = ap.parse_args(argv)

    try:
        if a.cmd == "scan":
            res = gem.scan(a.contract, chain=a.chain, refresh=a.refresh, deep=a.deep,
                           persist=not a.no_save, interpreter=a.for_mod)
            _emit(res, a.json, formatters.render(res))
            return 0 if res["kind"] == "verdict" else 2
        if a.cmd == "recheck":
            res = recheck.recheck(a.contract, chain=a.chain, interpreter=a.for_mod)
            _emit(res, a.json, formatters.render(res))
            return 0 if res["kind"] == "verdict" else 2
        conn = db.connect()
        if a.cmd == "watch":
            if a.action == "list":
                text = watch.show_list(conn)
            elif not a.contract:
                text = f"usage: watch {a.action} <contract>"
            elif a.action == "add":
                text = watch.add(conn, a.contract, a.chain)
            else:
                text = watch.remove(conn, a.contract, a.chain)
            _emit(radar.panel(conn)["watchlist"], a.json, text)
            return 0
        if a.cmd == "history":
            text, data = watch.show_history(conn, a.contract, a.chain, a.limit)
            _emit(data, a.json, text)
            return 0
        if a.cmd == "finalize":
            res = json.loads(sys.stdin.read())
            interp = (res.get("analysis") or {}).get("interpretation")
            if res.get("scan_id") != a.scan_id:
                raise GemRadarError("scan id mismatch")
            res["analysis"]["model"] = a.model
            history.annotate(conn, a.scan_id, model=a.model, interpretation=interp or "")
            _emit(res, a.json, formatters.render(res))
            return 0
        if a.cmd == "status":
            print(json.dumps(radar.status(conn), indent=1))
            return 0
        if a.cmd == "panel-data":
            print(json.dumps(radar.panel(conn), default=str))
            return 0
        if a.cmd == "probe":
            from .data.probe import probe
            print(json.dumps(probe(), indent=1))
            return 0
    except GemRadarError as e:
        print(json.dumps({"error": e.to_dict()}) if getattr(a, "json", False) else f"error: {e}")
        return 1
    return 1


if __name__ == "__main__":
    sys.exit(main())
