"""
Separate, read-only RESEARCH status page for the collector (127.0.0.1 only).

    GET /status   JSON: source health / latency / reconnects, CF RTI, multi-exchange reference,
                  settlement accumulator, high-resolution volatility, capture counters
    GET /         a small self-refreshing HTML table of the same (no external scripts)

It is a different process/port from the production dashboard (kalshi_dashboard.py is untouched) and
has no control endpoints: it cannot change anything, least of all calls.
"""
import html
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PAGE = """<!doctype html><meta charset=utf-8><title>Research data capture</title>
<meta http-equiv=refresh content=2><style>body{font:13px monospace;background:#0b0f0d;color:#d6e6df;margin:16px}
td,th{border:1px solid #1d2a24;padding:3px 6px;text-align:left}table{border-collapse:collapse;margin:8px 0}
.b{color:#e6b84c}</style><h3>Research data capture (RESEARCH ONLY - no effect on calls)</h3>%s"""


def render(st):
    rows = ["<p>session <b>%s</b> &middot; %s &middot; events %s &middot; gaps %s &middot; failures %s</p>" % (
        html.escape(str(st.get("session_id"))), html.escape(str(st.get("now_utc"))), st["counts"].get("events"),
        st["counts"].get("gaps"), st["counts"].get("failures"))]
    rows.append("<table><tr><th>source</th><th>state</th><th>msgs</th><th>reconnects</th><th>last error</th></tr>")
    for n, h in (st.get("sources") or {}).items():
        rows.append("<tr><td>%s</td><td class=b>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>" % (
            html.escape(n), html.escape(str(h.get("state"))), h.get("messages"), h.get("reconnect_attempts"),
            html.escape(str(h.get("last_error") or ""))))
    rows.append("</table>")
    for a, d in (st.get("assets") or {}).items():
        rows.append("<table><tr><th colspan=3>%s &middot; %s</th></tr>" % (html.escape(a), html.escape(str(d.get("market")))))
        for k, v in d["features"].items():
            val = v["value"]
            rows.append("<tr><td>%s</td><td>%s</td><td>%s</td></tr>" % (
                html.escape(k), html.escape(f"{val:.6g}" if isinstance(val, float) else str(val)), html.escape(v["status"])))
        rows.append("</table>")
    return PAGE % "".join(rows)


def serve(status_fn, port):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            try:
                st = status_fn()
            except Exception as e:                              # noqa: BLE001
                st = {"error": str(e)[:200], "counts": {}}
            if self.path.startswith("/status"):
                body, ctype = json.dumps(st, default=str).encode(), "application/json"
            else:
                body, ctype = render(st).encode(), "text/html; charset=utf-8"
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    srv = ThreadingHTTPServer(("127.0.0.1", port), H)
    threading.Thread(target=srv.serve_forever, name="research-status", daemon=True).start()
    return srv
