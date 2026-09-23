#!/usr/bin/env python3
"""
================================================================================
 kalshi_api_learn.py  —  learn the Kalshi API with YOUR key, safely
================================================================================
You can plug in your REAL Kalshi key and this will read your account (balance,
positions, market data). It will NOT place real-money orders — order placement
is hard-locked to Kalshi's DEMO environment. That boundary is enforced in code,
not just by a comment, and matches this project's paper/demo-only scope.

Two safety layers:
  KALSHI_ENV = "prod"  -> real account, READ-ONLY. create_order/cancel refuse.
  KALSHI_ENV = "demo"  -> fake money. orders allowed, still gated by DRY_RUN.
  DRY_RUN    = True     -> even on demo, order funcs print instead of sending.

--------------------------------------------------------------------------------
 HOW THE KEY WORKS
--------------------------------------------------------------------------------
Kalshi doesn't use a plain secret string. You make an RSA key PAIR, upload the
PUBLIC half to Kalshi (which gives you an "API Key ID" UUID), and keep the
PRIVATE half secret. Every request is SIGNED with the private key; Kalshi checks
it against your public key. No secret ever crosses the wire, and a signature
can't be replayed for a different request.

You sign exactly:  timestamp_ms + METHOD + path_without_query
and send headers: KALSHI-ACCESS-KEY, KALSHI-ACCESS-TIMESTAMP, KALSHI-ACCESS-SIGNATURE.
Classic mistakes: seconds instead of milliseconds; signing the path with its
?query attached (sign the bare path).

--------------------------------------------------------------------------------
 SETUP (secrets come from environment variables — nothing hardcoded)
--------------------------------------------------------------------------------
  pip install requests cryptography
  openssl genrsa -out kalshi_private_key.pem 4096
  openssl rsa -in kalshi_private_key.pem -pubout -out kalshi_public_key.pem
  # upload kalshi_public_key.pem in Kalshi Settings -> API Keys, copy the Key ID

  PowerShell (this terminal session):
    $env:KALSHI_API_KEY_ID   = "your-key-id-uuid"
    $env:KALSHI_PRIVATE_KEY_PATH = "kalshi_private_key.pem"
    $env:KALSHI_ENV = "prod"      # "prod" = read your real account; "demo" = simulate orders
    py kalshi_api_learn.py
================================================================================
"""

import os
import base64
import datetime
import time
import uuid
import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

DRY_RUN = True   # keep True while learning; even on demo, orders only print

# ---- config from environment (never hardcode secrets) ----
API_KEY_ID       = os.environ.get("KALSHI_API_KEY_ID", "")
PRIVATE_KEY_PATH = os.environ.get("KALSHI_PRIVATE_KEY_PATH", "kalshi_private_key.pem")
ENV              = os.environ.get("KALSHI_ENV", "demo").lower()   # "demo" or "prod"

BASE = "https://demo-api.kalshi.co" if ENV == "demo" else "https://api.elections.kalshi.com"
# NOTE: BASE is the HOST ONLY. Each method adds the full path (e.g. "/trade-api/v2/...").
# Kalshi signs that same full path, so BASE must not repeat it.


class KalshiClient:
    def __init__(self, api_key_id, private_key_path):
        if not api_key_id:
            raise SystemExit("Set KALSHI_API_KEY_ID (see the SETUP notes).")
        self.api_key_id = api_key_id
        with open(private_key_path, "rb") as f:
            self.private_key = serialization.load_pem_private_key(f.read(), password=None)

    def _headers(self, method, path):
        ts = str(int(datetime.datetime.now().timestamp() * 1000))     # milliseconds
        message = (ts + method + path).encode("utf-8")                # bare path
        sig = self.private_key.sign(
            message,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()),
                        salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode("utf-8"),
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "Content-Type": "application/json",
        }

    def _get(self, path, params=None):
        r = requests.get(BASE + path, headers=self._headers("GET", path), params=params, timeout=10)
        r.raise_for_status(); return r.json()

    def _post(self, path, body):
        r = requests.post(BASE + path, headers=self._headers("POST", path), json=body, timeout=10)
        if r.status_code not in (200, 201):
            raise RuntimeError(f"POST {path} -> {r.status_code}: {r.text}")
        return r.json()

    def _delete(self, path):
        r = requests.delete(BASE + path, headers=self._headers("DELETE", path), timeout=10)
        if r.status_code not in (200, 202, 204):
            raise RuntimeError(f"DELETE {path} -> {r.status_code}: {r.text}")
        return True

    # ---------- READ-ONLY (safe with your REAL key) ----------
    def balance(self):
        return self._get("/trade-api/v2/portfolio/balance")

    def positions(self):
        return self._get("/trade-api/v2/portfolio/positions")

    def market(self, ticker):
        return self._get(f"/trade-api/v2/markets/{ticker}").get("market", {})

    def resting_orders(self):
        return self._get("/trade-api/v2/portfolio/orders", params={"status": "resting"})

    def order(self, order_id):
        return self._get(f"/trade-api/v2/portfolio/orders/{order_id}").get("order", {})

    def contracts_for(self, pct, price_cents):
        bal = self.balance().get("balance", 0)
        return int((pct / 100.0) * bal / price_cents) if price_cents > 0 else 0

    # ---------- ORDERS: hard-locked to demo ----------
    def _orders_allowed(self):
        # THE BOUNDARY: real-account order placement is refused, on purpose.
        if ENV != "demo":
            raise PermissionError(
                "Order placement is disabled outside the demo environment. "
                "This tool never sends real-money orders. Set KALSHI_ENV=demo (with a demo key) "
                "to practice, or write your own live layer if you choose to go live.")

    def create_order(self, ticker, action, side, count, price_cents):
        self._orders_allowed()
        body = {"ticker": ticker, "action": action, "side": side, "count": count,
                "type": "limit", "yes_price": price_cents,
                "client_order_id": str(uuid.uuid4())}
        if DRY_RUN:
            print("[DRY_RUN demo] POST /portfolio/orders", body)
            return {"dry_run": True, "body": body}
        return self._post("/trade-api/v2/portfolio/orders", body)["order"]

    def cancel(self, order_id):
        self._orders_allowed()
        if DRY_RUN:
            print(f"[DRY_RUN demo] DELETE order {order_id}")
            return {"dry_run": True}
        return self._delete(f"/trade-api/v2/portfolio/orders/{order_id}")


def example_flow(cli, ticker, entry_cents, stop_cents, pct):
    count = cli.contracts_for(pct, entry_cents)
    print(f"Sizing: {pct}% at {entry_cents}c -> {count} contracts")
    if count < 1:
        print("Too small to trade."); return

    # The ENTRY is a normal order.
    entry = cli.create_order(ticker, "buy", "yes", count, entry_cents)
    print("Entry:", entry)

    # THE STOP IS NOT AN ORDER YOU PLACE UP FRONT.
    # A sell limit priced BELOW the current bid is "marketable" — it crosses the
    # book and fills immediately. So resting a sell at your stop right after
    # entering would exit you on the spot, not protect you. Kalshi has no native
    # stop order, so a real stop must be CLIENT-SIDE and event-driven:
    #
    #   loop while the position is open:
    #       bid = current executable exit price (the YES bid)
    #       if bid <= stop_cents:            # the stop condition is now TRUE
    #           sell your contracts          # only NOW do you submit the exit
    #           break
    #       else:
    #           wait a moment and check again
    #
    # This file deliberately does NOT run that loop — it's the trigger that turns a
    # watcher into a live auto-trader, and this stays a read/demo learning tool.
    print(f"Stop is client-side: watch the YES bid and sell only if it falls to {stop_cents}c.")
    print("(Not placed here — a resting sell below the bid would fill instantly.)")


def main():
    cli = KalshiClient(API_KEY_ID, PRIVATE_KEY_PATH)
    print(f"env={ENV}  base={BASE}  dry_run={DRY_RUN}")
    print("Balance:", cli.balance())          # works with your real key (read-only)
    if ENV == "demo":
        example_flow(cli, "KXBTC15M-EXAMPLE", entry_cents=90, stop_cents=68, pct=2)
    else:
        print("Read-only mode (prod). Order functions are disabled here by design.")


if __name__ == "__main__":
    main()
