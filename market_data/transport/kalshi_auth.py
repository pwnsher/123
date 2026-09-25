"""
READ-ONLY Kalshi websocket authentication for the cfbenchmarks_value channel.

Kalshi signs `timestamp_ms + METHOD + path` with the account's RSA private key (RSA-PSS, SHA-256) and
sends KALSHI-ACCESS-KEY / KALSHI-ACCESS-TIMESTAMP / KALSHI-ACCESS-SIGNATURE on the websocket handshake.

Boundary: this module will only ever sign `GET /trade-api/ws/...` (the websocket upgrade). Any other
method or path raises; it cannot authorise orders, transfers or any REST call. Credentials come from
KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH (the same variables kalshi_api_learn.py documents), are
never logged, and are reported elsewhere only as configured / not configured.
"""
import base64
import os

WS_PATH_PREFIX = "/trade-api/ws/"


class AuthUnavailable(RuntimeError):
    pass


def configured(environ=None):
    e = os.environ if environ is None else environ
    return bool(e.get("KALSHI_API_KEY_ID")) and bool(e.get("KALSHI_PRIVATE_KEY_PATH")) \
        and os.path.isfile(e.get("KALSHI_PRIVATE_KEY_PATH", ""))


def check_request(method, path):
    if method != "GET" or not path.startswith(WS_PATH_PREFIX):
        raise PermissionError("read-only websocket auth: only GET /trade-api/ws/... may be signed")


def ws_headers(path, timestamp_ms, environ=None):
    check_request("GET", path)
    e = os.environ if environ is None else environ
    if not configured(e):
        raise AuthUnavailable("KALSHI_API_KEY_ID / KALSHI_PRIVATE_KEY_PATH not configured")
    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding
    except ImportError as err:
        raise AuthUnavailable("the 'cryptography' package is required for authenticated CF data") from err
    with open(e["KALSHI_PRIVATE_KEY_PATH"], "rb") as f:
        key = serialization.load_pem_private_key(f.read(), password=None)
    msg = f"{int(timestamp_ms)}GET{path}".encode()
    sig = key.sign(msg, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
                   hashes.SHA256())
    return {"KALSHI-ACCESS-KEY": e["KALSHI_API_KEY_ID"], "KALSHI-ACCESS-TIMESTAMP": str(int(timestamp_ms)),
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode()}


def cfb_direct_headers(environ=None):
    e = os.environ if environ is None else environ
    if not (e.get("CFB_API_ID") and e.get("CFB_API_SECRET")):
        raise AuthUnavailable("CFB_API_ID / CFB_API_SECRET not configured")
    token = base64.b64encode(f"{e['CFB_API_ID']}:{e['CFB_API_SECRET']}".encode()).decode()
    return {"Authorization": "Basic " + token}
