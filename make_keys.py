#!/usr/bin/env python3
"""
make_keys.py — generate a Kalshi RSA key pair without OpenSSL.

Run once:   py make_keys.py

Creates two files in the current folder:
  kalshi_private_key.pem   <- SECRET. Never share, upload, or screenshot this.
  kalshi_public_key.pem    <- upload THIS one to Kalshi (Settings -> API Keys).

If a private key already exists it will NOT overwrite it (so you can't wipe a key
you've already registered by mistake). Delete it yourself first if you truly want
a fresh pair.
"""

import os
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

PRIV = "kalshi_private_key.pem"
PUB  = "kalshi_public_key.pem"

def main():
    if os.path.exists(PRIV):
        print(f"{PRIV} already exists — not overwriting. Delete it first for a new pair.")
        return

    # 4096-bit RSA, the size Kalshi expects
    key = rsa.generate_private_key(public_exponent=65537, key_size=4096)

    with open(PRIV, "wb") as f:
        f.write(key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        ))
    with open(PUB, "wb") as f:
        f.write(key.public_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        ))

    print(f"Created {PRIV} (keep secret) and {PUB} (upload this to Kalshi).")
    print("Next: Kalshi -> Settings -> API Keys -> add key -> paste the contents of")
    print(f"{PUB}, then copy the API Key ID it gives you.")

if __name__ == "__main__":
    main()
