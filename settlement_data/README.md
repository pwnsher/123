# settlement_data/ — local settlement research store (git-ignored except this file)

Captured CF Benchmarks / Kalshi data imported with `py scripts/settlement_import.py` lands here as
append-only JSON-lines stores (`*.jsonl`). Every tool in `scripts/` reads `settlement_data/*.jsonl` by default
and works fully offline. See `docs/SETTLEMENT_ENGINE.md` ("Collecting validation data").

Never put credentials here. Stores refuse secret-looking fields.
