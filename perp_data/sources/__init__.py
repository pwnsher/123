"""Perp venue adapters: raw venue message text -> PerpEvents. No I/O, no clocks (the caller passes
receive times), so normalization is deterministic and re-runnable from the stored raw text."""
