"""TEST/MOCK fixtures. Shapes follow each provider's public API documentation;
every value is invented for tests and is NOT market data."""
from __future__ import annotations

import time

MOCK_LABEL = "TEST/MOCK fixture — invented values, not market data"

EVM_TOKEN = "0x1111111111111111111111111111111111111111"
EVM_PAIR = "0x2222222222222222222222222222222222222222"
SOL_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # format-valid key used as a test id

NOW_MS = int(time.time() * 1000)


def dexscreener(chains=("ethereum",), token=EVM_TOKEN, liquidity=1_000_000.0):
    pairs = []
    for i, c in enumerate(chains):
        pairs.append({
            "chainId": c, "dexId": "uniswap", "url": "https://dexscreener.com/x",
            "pairAddress": EVM_PAIR if i == 0 else f"0x{str(i) * 40}",
            "baseToken": {"address": token, "name": "Mock Token", "symbol": "MOCK"},
            "quoteToken": {"address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                           "symbol": "WETH"},
            "priceUsd": "0.0012", "txns": {"h24": {"buys": 900, "sells": 700}},
            "volume": {"h24": 750000}, "priceChange": {"h24": -3.5},
            "liquidity": {"usd": liquidity}, "fdv": 12000000, "marketCap": 11000000,
            "pairCreatedAt": NOW_MS - 200 * 86400 * 1000,
            "info": {"websites": [{"label": "Website", "url": "https://example.invalid"}],
                     "socials": [{"type": "twitter", "url": "https://x.invalid/mock"},
                                 {"type": "telegram", "url": "https://t.invalid/mock"}]},
        })
    return {"_comment": MOCK_LABEL, "schemaVersion": "1.0.0", "pairs": pairs}


def goplus_evm(token=EVM_TOKEN, **over):
    rec = {
        "buy_tax": "0", "sell_tax": "0.03", "cannot_sell_all": "0", "is_honeypot": "0",
        "is_mintable": "0", "owner_address": "0x000000000000000000000000000000000000dead",
        "owner_change_balance": "0", "hidden_owner": "0", "can_take_back_ownership": "0",
        "transfer_pausable": "0", "is_blacklisted": "0", "is_proxy": "0", "is_open_source": "1",
        "slippage_modifiable": "0", "personal_slippage_modifiable": "0", "trading_cooldown": "0",
        "holder_count": "15234", "creator_address": "0x3333333333333333333333333333333333333333",
        "creator_percent": "0.004", "owner_percent": "0",
        "holders": [
            {"address": EVM_PAIR, "percent": "0.30", "is_contract": 1, "tag": "UniswapV2"},
            {"address": "0x000000000000000000000000000000000000dead", "percent": "0.10"},
            {"address": "0x4444444444444444444444444444444444444444", "percent": "0.04"},
            {"address": "0x5555555555555555555555555555555555555555", "percent": "0.03"},
            {"address": "0x6666666666666666666666666666666666666666", "percent": "0.02"},
        ],
        "lp_holders": [
            {"address": "0x000000000000000000000000000000000000dead", "percent": "0.97",
             "is_locked": 0},
            {"address": "0x7777777777777777777777777777777777777777", "percent": "0.03",
             "is_locked": 0},
        ],
        "dex": [{"name": "UniswapV2", "liquidity": "980000.5", "pair": EVM_PAIR}],
    }
    rec.update(over)
    return {"_comment": MOCK_LABEL, "code": 1, "message": "OK", "result": {token.lower(): rec}}


def honeypot_is(is_honeypot=False, sell_tax=3.0):
    return {"_comment": MOCK_LABEL, "simulationSuccess": True,
            "honeypotResult": {"isHoneypot": is_honeypot},
            "simulationResult": {"buyTax": 0, "sellTax": sell_tax, "transferTax": 0},
            "contractCode": {"openSource": True, "isProxy": False},
            "token": {"totalHolders": 15000}, "summary": {"risk": "low"}}


def rugcheck(mint=SOL_MINT, mint_authority=None, liquidity=500000.0):
    return {
        "_comment": MOCK_LABEL, "mint": mint, "creator": "CreatorMockWa11et1111111111111111111111111",
        "token": {"mintAuthority": mint_authority, "freezeAuthority": None, "supply": 10 ** 15,
                  "decimals": 6},
        "tokenMeta": {"mutable": False}, "rugged": False, "totalHolders": 8200,
        "totalMarketLiquidity": liquidity,
        "markets": [{"pubkey": "PoolMock1111111111111111111111111111111111",
                     "liquidityAAccount": "VaultA111111111111111111111111111111111111",
                     "liquidityBAccount": "VaultB111111111111111111111111111111111111",
                     "lp": {"lpLockedPct": 99.5, "baseUSD": 250000, "quoteUSD": 250000}}],
        "topHolders": [
            {"address": "VaultA111111111111111111111111111111111111", "pct": 20.0,
             "owner": "AmmAuth11111111111111111111111111111111111"},
            {"address": "Holder1111111111111111111111111111111111111", "pct": 6.0,
             "owner": "Owner11111111111111111111111111111111111111"},
            {"address": "Holder2222222222222222222222222222222222222", "pct": 4.0,
             "owner": "CreatorMockWa11et1111111111111111111111111"},
            {"address": "Holder3333333333333333333333333333333333333", "pct": 2.5,
             "owner": "Owner33333333333333333333333333333333333333"},
        ],
        "risks": [{"name": "Low amount of LP Providers", "level": "warn"}],
    }


def solana_rpc_mint(mint_authority=None, freeze_authority=None, token_2022=False, extensions=None):
    info = {"decimals": 6, "isInitialized": True, "supply": "1000000000000000",
            "mintAuthority": mint_authority, "freezeAuthority": freeze_authority}
    if extensions is not None:
        info["extensions"] = extensions
    owner = ("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb" if token_2022
             else "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
    return {"jsonrpc": "2.0", "id": 1, "result": {"context": {"slot": 1}, "value": {
        "owner": owner, "data": {"parsed": {"type": "mint", "info": info}, "program": "spl-token"}}}}
