#!/usr/bin/env python3
"""
Kalshi Dashboard — Discord control bot (slash commands)

LEGACY, OPTIONAL ADAPTER. Discord is not part of the local-first architecture. The core watcher,
strategy, dashboard and evaluation tools never import this file or discord.py; run them locally
with `py run_local.py` (or `py kalshi_dashboard.py`). This adapter is kept, unchanged in
behaviour, for anyone who still wants the Discord channel posts and slash commands.

Wraps kalshi_dashboard.py so you can run it from Discord with /commands and shows
an online/offline status by renaming a channel. It is READ-ONLY: it starts,
stops, reports, and explains the watcher. It never places a trade.

Commands
    /start     resume the watcher, set the channel to online
    /stop      pause the watcher, set the channel to offline
    /status    show each coin's current verdict and the live W/L
    /backtest  show the latest backtest summary
    /explain   post the how-it-works explainer to its channel

Setup (one time)
    pip install -U discord.py
    Fill in BOT_TOKEN, GUILD_ID, STATUS_CHANNEL_ID below, then:
    python kalshi_bot.py

Keep kalshi_bot.py in the SAME folder as kalshi_dashboard.py.
"""

import os
import asyncio
import threading
import datetime as dt
from http.server import ThreadingHTTPServer

try:
    import discord
    from discord import app_commands
except ImportError as _e:                      # optional dependency of this legacy adapter only
    raise SystemExit("kalshi_bot.py is the OPTIONAL legacy Discord adapter and needs discord.py "
                     "(pip install -U discord.py).\nTo run locally without Discord:  py run_local.py") from _e

import kalshi_dashboard as k
from kalshi_core.config import discord_token_usable

# ─────────────────────── CONFIG (fill these in) ───────────────────────
BOT_TOKEN         = os.environ.get("DISCORD_BOT_TOKEN") or "PASTE_YOUR_BOT_TOKEN_HERE"  # fine to hardcode (shared, trusted)
# IDs are not secrets; each can be overridden by an environment variable (defaults unchanged).
GUILD_ID          = int(os.environ.get("DISCORD_GUILD_ID") or 1550200013324288040)     # 0 = global sync (~1 hour)
STATUS_CHANNEL_ID = int(os.environ.get("DISCORD_STATUS_CHANNEL_ID") or 1550274779121192990)  # NAME flips online/offline
CALLS_CHANNEL_ID  = int(os.environ.get("DISCORD_CALLS_CHANNEL_ID") or 1550200016071823422)   # calls + buttons
JOURNAL_CHANNEL_ID = int(os.environ.get("DISCORD_JOURNAL_CHANNEL_ID") or 0)   # results + weekly; 0 = calls channel
EXPLAIN_CHANNEL_ID = int(os.environ.get("DISCORD_EXPLAIN_CHANNEL_ID") or 0)   # explainer; 0 = calls channel
ONLINE_NAME       = "🟢-bot-online"    # rename to plain text if you prefer no dot
OFFLINE_NAME      = "🔴-bot-offline"

# ─────────────────────── bot scaffolding ───────────────────────
intents = discord.Intents.default()          # slash commands need no privileged intents
ACTIVITY_ON  = discord.Activity(type=discord.ActivityType.watching, name="Kalshi 15m markets")
ACTIVITY_OFF = discord.Activity(type=discord.ActivityType.watching, name="paused")
bot = discord.Client(intents=intents, activity=ACTIVITY_ON, status=discord.Status.online)
tree = app_commands.CommandTree(bot)
_started = threading.Event()                  # dashboard threads started once
_synced = False                               # slash commands synced once

def start_dashboard():
    if _started.is_set():
        return
    _started.set()
    threading.Thread(target=k.poller, daemon=True).start()
    threading.Thread(target=k.backtest_worker, daemon=True).start()
    threading.Thread(target=k.weekly_worker, daemon=True).start()
    try:
        srv = ThreadingHTTPServer(("127.0.0.1", k.PORT), k.Handler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        print(f"Dashboard threads live — http://localhost:{k.PORT}")
    except OSError as e:
        print(f"web dashboard not started (is another copy already running on port {k.PORT}?) {e}")

async def set_channel(name):
    """Rename the status channel, but only if it actually changed (Discord limits
    channel renames to ~2 per 10 minutes)."""
    if not STATUS_CHANNEL_ID:
        return
    ch = bot.get_channel(STATUS_CHANNEL_ID)
    if ch is None:
        try:
            ch = await bot.fetch_channel(STATUS_CHANNEL_ID)
        except discord.Forbidden:
            print("status channel: bot lacks access — grant it 'View Channel' + 'Manage Channels' on that channel")
            return
        except discord.HTTPException as e:
            print(f"cannot see status channel {STATUS_CHANNEL_ID}: {e}")
            return
    if ch and ch.name != name:
        try:
            await ch.edit(name=name)
        except discord.Forbidden:
            print("rename failed: give the bot 'Manage Channels' on that channel")
        except discord.HTTPException as e:
            print(f"rename failed (rate limit?): {e}")

# ─────────────────────── status embeds ───────────────────────
class CallView(discord.ui.View):
    def __init__(self, ticker, side, stop):
        super().__init__(timeout=900)   # 15 min, the life of the market
        s = f"{stop:.0f}\u00a2" if stop is not None else "your stop level"
        self.exit_txt = (
            f"**Exit plan — {ticker} ({side})**\n"
            f"Kalshi has no stop order, so to cap the loss, place a **limit SELL at {s}** on this "
            f"position right after you enter.\n"
            f"Heads-up: in a fast move a resting limit sell may not fill, so also watch the CF index "
            f"and sell at market if it turns hard against you. Stand down in the last 2 minutes.\n"
            f"Market: {k.kalshi_link(ticker)}")
        self.add_item(discord.ui.Button(label="1 · Open market", style=discord.ButtonStyle.link,
                                        url=k.kalshi_link(ticker)))
        b = discord.ui.Button(label="2 · Exit plan (limit sell)", style=discord.ButtonStyle.secondary)
        b.callback = self._exit
        self.add_item(b)

    async def _exit(self, interaction: discord.Interaction):
        await interaction.response.send_message(self.exit_txt, ephemeral=True)

async def _send_call(coin, r, crec):
    try:
        ch = bot.get_channel(CALLS_CHANNEL_ID) or await bot.fetch_channel(CALLS_CHANNEL_ID)
        emb = discord.Embed.from_dict(k.build_call_embed(coin, r, crec))
        await ch.send(embed=emb, view=CallView(r["ticker"], r["fav"], r.get("rec_stop")))
    except Exception as e:
        print(f"send call failed: {e}")

def _on_call(coin, r, crec):
    asyncio.run_coroutine_threadsafe(_send_call(coin, r, crec), bot.loop)

def _chan(dest):
    return {"calls": CALLS_CHANNEL_ID,
            "journal": JOURNAL_CHANNEL_ID or CALLS_CHANNEL_ID,
            "explain": EXPLAIN_CHANNEL_ID or CALLS_CHANNEL_ID}.get(dest, CALLS_CHANNEL_ID)

async def _send_to(channel_id, embed_dict):
    ch = bot.get_channel(channel_id) or await bot.fetch_channel(channel_id)
    await ch.send(embed=discord.Embed.from_dict(embed_dict))

def _post_embed_dest(dest, embed_dict):
    """Called from the dashboard's background threads to post an embed to a channel."""
    asyncio.run_coroutine_threadsafe(_send_to(_chan(dest), embed_dict), bot.loop)

def status_embed():
    with k.LOCK:
        coins = dict(k.STATE.get("coins", {}))
        calls = dict(k.STATE.get("calls", {}))
        updated = k.STATE.get("updated", "")
    running = k.RUNNING.is_set()
    lines = []
    for c in ("BTC", "ETH", "SOL", "XRP"):
        r = coins.get(c)
        if not r or r.get("status") != "ok":
            lines.append(f"**{c}** — {r.get('status', 'no data') if r else 'no data'}")
        else:
            lines.append(f"**{c}** — {r['verdict']}  ·  conf {r['conf']:.0f}%  ·  edge {r['edge']:+.1f}¢")
    rec = ""
    if calls.get("n"):
        rec = (f"\n\n**Calls record:** {calls['w']}-{calls['n'] - calls['w']} "
               f"({calls['pct']}%)  ·  streak {calls['streak']}W  ·  "
               f"net ${calls['pnl']/100:+.2f} (losses capped at stop)")
    return {
        "title": f"Watcher status — {'ONLINE' if running else 'PAUSED'}",
        "color": 0x3ddc84 if running else 0xff5c5c,
        "description": "\n".join(lines) + rec,
        "footer": {"text": f"updated {updated}"},
    }

def backtest_embed(bt=None):
    if bt is None:
        bt = dict(k.STATE.get("backtest", {}))
    if bt.get("status") != "ok":
        return {"title": "Backtest", "color": 0x4a86e8,
                "description": f"Status: {bt.get('status', 'not ready')} — try again shortly."}
    rows = bt.get("rows", {}); rec = bt.get("recent", {}); rh = bt.get("recent_hours", 12)
    body = "```\n      win%     net$    vol/min\n"
    for c in ("ALL", "BTC", "ETH", "SOL", "XRP"):
        r = rows.get(c)
        if r:
            vol = f"{r['vol']:.3f}%" if r.get("vol") is not None else "   -"
            body += f"{c:4} {r['wr']:>6}%  ${r.get('net',0)/100:>+8.2f}  {vol}\n"
    body += "```"
    coins = [(c, rows[c]) for c in ("BTC", "ETH", "SOL", "XRP") if rows.get(c)]
    extra = ""
    if coins:
        worst = min(coins, key=lambda x: x[1].get("net", 0))
        mvol = max(coins, key=lambda x: x[1].get("vol") or 0)
        extra = (f"\nBiggest loser: **{worst[0]}** (${worst[1].get('net',0)/100:+.2f})  ·  "
                 f"Most volatile: **{mvol[0]}** ({(mvol[1].get('vol') or 0):.3f}%/min)")
    recent = ""
    if rec:
        recent = "\n**Recent (" + str(rh) + "h) win%:** " + "  ".join(
            f"{c} {rec[c]['wr']:.0f}%" for c in ("ALL", "BTC", "ETH", "SOL", "XRP")
            if rec.get(c) and rec[c]["n"])
    return {"title": f"Backtest — last {bt.get('days', '?')} days", "color": 0x4a86e8,
            "description": body + extra + recent}

# ─────────────────────── events + commands ───────────────────────
@bot.event
async def on_ready():
    global _synced
    k.ON_CALL = _on_call
    k.POST_EMBED = _post_embed_dest
    try:
        start_dashboard()
    except Exception as e:
        print(f"dashboard start issue (continuing): {e}")
    # Stage 5: a Discord reconnect must NOT silently resume a paused watcher.
    # Restore the intended state that was persisted, and reflect it.
    state = k.load_watcher_state()
    if not _synced:
        try:
            if GUILD_ID:
                g = discord.Object(id=GUILD_ID)
                tree.copy_global_to(guild=g)
                cmds = await tree.sync(guild=g)
            else:
                cmds = await tree.sync()
            _synced = True
            print("Synced commands: " + ", ".join(c.name for c in cmds))
        except discord.HTTPException as e:
            print(f"command sync failed: {e}")
    online = state == "running"
    await bot.change_presence(status=discord.Status.online if online else discord.Status.dnd,
                              activity=ACTIVITY_ON if online else ACTIVITY_OFF)
    await set_channel(ONLINE_NAME if online else OFFLINE_NAME)
    print(f"Bot online as {bot.user} — watcher {state}")

@tree.command(name="start", description="Resume the watcher and set the channel online")
async def start(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    k.set_watcher_state(True)
    await bot.change_presence(status=discord.Status.online, activity=ACTIVITY_ON)
    await set_channel(ONLINE_NAME)
    await interaction.followup.send("Watcher resumed — status set to online.", ephemeral=True)

@tree.command(name="stop", description="Pause the watcher and set the channel offline")
async def stop(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    k.set_watcher_state(False)
    await bot.change_presence(status=discord.Status.dnd, activity=ACTIVITY_OFF)
    await set_channel(OFFLINE_NAME)
    await interaction.followup.send("Watcher paused — status set to offline.", ephemeral=True)

@tree.command(name="status", description="Show each coin's verdict and the live W/L")
async def status(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    await interaction.followup.send(embed=discord.Embed.from_dict(status_embed()), ephemeral=True)

@tree.command(name="backtest", description="Backtest summary; add a timeframe like 2h, 10d, 2w, 3m")
@app_commands.describe(timeframe="How far back: 48h, 10d, 2w, 3m (blank = cached 30-day run)")
async def backtest(interaction: discord.Interaction, timeframe: str = None):
    await interaction.response.defer(ephemeral=True)
    if timeframe:
        res = await asyncio.to_thread(k.run_backtest, timeframe)
        emb = backtest_embed(res)
    else:
        emb = backtest_embed()
    await interaction.followup.send(embed=discord.Embed.from_dict(emb), ephemeral=True)

@tree.command(name="explain", description="Post the how-it-works explainer to its channel")
async def explain(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    await _send_to(_chan("explain"), k.build_explain_embed())
    await interaction.followup.send("Explainer posted.", ephemeral=True)

@tree.command(name="weekly", description="Post the best-day-of-week report to the journal")
async def weekly(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    emb = await asyncio.to_thread(k.build_weekly_embed)
    await _send_to(_chan("journal"), emb)
    await interaction.followup.send("Weekly report posted.", ephemeral=True)

@tree.command(name="update", description="Recent market update: 2h/6h/24h, by coin, up vs down")
async def update(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    emb = await asyncio.to_thread(k.build_update_embed)
    await interaction.followup.send(embed=discord.Embed.from_dict(emb), ephemeral=True)

def main():
    if not discord_token_usable(BOT_TOKEN):
        print("No Discord bot token: set $env:DISCORD_BOT_TOKEN (see .env.example). Discord is optional;\n"
              "to run locally without it:  py run_local.py")
        return
    bot.run(BOT_TOKEN)

if __name__ == "__main__":
    main()
