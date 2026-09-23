#!/usr/bin/env python3
"""
Channel rename diagnostic. Run:  py check_channel.py
It logs in, inspects the status channel, and prints exactly what's wrong
(or renames it if everything is fine). Uses the token/IDs from kalshi_bot.py.
"""
import asyncio
import discord
import kalshi_bot as b

async def run():
    client = discord.Client(intents=discord.Intents.default())

    @client.event
    async def on_ready():
        print(f"Logged in as {client.user}")
        try:
            ch = client.get_channel(b.STATUS_CHANNEL_ID)
            if ch is None:
                ch = await client.fetch_channel(b.STATUS_CHANNEL_ID)
            print(f"Channel found: '{ch.name}'  (type: {type(ch).__name__})")
            me = ch.guild.me
            perms = ch.permissions_for(me)
            print(f"Bot can View Channel   : {perms.view_channel}")
            print(f"Bot can Manage Channels: {perms.manage_channels}")
            if not perms.manage_channels:
                print(">>> FIX: give the bot 'Manage Channels' on this channel "
                      "(channel settings -> Permissions -> add the bot's role).")
            else:
                try:
                    await ch.edit(name=b.ONLINE_NAME)
                    print(f">>> RENAME OK — channel is now '{b.ONLINE_NAME}'. Wiring is fine.")
                except discord.HTTPException as e:
                    print(f">>> RENAME ERROR (often the ~2-per-10-min rate limit): {e}")
        except discord.NotFound:
            print(">>> CHANNEL NOT FOUND: the ID is wrong, or the bot isn't in that server.")
        except discord.Forbidden:
            print(">>> CANNOT ACCESS CHANNEL: the bot lacks 'View Channel' here.")
        except Exception as e:
            print(f">>> error: {type(e).__name__}: {e}")
        await client.close()

    await client.start(b.BOT_TOKEN)

asyncio.run(run())
