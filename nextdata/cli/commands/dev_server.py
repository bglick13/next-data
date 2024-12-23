import asyncio
import click

from nextdata.cli.commands import NDX_SINGLETON


@click.group()
def dev_server():
    """Dev server commands"""
    pass


@dev_server.command(name="start")
def start():
    """Start the dev server"""
    # Create event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(NDX_SINGLETON.dev_server.start_async())
    finally:
        loop.close()
