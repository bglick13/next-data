import asyncio
import asyncclick as click

from nextdata.cli.dashboard_installer import DashboardInstaller
from nextdata.cli.dev_server.main import DevServer


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
        dev_server = DevServer()
        loop.run_until_complete(dev_server.start_async())
    finally:
        loop.close()


@dev_server.command(name="setup")
def setup():
    """Setup the dev server"""
    dashboard_installer = DashboardInstaller()
    dashboard_installer.install()
