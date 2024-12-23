import asyncio
import click
import subprocess
from pathlib import Path
import importlib.resources


class DevServer:
    def __init__(self):
        self.dashboard_path = importlib.resources.files("nextdata") / "dashboard"
        self.process = None

    async def start_async(self):
        """Start the development server asynchronously"""
        try:
            click.echo(f"Starting dashboard from: {self.dashboard_path}")

            # Check if pnpm is installed
            if subprocess.run(["which", "pnpm"], capture_output=True).returncode != 0:
                click.echo("Installing pnpm...")
                await asyncio.create_subprocess_exec("npm", "install", "-g", "pnpm")

            # Install dependencies if needed
            if not (self.dashboard_path / "node_modules").exists():
                click.echo("Installing dashboard dependencies...")
                proc = await asyncio.create_subprocess_exec(
                    "pnpm", "install", cwd=self.dashboard_path
                )
                await proc.wait()

            # Start the dev server
            click.echo("Starting Next.js development server...")
            self.process = await asyncio.create_subprocess_exec(
                "pnpm",
                "run",
                "dev",
                cwd=self.dashboard_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            # Monitor the process output
            async def read_output(stream, prefix):
                while True:
                    line = await stream.readline()
                    if not line:
                        break
                    click.echo(f"{prefix}: {line.decode().strip()}")

            # Create tasks for reading stdout and stderr
            await asyncio.gather(
                read_output(self.process.stdout, "Dashboard"),
                read_output(self.process.stderr, "Dashboard Error"),
            )

        except Exception as e:
            click.echo(f"Error starting development server: {str(e)}", err=True)
            raise

    async def stop_async(self):
        """Stop the development server"""
        if self.process:
            self.process.terminate()
            await self.process.wait()
            self.process = None
