import asyncio
import click
import subprocess
import uvicorn
import importlib.resources
from queue import Queue
import sys
from pathlib import Path

from .backend.main import app


class DevServer:
    def __init__(self, resource_request_queue: Queue, resource_response_queue: Queue):
        self.dashboard_path = importlib.resources.files("nextdata") / "dashboard"
        self.backend_path = (
            importlib.resources.files("nextdata") / "dev_server" / "backend"
        )
        self.frontend_process = None
        self.backend_process = None
        self.backend_app = app
        # Store queues in FastAPI app state
        self.backend_app.state.resource_request_queue = resource_request_queue
        self.backend_app.state.resource_response_queue = resource_response_queue

    async def start_frontend(self):
        """Start the Next.js frontend server"""
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
            self.frontend_process = await asyncio.create_subprocess_exec(
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
                read_output(self.frontend_process.stdout, "Frontend"),
                read_output(self.frontend_process.stderr, "Frontend Error"),
            )

        except Exception as e:
            click.echo(f"Error starting frontend server: {str(e)}", err=True)
            raise

    async def start_backend(self):
        """Start the FastAPI backend server"""
        click.echo(f"Starting backend from: {self.backend_path}")
        # Get the parent directory of backend_path to watch the whole cli module
        watch_dir = str(self.backend_path.parent.parent)
        click.echo(f"Watching directory: {watch_dir}")

        # Start uvicorn in a separate process for proper reload support
        self.backend_process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "uvicorn",
            "nextdata.cli.dev_server.backend.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            "8000",
            "--reload",
            "--reload-dir",
            watch_dir,
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
            read_output(self.backend_process.stdout, "Backend"),
            read_output(self.backend_process.stderr, "Backend Error"),
        )

    async def start_async(self):
        """Start both frontend and backend servers"""
        try:
            # Run both servers concurrently
            await asyncio.gather(self.start_frontend(), self.start_backend())
        except Exception as e:
            click.echo(f"Error starting development servers: {str(e)}", err=True)
            await self.stop_async()
            raise

    async def stop_async(self):
        """Stop both frontend and backend servers"""
        click.echo("Stopping development servers...")
        try:
            # Stop frontend
            if self.frontend_process:
                click.echo("Stopping frontend server...")
                self.frontend_process.terminate()
                try:
                    await asyncio.wait_for(self.frontend_process.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    click.echo("Frontend server didn't stop gracefully, forcing...")
                    self.frontend_process.kill()
                self.frontend_process = None

            # Stop backend
            if self.backend_process:
                click.echo("Stopping backend server...")
                self.backend_process.terminate()
                try:
                    await asyncio.wait_for(self.backend_process.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    click.echo("Backend server didn't stop gracefully, forcing...")
                    self.backend_process.kill()
                self.backend_process = None

        except Exception as e:
            click.echo(f"Error during server shutdown: {str(e)}", err=True)
            # Still try to clean up
            if self.frontend_process:
                self.frontend_process.kill()
                self.frontend_process = None
            if self.backend_process:
                self.backend_process.kill()
                self.backend_process = None
