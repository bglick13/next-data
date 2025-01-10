import asyncio
import queue
import threading
import time
import click
import subprocess
import importlib.resources
import sys
from watchdog.observers import Observer
from nextdata.cli.data_directory_handler import DataDirectoryHandler
from nextdata.core.project_config import NextDataConfig

from .backend.main import app


class DevServer:
    def __init__(self):
        self.config = NextDataConfig.from_env()
        self.dashboard_path = importlib.resources.files("nextdata") / "dashboard"
        self.backend_path = (
            importlib.resources.files("nextdata") / "dev_server" / "backend"
        )
        self.event_queue = queue.Queue()
        self.should_stop = threading.Event()
        self.observer = None
        self.watcher_thread = None
        self.frontend_process = None
        self.backend_process = None
        self.backend_app = app

    def _run_file_watcher(self):
        """Run the file watcher in a separate thread"""
        data_dir = self.config.data_dir
        if not data_dir.exists():
            data_dir.mkdir(parents=True)
            click.echo(f"📁 Created data directory: {data_dir}")

        event_handler = DataDirectoryHandler(self.event_queue)
        self.observer = Observer()
        self.observer.schedule(event_handler, str(data_dir), recursive=True)
        self.observer.start()
        click.echo(f"👀 Watching for changes in {data_dir}")

        try:
            while not self.should_stop.is_set():
                time.sleep(1)
        finally:
            if self.observer:
                self.observer.stop()
                self.observer.join()

    def _cleanup_threads(self):
        """Clean up thread resources"""
        if self.observer:
            self.observer.stop()
            self.observer.join()
        if self.watcher_thread and self.watcher_thread.is_alive():
            self.watcher_thread.join(timeout=5)

    async def start_frontend(self, dashboard_port: int):
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
            click.echo(
                f"Starting Next.js development server on port {dashboard_port}..."
            )
            self.frontend_process = await asyncio.create_subprocess_exec(
                "pnpm",
                "run",
                "dev",
                "--port",
                str(dashboard_port),
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

    async def start_backend(self, api_port: int):
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
            str(api_port),
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
            read_output(self.backend_process.stderr, "Backend"),
        )

    async def start_async(self, skip_init: bool, dashboard_port: int, api_port: int):
        """Start both frontend and backend servers"""
        try:
            # Run both servers concurrently
            self.watcher_thread = threading.Thread(
                target=self._run_file_watcher, daemon=True
            )
            self.watcher_thread.start()
            await asyncio.gather(
                self.start_frontend(dashboard_port), self.start_backend(api_port)
            )
        except Exception as e:
            click.echo(f"Error starting development servers: {str(e)}", err=True)
            await self.stop_async()
            raise

    async def stop_async(self):
        """Stop both frontend and backend servers"""
        click.echo("Stopping development servers...")
        try:
            self.should_stop.set()
            self._cleanup_threads()
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
