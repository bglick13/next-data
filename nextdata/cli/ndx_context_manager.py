import time
import queue
import threading
import asyncio
import click
import webbrowser
from pathlib import Path
from watchdog.observers import Observer

from .data_directory_handler import DataDirectoryHandler
from .pulumi_context_manager import PulumiContextManager
from .dev_server import DevServer


class NdxContextManager:
    def __init__(self):
        self.pulumi_context_manager = PulumiContextManager()
        self.dev_server = DevServer()
        self.event_queue = queue.Queue()
        self.should_stop = threading.Event()

    def start(self):
        """Start the development environment"""
        try:
            # Initialize Pulumi stack in the main thread
            click.echo("Initializing Pulumi stack...")
            self.pulumi_context_manager.initialize_stack()
            self.pulumi_context_manager.create_stack()

            # Start file watcher in a separate thread
            click.echo("Starting file watcher...")
            watcher_thread = threading.Thread(
                target=self._run_file_watcher, daemon=True
            )
            watcher_thread.start()

            # Create event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            # Start both the dev server and event processor
            click.echo("Starting development environment...")
            try:
                # Open browser
                webbrowser.open("http://localhost:3000")

                # Run both the dev server and event processor
                loop.run_until_complete(
                    asyncio.gather(self._run_dev_server(), self._run_event_processor())
                )
            finally:
                loop.close()

        except KeyboardInterrupt:
            click.echo("\nShutting down...")
            self.should_stop.set()
        except Exception as e:
            click.echo(f"Error: {str(e)}", err=True)
            self.should_stop.set()
            raise

    def _run_file_watcher(self):
        """Run the file watcher in a separate thread"""
        data_dir = self.pulumi_context_manager.config.data_dir
        if not data_dir.exists():
            data_dir.mkdir(parents=True)
            click.echo(f"📁 Created data directory: {data_dir}")

        event_handler = DataDirectoryHandler(self.event_queue)
        observer = Observer()
        observer.schedule(event_handler, str(data_dir), recursive=True)
        observer.start()
        click.echo(f"👀 Watching for changes in {data_dir}")

        try:
            while not self.should_stop.is_set():
                time.sleep(1)
        finally:
            observer.stop()
            observer.join()

    async def _run_dev_server(self):
        """Run the development server asynchronously"""
        click.echo("Starting development server...")
        await self.dev_server.start_async()

    async def _run_event_processor(self):
        """Process events asynchronously"""
        click.echo("Processing events...")
        while not self.should_stop.is_set():
            try:
                # Non-blocking queue check
                try:
                    event = self.event_queue.get_nowait()
                    if event["type"] == "create_table":
                        click.echo(f"Creating table for: {event['path']}")
                        self.pulumi_context_manager.create_table(event["path"])
                    # Add more event types as needed
                except queue.Empty:
                    pass

                # Sleep briefly to prevent CPU spinning
                await asyncio.sleep(0.1)
            except Exception as e:
                click.echo(f"Error processing event: {str(e)}", err=True)
                await asyncio.sleep(1)  # Wait longer on error
