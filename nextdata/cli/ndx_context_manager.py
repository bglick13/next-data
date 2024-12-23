import time
import queue
import threading
import asyncio
import click
import webbrowser
import signal
from watchdog.observers import Observer

from .data_directory_handler import DataDirectoryHandler
from .pulumi_context_manager import PulumiContextManager
from .dev_server import DevServer


class NdxContextManager:
    def __init__(self):
        self.pulumi_context_manager = PulumiContextManager()
        self.event_queue = queue.Queue()
        self.resource_request_queue = queue.Queue()
        self.resource_response_queue = queue.Queue()
        self.should_stop = threading.Event()
        self.observer = None
        self.watcher_thread = None
        # Initialize dev server with queues for resource requests
        self.dev_server = DevServer(
            resource_request_queue=self.resource_request_queue,
            resource_response_queue=self.resource_response_queue,
        )

    def handle_shutdown(self, signum=None, frame=None):
        """Handle shutdown gracefully"""
        click.echo("\n🛑 Shutting down...")
        self.should_stop.set()

    def start(self, skip_init: bool = False):
        """Start the development environment"""
        try:
            # Set up signal handlers
            signal.signal(signal.SIGINT, self.handle_shutdown)
            signal.signal(signal.SIGTERM, self.handle_shutdown)

            # Initialize Pulumi stack in the main thread
            if not skip_init:
                click.echo("Initializing Pulumi stack...")
                self.pulumi_context_manager.initialize_stack()
                self.pulumi_context_manager.create_stack()

            # Start file watcher in a separate thread
            click.echo("Starting file watcher...")
            self.watcher_thread = threading.Thread(
                target=self._run_file_watcher, daemon=True
            )
            self.watcher_thread.start()

            # Create event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            # Start both the dev server and event processor
            click.echo("Starting development environment...")
            try:
                # Run both the dev server and event processor
                loop.run_until_complete(
                    asyncio.gather(
                        self._run_dev_server(),
                        self._run_event_processor(),
                        self._run_resource_processor(),
                    )
                )
            except asyncio.CancelledError:
                click.echo("Shutting down servers...")
            finally:
                # Clean shutdown
                click.echo("Cleaning up resources...")
                loop.run_until_complete(self._cleanup())
                loop.close()
                self._cleanup_threads()

        except KeyboardInterrupt:
            self.handle_shutdown()
        except Exception as e:
            click.echo(f"Error: {str(e)}", err=True)
            self.handle_shutdown()
            raise

    async def _cleanup(self):
        """Clean up async resources"""
        try:
            await self.dev_server.stop_async()
        except Exception as e:
            click.echo(f"Error during cleanup: {str(e)}", err=True)

    def _cleanup_threads(self):
        """Clean up thread resources"""
        if self.observer:
            self.observer.stop()
            self.observer.join()
        if self.watcher_thread and self.watcher_thread.is_alive():
            self.watcher_thread.join(timeout=5)

    def _run_file_watcher(self):
        """Run the file watcher in a separate thread"""
        data_dir = self.pulumi_context_manager.config.data_dir
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

    async def _run_dev_server(self):
        """Run the development server asynchronously"""
        click.echo("Starting development server...")
        try:
            await self.dev_server.start_async()
        except Exception as e:
            click.echo(f"Error in dev server: {str(e)}", err=True)
            self.handle_shutdown()
            raise

    async def _run_event_processor(self):
        """Process file system events asynchronously"""
        click.echo("Processing file system events...")
        while not self.should_stop.is_set():
            try:
                # Non-blocking queue check
                try:
                    event = self.event_queue.get_nowait()
                    if event["type"] == "create_table":
                        click.echo(f"Creating table for: {event['path']}")
                        self.pulumi_context_manager.create_table(event["path"])
                except queue.Empty:
                    pass

                # Sleep briefly to prevent CPU spinning
                await asyncio.sleep(0.1)
            except Exception as e:
                click.echo(f"Error processing event: {str(e)}", err=True)
                await asyncio.sleep(1)  # Wait longer on error

    async def _run_resource_processor(self):
        """Process resource requests from the dev server"""
        click.echo("Starting resource processor...")
        while not self.should_stop.is_set():
            try:
                # Non-blocking queue check
                try:
                    request = self.resource_request_queue.get_nowait()
                    click.echo(f"Got resource request: {request}")
                    if request["type"] == "get_table_bucket":
                        # Get the table bucket from Pulumi context
                        table_bucket = self.pulumi_context_manager.table_bucket
                        click.echo(f"Got table bucket: {table_bucket}")
                        response = {
                            "id": request["id"],
                            "resource": {
                                "arn": table_bucket.arn.get(),
                                "name": table_bucket.name.get(),
                            },
                        }
                        click.echo(f"Sending response: {response}")
                        self.resource_response_queue.put(response)
                except queue.Empty:
                    pass

                # Sleep briefly to prevent CPU spinning
                await asyncio.sleep(0.1)
            except Exception as e:
                click.echo(f"Error processing resource request: {str(e)}", err=True)
                await asyncio.sleep(1)  # Wait longer on error
