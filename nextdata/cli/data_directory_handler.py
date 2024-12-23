from pathlib import Path
from watchdog.events import FileSystemEventHandler
import click
from queue import Queue


class DataDirectoryHandler(FileSystemEventHandler):
    def __init__(self, event_queue: Queue):
        super().__init__()
        self.event_queue = event_queue

    def on_created(self, event):
        if event.is_directory:
            try:
                event_path = Path(event.src_path)
                # Get the parent directory to check if this is a top-level data directory
                if event_path.parent.name == "data":
                    click.echo(f"📁 New data directory created: {event_path.name}")
                    # Queue the event for processing in the main thread
                    self.event_queue.put(
                        {"type": "create_table", "path": event.src_path}
                    )
            except Exception as e:
                click.echo(f"❌ Error queueing table creation: {str(e)}", err=True)

    def on_modified(self, event):
        if event.is_directory:
            event_path = Path(event.src_path)
            if event_path.parent.name == "data":
                click.echo(f"📝 Data directory modified: {event_path.name}")
                # TODO: Queue sync events for processing in main thread
