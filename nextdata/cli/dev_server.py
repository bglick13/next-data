import asyncio
import queue
import uuid
from typing import Annotated, Dict
import click
import subprocess
import uvicorn
from pathlib import Path
import importlib.resources
from queue import Queue
from fastapi import Depends, FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pyspark.sql import SparkSession
import time

from nextdata.cli.project_config import NextDataConfig


class DevServer:
    def __init__(self, resource_request_queue: Queue, resource_response_queue: Queue):
        self.config = NextDataConfig.from_env()
        self.dashboard_path = importlib.resources.files("nextdata") / "dashboard"
        self.frontend_process = None
        self.backend_app = FastAPI()
        self.resource_request_queue = resource_request_queue
        self.resource_response_queue = resource_response_queue
        self.setup_backend()

    def get_table_bucket(self) -> Dict:
        """Get table bucket info from the main thread"""
        request_id = str(uuid.uuid4())
        request = {"type": "get_table_bucket", "id": request_id}
        click.echo(f"Requesting table bucket info: {request}")
        self.resource_request_queue.put(request)

        # Wait for response with matching ID
        start_time = time.time()
        timeout = 30  # Total timeout in seconds
        retry_interval = 5  # Time between retries in seconds

        while time.time() - start_time < timeout:
            try:
                click.echo("Waiting for table bucket response...")
                response = self.resource_response_queue.get(timeout=retry_interval)
                click.echo(f"Got response: {response}")
                if response["id"] == request_id:
                    click.echo("Found matching response!")
                    return response["resource"]
                else:
                    click.echo(
                        f"Response ID {response['id']} didn't match request ID {request_id}"
                    )
            except queue.Empty:
                click.echo("No response received, retrying...")
                continue

        raise Exception(
            f"Timeout waiting for table bucket info after {timeout} seconds"
        )

    def pyspark_connection_dependency(self):
        """Get PySpark connection with S3 Tables configuration"""
        try:
            # Get bucket info from main thread
            bucket_info = self.get_table_bucket()
            click.echo(f"Connecting to S3 Tables bucket: {bucket_info['arn']}")

            spark = (
                SparkSession.builder.appName("NextData")
                .config(
                    "spark.sql.catalog.s3tablesbucket",
                    "org.apache.iceberg.spark.SparkCatalog",
                )
                .config(
                    "spark.sql.catalog.s3tablesbucket.catalog-impl",
                    "software.amazon.s3tables.iceberg.S3TablesCatalog",
                )
                .config(
                    "spark.sql.catalog.s3tablesbucket.warehouse", bucket_info["arn"]
                )
                .config(
                    "spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                )
                .config(
                    "spark.jars.packages",
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
                    "software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.3",
                )
                .getOrCreate()
            )
            return spark
        except Exception as e:
            click.echo(f"Error creating Spark session: {str(e)}", err=True)
            raise

    def setup_backend(self):
        """Setup FastAPI routes and middleware"""
        # Add CORS middleware
        self.backend_app.add_middleware(
            CORSMiddleware,
            allow_origins=["http://localhost:3000"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        @self.backend_app.get("/api/health")
        async def health_check(
            spark: Annotated[SparkSession, Depends(self.pyspark_connection_dependency)]
        ):
            try:
                connection_check = spark.sql("SELECT 1").collect()
                return {
                    "status": "healthy",
                    "connection_check": connection_check,
                    "pulumi_stack": self.config.stack_name,
                }
            except Exception as e:
                return {
                    "status": "error",
                    "error": str(e),
                    "pulumi_stack": self.config.stack_name,
                }

        @self.backend_app.get("/api/data_directories")
        async def list_data_directories():
            data_dir = Path.cwd() / "data"
            if not data_dir.exists():
                return {"directories": []}

            directories = [
                {
                    "name": d.name,
                    "path": str(d.relative_to(data_dir)),
                    "type": "directory" if d.is_dir() else "file",
                }
                for d in data_dir.iterdir()
                if d.is_dir()
            ]
            return {"directories": directories}

        @self.backend_app.post("/api/upload_csv")
        async def upload_csv(
            spark: Annotated[SparkSession, Depends(self.pyspark_connection_dependency)],
            file: UploadFile = File(...),
        ):
            try:
                # TODO: Implement CSV upload logic
                return {"status": "success", "filename": file.filename}
            except Exception as e:
                return {"status": "error", "error": str(e)}

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
        config = uvicorn.Config(
            app=self.backend_app, host="127.0.0.1", port=8000, log_level="info"
        )
        server = uvicorn.Server(config)

        # Modify uvicorn server to work with asyncio
        server.config.setup_event_loop = lambda: None
        await server.serve()

    async def start_async(self):
        """Start both frontend and backend servers"""
        try:
            # Run both servers concurrently
            await asyncio.gather(self.start_frontend(), self.start_backend())
        except Exception as e:
            click.echo(f"Error starting development servers: {str(e)}", err=True)
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

            # Clear queues
            while not self.resource_request_queue.empty():
                try:
                    self.resource_request_queue.get_nowait()
                except queue.Empty:
                    break

            while not self.resource_response_queue.empty():
                try:
                    self.resource_response_queue.get_nowait()
                except queue.Empty:
                    break

        except Exception as e:
            click.echo(f"Error during server shutdown: {str(e)}", err=True)
            # Still try to clean up
            if self.frontend_process:
                self.frontend_process.kill()
                self.frontend_process = None
