from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, joinedload

from nextdata.core.db.models import (
    AwsResource,
    Base,
    EmrJob,
    EmrJobScript,
    HumanReadableName,
    S3DataTable,
)

if TYPE_CHECKING:
    from pathlib import Path


class DatabaseManager:
    def __init__(self, db_path: Path) -> None:
        uri = f"sqlite:///{db_path.resolve()}"
        self.engine = create_engine(uri)

    def create_all(self) -> None:
        Base.metadata.create_all(self.engine)

    def reset(self) -> None:
        Base.metadata.drop_all(self.engine)
        self.create_all()

    def add_table(self, table: S3DataTable) -> None:
        with Session(self.engine) as session:
            session.add(table)
            session.commit()

    def add_job(
        self,
        job: EmrJob,
        input_tables: list[S3DataTable] | None = None,
        output_tables: list[S3DataTable] | None = None,
    ) -> None:
        with Session(self.engine) as session:
            session.add(job)
            if input_tables:
                job.input_tables.extend(input_tables)
            if output_tables:
                job.output_tables.extend(output_tables)
            session.commit()

    def add_resource(self, resource: AwsResource) -> None:
        with Session(self.engine) as session:
            session.add(resource)
            session.commit()

    def add_script(self, script: EmrJobScript) -> None:
        with Session(self.engine) as session:
            session.add(script)
            session.commit()

    def get_script_by_name(self, script_name: str) -> EmrJobScript | None:
        with Session(self.engine) as session:
            return session.query(EmrJobScript).filter(EmrJobScript.name == script_name).first()

    def get_table_by_name(self, table_name: str) -> S3DataTable | None:
        with Session(self.engine) as session:
            # Use joinedload to eagerly load the relationships
            table = (
                session.query(S3DataTable)
                .filter(S3DataTable.name == table_name)
                .options(
                    joinedload(S3DataTable.upstream_jobs).joinedload(EmrJob.script),
                    joinedload(S3DataTable.downstream_jobs).joinedload(EmrJob.script),
                )
                .first()
            )
            if table:
                # Ensure the relationships are loaded into the session
                session.refresh(table)
                # Explicitly load the relationships to ensure they're populated
                session.query(EmrJob).filter(
                    EmrJob.id.in_([job.id for job in table.upstream_jobs]),
                ).all()
                session.query(EmrJob).filter(
                    EmrJob.id.in_([job.id for job in table.downstream_jobs]),
                ).all()
        return table

    def get_job(self, job_name: str) -> EmrJob | None:
        with Session(self.engine) as session:
            job = (
                session.query(EmrJob)
                .filter(EmrJob.name == job_name)
                .options(
                    joinedload(EmrJob.script),
                    joinedload(EmrJob.input_tables),
                    joinedload(EmrJob.output_tables),
                )
                .first()
            )
            if job:
                # Ensure all relationships are loaded before leaving session
                session.refresh(job)
                # Keep the object attached to the session
                session.expunge_all()
            return job

    def get_resource_by_name(self, resource_name: HumanReadableName) -> AwsResource | None:
        with Session(self.engine) as session:
            return (
                session.query(AwsResource)
                .filter(AwsResource.human_readable_name == resource_name.name)
                .first()
            )

    def get_resource_by_id(self, resource_id: str) -> AwsResource | None:
        with Session(self.engine) as session:
            return session.query(AwsResource).filter(AwsResource.resource_id == resource_id).first()

    def get_resource_by_arn(self, resource_arn: str) -> AwsResource | None:
        with Session(self.engine) as session:
            return (
                session.query(AwsResource).filter(AwsResource.resource_arn == resource_arn).first()
            )

    def get_resources_by_type(self, resource_type: str) -> list[AwsResource]:
        with Session(self.engine) as session:
            return (
                session.query(AwsResource).filter(AwsResource.resource_type == resource_type).all()
            )
