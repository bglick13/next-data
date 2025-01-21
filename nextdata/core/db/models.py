from typing import Optional
from sqlalchemy import Boolean, Column, Enum, Integer, String, ForeignKey, JSON
from sqlalchemy.orm import DeclarativeBase, mapped_column, relationship, Mapped
import enum


class HumanReadableName(enum.Enum):
    EMR_APP = "EMR App"
    GLUE_JOB_BUCKET = "Glue Job Bucket"
    S3_TABLE_BUCKET = "S3 Table Bucket"
    S3_TABLE_NAMESPACE = "S3 Table Namespace"
    GLUE_ROLE = "Glue Role"
    EMR_ROLE = "EMR Role"
    EMR_CLUSTER = "EMR Cluster"
    EMR_STUDIO = "EMR Studio"


class JobType(enum.Enum):
    ETL = "etl"
    RETL = "retl"


class ConnectionType(enum.Enum):
    REDSHIFT = "redshift"
    POSTGRES = "postgres"
    MYSQL = "mysql"
    MSSQL = "mssql"
    ORACLE = "oracle"
    DSQL = "dsql"


class Base(DeclarativeBase):
    pass


class EmrJobInputTable(Base):
    __tablename__ = "emr_job_input_tables"
    job_id: Mapped[int] = mapped_column(ForeignKey("emr_jobs.id"), primary_key=True)
    table_id: Mapped[int] = mapped_column(
        ForeignKey("s3_data_tables.id"), primary_key=True
    )


class EmrJobOutputTable(Base):
    __tablename__ = "emr_job_output_tables"
    job_id: Mapped[int] = mapped_column(ForeignKey("emr_jobs.id"), primary_key=True)
    table_id: Mapped[int] = mapped_column(
        ForeignKey("s3_data_tables.id"), primary_key=True
    )


class S3DataTable(Base):
    __tablename__ = "s3_data_tables"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True)
    schema: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    downstream_jobs: Mapped[list["EmrJob"]] = relationship(
        secondary="emr_job_input_tables", back_populates="input_tables"
    )
    upstream_jobs: Mapped[list["EmrJob"]] = relationship(
        secondary="emr_job_output_tables", back_populates="output_tables"
    )


class EmrJob(Base):
    __tablename__ = "emr_jobs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True)
    job_type: Mapped[JobType] = mapped_column(Enum(JobType))
    connection_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    connection_type: Mapped[Optional[ConnectionType]] = mapped_column(
        Enum(ConnectionType), nullable=True
    )
    connection_properties: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    sql_table: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    incremental_column: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    is_full_load: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)

    script_id: Mapped[int] = mapped_column(ForeignKey("emr_job_scripts.id"))
    script: Mapped["EmrJobScript"] = relationship(back_populates="jobs")
    requirements: Mapped[str] = mapped_column(String, nullable=True)
    venv_s3_path: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    input_tables: Mapped[list[S3DataTable]] = relationship(
        secondary="emr_job_input_tables", back_populates="downstream_jobs"
    )
    output_tables: Mapped[list[S3DataTable]] = relationship(
        secondary="emr_job_output_tables", back_populates="upstream_jobs"
    )


class EmrJobScript(Base):
    __tablename__ = "emr_job_scripts"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True)
    s3_path: Mapped[str] = mapped_column(String)
    bucket: Mapped[str] = mapped_column(String)
    jobs: Mapped[list["EmrJob"]] = relationship(back_populates="script")


class AwsResource(Base):
    __tablename__ = "aws_resources"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True)
    human_readable_name: Mapped[HumanReadableName] = mapped_column(
        Enum(HumanReadableName)
    )
    resource_type: Mapped[str] = mapped_column(String)
    resource_id: Mapped[str] = mapped_column(String)
    resource_arn: Mapped[str] = mapped_column(String)
