# What is NextData?

NextData answers the question: "What would a nextjs for data worfklows look like?"

It is a framework that provides convention over configuration for data workflows.

A "data directory" is the single source of truth for your data. It is a directory that contains all of your data, and all of your code that operates on that data.

# How to use NextData

## Installing NextData

```bash
pip install nextdata
```

This will install the NextData framework and the `ndx` command line tool.

## Creating a new NextData project

```bash
ndx create-ndx-app my_project
```

### Configuring your project

NextData uses Pulumi under the hood to manage your infrastructure. Make sure you have pulumi installed and configured. You'll need AWS credentials in your environment, but NextData will confiure an IAM role specifically for Pulumi to use that only has access to the resources you need.

### Running your project

```bash
ndx dev
```

The dev server starts 4 processes:

1. A Pulumi stack that manages your infrastructure
2. A watch dog that watches your data directory and reruns your pulumi stack when you make changes
3. A FastAPI server that serves the NextData API
4. A NextJS server that serves the NextData Dashboard

In the future, this can be dockerized so you can self host just the components you need.

### Adding data to your project

Much like the app router in NextJS, NextData uses the `data directory` to represent your data. When you hadd a directory to your data directory, NextData will automatically generate an S3 table for you.

NextData also checks for certain magic files in each data table directory to determine how to process the data.

For example, adding an `etl.py` file will tell NextData to configure a Glue job to process the data. You only need to provide a connection name, and NextData will use the connection to read and write data to S3 using sensible defaults. Of course, you can customize the ETL script if you want using the `@glue_job` decorator.

Speaking of connections, NextData uses the `connections` directory to represent your data sources. Each connection is a set of credentials and configuration for a data source. NextData currently supports arbitrary connections through JDBC and DSQL, but in the future will support more data sources like Snowflake, BigQuery, etc.

## NextData Dashboard

The NextData Dashboard is a NextJS app that provides a UI for you to explore your data. It is powered by the NextData API, which is a FastAPI server that you can use to build your own custom APIs.

The Dashboard is where you can build your own queries, visualizations, and data products.
