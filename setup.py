from setuptools import setup, find_packages

setup(
    name="nextdata",
    version="0.1.10",
    packages=find_packages(
        exclude=[
            "*.node_modules",
            "*.node_modules.*",
            "*.next",
            "*.next.*",
            "*.pnpm-lock.yaml",
        ]
    ),
    include_package_data=True,
    install_requires=[
        "click>=8.0.0",
        "asyncclick>=8.0.0",
        "watchdog>=2.1.0",  # For file watching
        "fastapi>=0.68.0",  # For web server
        "uvicorn>=0.15.0",  # ASGI server
        "cookiecutter>=2.1.0",  # For project templates,
        "boto3>=1.26.0",  # For AWS SDK
        "pulumi>=3.0.0",  # For infrastructure management,
        "pulumi-aws>=6.66.0",  # For AWS infrastructure management,
        "python-dotenv>=1.0.0",  # For environment variables,
        "pyspark>=3.5.4",  # For Spark,
        "python-multipart>=0.0.20",  # For multipart/form-data parsing
    ],
    entry_points={
        "console_scripts": [
            "ndx=nextdata.cli.commands.main:cli",
        ],
    },
    package_data={
        "nextdata": [
            "templates/**/*",
            "templates/**/.*",  # Include hidden files
        ],
    },
    # Make sure package data is included in the wheel
    zip_safe=False,
    author="Benjamin Glickenhaus",
    author_email="benglickenhaus@gmail.com",
    description="NextData is a framework for building data pipelines with a focus on simplicity and scalability.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/bglick13/next-data",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
    ],
    python_requires=">=3.9",
)
