from setuptools import setup, find_packages
from pathlib import Path

# Read the contents of README file
# readme = Path("README.md").read_text()

setup(
    name="nextdata",
    version="0.1.0",
    packages=find_packages(include=["nextdata", "nextdata.*"]),
    include_package_data=True,
    install_requires=[
        "click>=8.0.0",
        "watchdog>=2.1.0",  # For file watching
        "fastapi>=0.68.0",  # For web server
        "uvicorn>=0.15.0",  # ASGI server
        "cookiecutter>=2.1.0",  # For project templates
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
            "static/**/*",
        ],
    },
    # Make sure package data is included in the wheel
    zip_safe=False,
)
