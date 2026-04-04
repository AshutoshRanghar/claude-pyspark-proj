"""Setup configuration for claude-pyspark-proj package."""

from setuptools import setup, find_packages

setup(
    name="claude-pyspark-proj",
    version="0.1.0",
    description="PySpark streaming pipeline with Azure Event Hubs integration",
    author="Ashutosh Ranghar",
    python_requires=">=3.10",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pyspark>=3.4.0",
        "httpx>=0.25.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
        ],
    },
)
