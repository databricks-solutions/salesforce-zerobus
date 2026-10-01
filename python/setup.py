"""
Setup script for Salesforce PubSub Spark Data Source
Creates a wheel package for Databricks cluster installation.
"""

from setuptools import setup, find_packages
import os

# Read requirements from file
def read_requirements():
    req_file = os.path.join(os.path.dirname(__file__), "spark_datasource", "requirements.txt")
    if os.path.exists(req_file):
        with open(req_file, 'r') as f:
            return [line.strip() for line in f if line.strip() and not line.startswith('#')]
    return []

# Read README for long description
def read_readme():
    readme_file = os.path.join(os.path.dirname(__file__), "spark_datasource", "README.md")
    if os.path.exists(readme_file):
        with open(readme_file, 'r', encoding='utf-8') as f:
            return f.read()
    return ""

setup(
    name="spark-datasource",
    version="1.0.0",
    description="Spark custom data source for Salesforce PubSub API",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    author="Salesforce",
    author_email="pubsub-api-support@salesforce.com",
    url="https://github.com/salesforce/pub-sub-api",
    
    # Package configuration
    packages=["spark_datasource", "spark_datasource.proto", "spark_datasource.util"],
    package_dir={"spark_datasource": "spark_datasource", "spark_datasource.proto": "spark_datasource/proto", "spark_datasource.util": "spark_datasource/util"},
    py_modules=[],
    
    # Dependencies
    install_requires=read_requirements() or [
        "grpcio>=1.50.0",
        "grpcio-tools>=1.50.0", 
        "protobuf>=4.21.0",
        "certifi>=2022.0.0",
        "avro-python3>=1.10.0",
        "requests>=2.28.0",
        "pyspark>=3.4.0",
        "bitstring>=4.0.0"
    ],
    
    # Package data
    package_data={
        "": ["*.py"],
        "CustomDataSource": ["*.py"]
    },
    include_package_data=True,
    
    # Python version requirement
    python_requires=">=3.8",
    
    # Entry points for easy access
    entry_points={
        "console_scripts": [
            "salesforce-pubsub-register=spark_datasource:register_data_source",
        ],
    },
    
    # Wheel configuration
    zip_safe=False,  # Important for Spark compatibility
    
    # Classifiers
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database",
        "Framework :: Apache Spark",
    ],
    
    # Keywords for PyPI
    keywords="salesforce pubsub spark streaming databricks",
    
    # Project URLs
    project_urls={
        "Bug Reports": "https://github.com/salesforce/pub-sub-api/issues",
        "Source": "https://github.com/salesforce/pub-sub-api",
        "Documentation": "https://developer.salesforce.com/docs/platform/pub-sub-api/overview",
    },
) 