from setuptools import setup, find_packages

setup(
    name='etl_pipeline',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'pyspark',
        "pytest",
        "pandas",
        'jsonschema',
        # Add other dependencies
    ],
)
