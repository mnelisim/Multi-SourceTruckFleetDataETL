from setuptools import setup, find_packages

setup(
    name='multi_sourcefleetdataetl',
    version='0.1.0',
    packages=find_packages(),  # finds ingestion, storage, processor
    install_requires=[
        'kafka-python',
        'google-cloud-bigquery',
        'pyspark',
        'apache-airflow',
        # add any other libs your DAG needs
    ],
)

