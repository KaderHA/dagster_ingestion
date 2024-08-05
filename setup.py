from setuptools import find_packages, setup

setup(
    name="ingestion",
    packages=find_packages(exclude=["ingestion_tests"]),
    install_requires=[
        "dagster==1.7.15",
        "dagster-databricks==0.23.15",
        "dagster-cloud==1.7.15",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
