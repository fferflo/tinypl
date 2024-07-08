#!/usr/bin/env python3

from setuptools import setup

with open("README.md") as f:
    long_description = f.read()

setup(
    name="tinypl",
    version="0.1.0",
    description="Pipelines for asynchronous processing",
    author="Florian Fervers",
    author_email="florian.fervers@gmail.com",
    packages=["tinypl"],
    install_requires=[
        "pytest",
        "threadpoolctl",
    ],
    license="MIT",
    url="https://github.com/fferflo/tinypl",
    long_description=long_description,
    long_description_content_type="text/markdown",
)
