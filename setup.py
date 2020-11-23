from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="pff-factfinder",
    version="0.0.0",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/NYCPlanning/db-factfinder",
    packages=find_packages(),
    package_data={'factfinder': ['data/*.csv', 'data/*.json']},
    python_requires='>=3.6',
    install_requires=[
        "pandas", 
        "census", 
        "cached-property",
        "python-dotenv",
        "typer"
    ]
)