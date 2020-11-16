import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pff-factfinder",
    version="0.0.0",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/NYCPlanning/db-factfinder",
    packages=setuptools.find_packages(),
    python_requires='>=3.6',
    install_requires=[
        "pandas", 
        "census", 
        "cached-property",
        "python-dotenv",
        "typer"
    ]
)