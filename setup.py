import os
from setuptools import setup, find_packages

# Utility function to read the README file.
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "esgfpy-publish",
    version = "0.9",
    author = "Luca Cinquini",
    author_email = "luca.cinquini@jpl.nasa.gov",
    description = ("Python client-side package for publishing data to the Earth System Grid Federation (ESGF)"),
    license = "ASF",
    keywords = "python publishing client ESGF earth system grid federation",
    url = "https://github.com/EarthSystemCoG/esgfpy-publish",
    packages=find_packages(),
    install_requires=['python-dateutil', 'pil', 'h5py', 'netCDF4', 'solrpy'],
    long_description=read('README'),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Topic :: Utilities",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 2 :: Only",
    ],
)
