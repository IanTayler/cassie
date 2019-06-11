"""Setup and install cassie."""
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="cassie",
    version="0.0.1",
    description="Concurrency utilities for python.",
    ext_modules=[setuptools.Extension("_treiber", ["cassie/_treibermodule.c"])],
    packages=setuptools.find_packages(),
    long_description=long_description,
)
