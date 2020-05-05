from setuptools import find_namespace_packages, find_packages, setup

setup(
    name='api_wrapper',
    package_dir={"": "src"},
    packages=find_namespace_packages(where="src"),
    version='0.1.0',
    description='A simple wrapper to interact with web apis',
    author='Michael Duncan',
    license='MIT',
)
