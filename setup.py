from setuptools import setup, find_packages

setup(
    name='gym_framework',
    version='1.1',
    packages=find_packages(),  # detect subdependencies
    install_requires=[],
    author='Mikael, Yoni e George',
    description='Uma biblioteca voltada para frameworks com ferramentas para extração, transformação e modelagem de dados',
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.7', #minumum version that accept dataclasses
)