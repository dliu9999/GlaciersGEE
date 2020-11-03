import sys
from setuptools import setup


with open('requirements.txt') as fid:
    install_requires = [l.strip() for l in fid.readlines() if l]


setup(
    name = 'Glaciers-GEE',
    packages = ['Glaciers-GEE'],
    version = '0.1.0',
    install_requires = install_requires,
    description = 'Researching and predicting glacier recession',
    author = 'Darren Liu',
    author_email = '',
    scripts = ['scripts/glacier.py']
)
