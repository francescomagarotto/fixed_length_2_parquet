from setuptools import setup
from Cython.Build import cythonize

setup(
    name='flen2pq',
    version='1.0.0',
    py_modules=['flen2pq'],
    ext_modules=cythonize('flen2pq.py'),
    long_description=open('README.rst').read(),
)