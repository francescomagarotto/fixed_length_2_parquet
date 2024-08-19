import numpy
from setuptools import setup
from Cython.Build import cythonize

setup(
    name='flen2pq',
    version='1.0.0',
    py_modules=['flen2pq'],
    ext_modules=cythonize('flen2pq/flen2pq.py', compiler_directives={"language_level": "3"}, annotate=True
                          ),

    zip_safe=False,
    long_description=open('README.rst').read(),
)
