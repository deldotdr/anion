
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import anion

setup(
        name='anion',
        version=anion.__version__,
        author='Dorian Raymer',
        author_email='deldotdr@gmail.com',
        install_requires=['Twisted'],
        packages=['anion'],
        #scripts=['']
        )
