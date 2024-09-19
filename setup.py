try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

setup(name='strider',
      version='0.1.0',
      url='https://github.com/aini-naire/strider',
      description='Simple Python time-series DB',
      author='Emily Cavalcante',
      license='MIT',
      packages=find_packages(include=['strider', 'strider.*']),
      )