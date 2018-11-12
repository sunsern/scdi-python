from setuptools import setup

setup(name='pyscdi',
      version='0.2.0',
      description='Python binding for SCDI',
      url='http://github.com/sunsern/scdi-python',
      author='Sunsern Cheamanunkul',
      author_email='sunsern@gmail.com',
      license='MIT',
      packages=['pyscdi'],
      install_requires=[
          'requests'
      ],
      zip_safe=False)
