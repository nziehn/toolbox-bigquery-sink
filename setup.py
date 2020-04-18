from os import path
import setuptools

# read the contents of your README file
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

dependencies = [
    'google-cloud-storage>=1.26.0',
    'google-cloud-bigquery>=1.24.0'
]


setuptools.setup(
    name='toolbox-bigquery-sink',
    version='0.0.2',
    description='Tooling to write data to google bigquery',
    # long_description=long_description,
    # long_description_content_type='text/markdown',
    url='http://github.com/nziehn/toolbox-bigquery-sink',
    author='Nils Ziehn',
    author_email='nziehn@gmail.com',
    license='MIT',
    packages=[
        package for package in setuptools.find_packages() if package.startswith('toolbox')
    ],
    namespace_packages=['toolbox'],
    install_requires=dependencies,
    zip_safe=False
)