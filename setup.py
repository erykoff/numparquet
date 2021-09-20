from setuptools import setup, find_packages


name = 'numparquet'

setup(
    name=name,
    packages=find_packages(exclude=('tests')),
    description='Storing numpy recarrays in parquet',
    author='Eli Rykoff',
    author_email='erykoff@stanford.edu',
    url='https://github.com/erykoff/numparquet',
    install_requires=['numpy', 'pyarrow'],
    use_scm_version=True,
    setup_requires=['setuptools_scm', 'setuptools_scm_git_archive'],
)
