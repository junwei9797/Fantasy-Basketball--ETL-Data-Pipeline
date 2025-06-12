from setuptools import setup, find_packages

setup(
    name='my_package',           # Your package name
    version='0.1.0',
    packages=find_packages(),    # Automatically find packages in your project
    install_requires=[
        'pandas',
        'sqlalchemy',
        'python-dotenv',
        'psycopg2-binary',
        'nba_api',
        'numpy'
    ],
    author='JW',
    description='package for Fantasy ETL Pipeline',
)