from distutils.core import setup

setup(
    name='hey_pgsqlexec',
    version='1.0.0',
    author='Paul Heymann',
    author_email='hey_pgsqlexec@heymann.be',
    packages=['hey_pgsqlexec', ],
    license='LICENSE.txt',
    description='Hey! PostgreSQL SQL Executable Object',
    long_description=open('README.rst').read(),
    install_requires=[
        'psycopg2',
    ]
)
