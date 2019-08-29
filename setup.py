import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='oedialect',
    version='v0.0.7',
    author='MGlauer',
    author_email='martinglauer89@gmail.com',
    description='SQL-Alchemy dialect for the OpenEnergy Platform',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/openego/oedialect',
    packages=setuptools.find_packages(exclude=["test"]),
    install_requires=[
        'sqlalchemy >= 1.2.0',
        'requests >= 2.13',
        'psycopg2-binary',
        'geoalchemy2',
        'shapely',
        'python-dateutil'
    ],
    keywords=['postgres', 'open', 'energy', 'database', 'sql', 'rest'],
    entry_points={
     'sqlalchemy.dialects': [
          'postgres.oedialect = oedialect.dialect:OEDialect'
          ]
    }
)


