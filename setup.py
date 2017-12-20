from distutils.core import setup

setup(
    name='oedialect',
    version='v0.0.1',
    packages=['oedialect'],
    install_requires=['sqlalchemy >= 1.2.0b1',
                      'requests >= 2.13'],
    url='',
    license='',
    author='MGlauer',
    author_email='',
    description='',
    entry_points={
     'sqlalchemy.dialects': [
          'postgres.oedialect = oedialect.dialect:OEDialect'
          ]
    }

)


