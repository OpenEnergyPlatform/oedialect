<a href="http://oep.iks.cs.ovgu.de/"><img align="right" width="200" height="200" src="https://avatars2.githubusercontent.com/u/37101913?s=400&u=9b593cfdb6048a05ea6e72d333169a65e7c922be&v=4" alt="OpenEnergyPlatform"></a>

# An [SQLAlchemy][0] [Dialect][1] for the [OEP][2]

SQLAlchemy internally uses so called "dialects" to provide a consistent
interface to different database drivers. The `oedialect` supplies your
SQLAlchemy installation with a dialect using the REST-API of the [Open
Energy Platform (OEP)][2]. In short, the `oedialect` allows you to use
SQLAlchemy to down- and upload data to an OEP instance.

[0]: https://www.sqlalchemy.org/
[1]: https://docs.sqlalchemy.org/en/13/dialects/
[2]: https://github.com/OpenEnergyPlatform/oeplatform

## License / Copyright

This repository is licensed under [GNU Affero General Public License v3.0 (AGPL-3.0)](https://www.gnu.org/licenses/agpl-3.0.en.html)

## Installation

`pip install oedialect`

On MS-Windows make sure to install a version of `shapely` first.
`conda install shapely -c conda-forge`

## Example

You can find a basic example [here](doc/example/oedialect_basic_example.ipynb).
