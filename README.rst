
.. figure:: https://user-images.githubusercontent.com/14353512/185425447-85dbcde9-f3a2-4f06-a2db-0dee43af2f5f.png
    :align: left
    :target: https://github.com/rl-institut/super-repo/
    :alt: Repo logo

==========
Oedialect
==========

.. list-table::
   :widths: auto

   * - License
     - |badge_license|
   * - Documentation
     - |badge_documentation|
   * - Publication
     -
   * - Development
     - |badge_issue_open| |badge_issue_closes| |badge_pr_open| |badge_pr_closes|
   * - Community
     - |badge_contributing| |badge_contributors| |badge_repo_counts|

.. contents::
    :depth: 2
    :local:
    :backlinks: top

 image:: https://avatars2.githubusercontent.com/u/37101913?s=400&u=9b593cfdb6048a05ea6e72d333169a65e7c922be&v=4
   :align: right
   :width: 200
   :height: 200
   :alt: OpenEnergyPlatform
   :target: http://oep.iks.cs.ovgu.de/

An `SQLAlchemy <https://www.sqlalchemy.org/>`_ `Dialect <https://docs.sqlalchemy.org/en/13/dialects/>`_ for the `OEP <https://github.com/OpenEnergyPlatform/oeplatform>`_
======================================================================================================================

SQLAlchemy internally uses so called "dialects" to provide a consistent
interface to different database drivers. The ``oedialect`` supplies your
SQLAlchemy installation with a dialect using the REST-API of the `Open
Energy Platform (OEP) <https://github.com/OpenEnergyPlatform/oeplatform>`_. In short, the ``oedialect`` allows you to use
SQLAlchemy to down- and upload data to an OEP instance.

License / Copyright
===================

This repository is licensed under `GNU Affero General Public License v3.0 (AGPL-3.0) <https://www.gnu.org/licenses/agpl-3.0.en.html>`_

Installation
=============

``pip install oedialect``

On MS-Windows make sure to install a version of ``shapely`` first.
``conda install shapely -c conda-forge``

Tutorials
==========

You can find tutorials and examples `here <https://github.com/OpenEnergyPlatform/examples/tree/master/api>`_.

Testing
========

To run the tests locally, first install the ``tox`` test environment
``pip install tox``

You need to setup a local instance of the `Open Energy Platform <https://github.com/OpenEnergyPlatform/oeplatform>`_

Set your connection token that you got from your local OEP instance
``LOCAL_OEP_TOKEN=<your_token>``

Finally, run
``tox``


.. |badge_license| image:: https://img.shields.io/github/license/OpenEnergyPlatform/oedialect
    :target: LICENSE.txt
    :alt: License

.. |badge_documentation| image::
    :target:
    :alt: Documentation

.. |badge_contributing| image:: https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat
    :alt: contributions

.. |badge_repo_counts| image:: http://hits.dwyl.com/OpenEnergyPlatform/oedialect.svg
    :alt: counter

.. |badge_contributors| image:: https://img.shields.io/badge/all_contributors-1-orange.svg?style=flat-square
    :alt: contributors

.. |badge_issue_open| image:: https://img.shields.io/github/issues-raw/OpenEnergyPlatform/oedialect
    :alt: open issues

.. |badge_issue_closes| image:: https://img.shields.io/github/issues-closed-raw/OpenEnergyPlatform/oedialect
    :alt: closes issues

.. |badge_pr_open| image:: https://img.shields.io/github/issues-pr-raw/OpenEnergyPlatform/oedialect
    :alt: closes issues

.. |badge_pr_closes| image:: https://img.shields.io/github/issues-pr-closed-raw/OpenEnergyPlatform/oedialect
    :alt: closes issues
