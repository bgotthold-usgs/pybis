pyBIS
=======================


-----------
Installation Notes:
-----------

If you have git and pip available, bis can be installed with:

`pip install git+https://github.com/usgs-bis/pybis.git`

* The db module in this package currently requires system variables to be set in the running environment in order to connect to cloud-based database infrastructure.
* Current instances of this package were built using a Conda Python 3.6 environment in order to elegantly handle the GDAL installation. The requirements.txt was built from this virtual environment.


-----------
Purpose:
-----------
The Biogeographic Information System is a project of the Biogeographic Characterization Branch, part of Core Science Analytics, Synthesis and Library in the US Geological Survey. Our program works to characterize species, habitats, conservation protection measures, and active and projected threats to biodiversity in a living data system that helps inform decisions by resource managers and policy makers. The "pyBIS" Python package is part of our Biogeographic Information System, the underlying intelligence engine behind all of our work. The package contains several modules that perform data management functions for the BIS.


----------------------
Copyright and License:
---------------------
## Provisional Software Disclaimer
Under USGS Software Release Policy, the software codes here are considered preliminary, not released officially, and posted to this repo for informal sharing among colleagues.

This software is preliminary or provisional and is subject to revision. It is being provided to meet the need for timely best science. The software has not received final approval by the U.S. Geological Survey (USGS). No warranty, expressed or implied, is made by the USGS or the U.S. Government as to the functionality of the software and related material nor shall the fact of release constitute any such warranty. The software is provided on the condition that neither the USGS nor the U.S. Government shall be held liable for any damages resulting from the authorized or unauthorized use of the software.
