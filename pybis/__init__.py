# BIS PACKAGE

"""

~~~~~~~~~~~~~~~~~~~~~
BIS PYTHON PACKAGE
~~~~~~~~~~~~~~~~~~~~~

A set of helper code for Biogeographic Information System projects

url : https://maps.usgs.gov/
Email : bcb@usgs.gov

Author: Core Science Analytics, Synthesis and Libraries
Core Science Systems Division, U.S. Geological Survey

Software metadata: retrieve using "bis.get_package_metadata()"

"""

import pkg_resources  # part of setuptools


# Import bis objects
from . import bis
from . import bison
from . import db
from . import gap
from . import itis
from . import iucn
from . import natureserve
from . import rrl
from . import sgcn
from . import tess
from . import worms

# provide version, PEP - three components ("major.minor.micro")
__version__ = pkg_resources.require("pybis")[0].version

# metadata retrieval
def get_package_metadata():
    d = pkg_resources.get_distribution('pybis')
    for i in d._get_metadata(d.PKG_INFO):
        print(i)
