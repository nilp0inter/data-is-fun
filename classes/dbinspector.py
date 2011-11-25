#!/usr/bin/env python
"""Clase de inspeccion de base de datos.

Interfaz para table_maker, dependiendo de la configuracion crea o modifica
tablas de MySQL.
"""


import sys
import logging

#
# apt-get install python-mysqldb
#
import MySQLdb

from table_maker import table_maker

__author__ = "Roberto Abdelkader"
__credits__ = ["Roberto Abdelkader"]
__license__ = "GPL"
__version__ = "1.0"
__maintainer__ = "Roberto Abdelkader"
__email__ = "contacto@robertomartinez.es"


