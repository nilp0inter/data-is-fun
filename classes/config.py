#!/usr/bin/env python
"""Clase de configuración básica sobre ficheros de texto.

"""

import ConfigParser

__author__ = "Roberto Abdelkader"
__credits__ = ["Roberto Abdelkader"]
__license__ = "GPL"
__version__ = "1.0"
__maintainer__ = "Roberto Abdelkader"
__email__ = "contacto@robertomartinezp.es"
__status__ = "Production"


class Config:
    def __init__(self, config_file):
        self.c = ConfigParser.RawConfigParser()
        self.c.read(config_file)

    def get(self, section, option, option_type="string", default=None):
        """
            Carga una opcion del fichero de configuracion.
            Version safe getopt

        """
        value = default
        try:
            if option_type == "string":
                value = c.get(section,option)
            elif option_type == "boolean":
                value = c.getboolean(section,option)
            elif option_type == "int":
                value = c.getint(section,option)
            elif option_type == "float":
                value = c.getfloat(section,option)
        except:
            value = default

        log = logging.getLogger('main.config')
        log.debug("Setting option %s\\%s = %s" % (section, option, value))
        return value

