#!/usr/bin/env python
#
#    Copyright (C) 2011 Roberto A. Martinez Perez
#
#    This file is part of data-is-fun.
#
#    data-is-fun is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    data-is-fun is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with data-is-fun.  If not, see <http://www.gnu.org/licenses/>.

"""Lectores para text2db.

Estas clases reciben interpretan datos desde distintos medios y los transforman
en diccionarios clave valor para enviarlos al core. Deben ser iteradores ya que 
el core los tratara como tal.
"""


import subprocess

__author__ = "Roberto Abdelkader"
__credits__ = ["Roberto Abdelkader"]
__copyright__ = "Copyright (C) 2011 Roberto A. Martinez Perez"
__license__ = "GPL v3"
__version__ = "2.0"
__maintainer__ = "Roberto Abdelkader"
__email__ = "contacto@robertomartinezp.es"

from dataisfun.readers import Reader

class command(Reader):
    """
        Lector command.
            Ejecuta un comando y devuelve su resultado.
            Soporta reemplazo de argumentos.
    """

    def __init__(self, config, name, input_files):
       
        super(command, self).__init__(config, name)

        commands = self.config.c.items(self.name)
        commands = dict(filter(lambda x: x[0].startswith('exec_'), commands))

        self.strip = self.config.get(self.name, "strip", "boolean", True)

        self.command = {}
        for name, exestr in commands.iteritems():
            self.command[name[5:]] = exestr

    def next(self, extra_data = None):

        data = {}
        if extra_data:
            data.update(extra_data)

        for name, exestr in self.command.iteritems():
            if extra_data:
                exestr = exestr % extra_data

            exestr = filter(lambda x: x, exestr.split(' '))

            if self.strip:
                data[name] = subprocess.Popen(exestr, stdout=subprocess.PIPE).communicate()[0].rstrip('\r\n')
            else:
                data[name] = subprocess.Popen(exestr, stdout=subprocess.PIPE).communicate()[0]

        return data


