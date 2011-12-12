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


__author__ = "Roberto Abdelkader"
__credits__ = ["Roberto Abdelkader"]
__copyright__ = "Copyright (C) 2011 Roberto A. Martinez Perez"
__license__ = "GPL v3"
__version__ = "2.0"
__maintainer__ = "Roberto Abdelkader"
__email__ = "contacto@robertomartinezp.es"

from dataisfun.readers import Reader

class sequence(Reader):
    """
        Genera rangos numericos
    """

    def __init__(self, config, name, input_files):
        super(sequence, self).__init__(config, name)

        self.seq_name = self.config.get(self.name, "name", "string", "sequence")
        self.seq_start = self.config.get(self.name, "start", "int", 0)
        self.seq_stop = self.config.get(self.name, "stop", "int", 10)
        self.seq_step = self.config.get(self.name, "step", "int", 1)

    def start(self):
        super(sequence, self).start()
        self.seq_current = self.seq_start

    def next(self, extra_data = None):
        if self.seq_current + self.seq_step < self.seq_stop:
            self.seq_current += self.seq_step
            data = {self.seq_name : self.seq_current}
            if extra_data:
                data.update(extra_data)
            return data
        else:
            raise StopIteration
        
    def finish(self):
        pass
