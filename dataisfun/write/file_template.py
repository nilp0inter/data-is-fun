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

"""Provee clases de escritores para utilizar desde el core.

Los escritores reciben diccionarios clave-valor y los escriben en 
distintos medios: bases de datos, xml, binario, etc.
"""


try:
    import cStringIO as StringIO
except:
    import StringIO

__author__ = "Roberto Abdelkader"
__credits__ = ["Roberto Abdelkader"]
__copyright__ = "Copyright (C) 2011 Roberto A. Martinez Perez"
__license__ = "GPL v3"
__version__ = "2.0"
__maintainer__ = "Roberto Abdelkader"
__email__ = "contacto@robertomartinezp.es"

from dataisfun.writers import Writer 

class file_template(Writer):
    """ Class file_template.
        Write to plain text with templates.
    """

    def __init__(self, config, name):


        super(plain_file_template, self).__init__(config, name)
        self.template_filename = self.config.get(self.name, "template")

        self.output_location = self.config.get(self.name, "output")

    def start(self):

        try:
            # Restart template file
            self.template_content.seek(0)
        except:
            # Load template file
            try:
                self.template_file = open(self.template_filename, 'r')
            except:
                raise ValueError('Cannot open template (%s)' % self.template_filename)

            self.template_content = StringIO.StringIO()
            self.template_content.write(self.template_file.read())
            self.template_file.close()
            self.template_content.seek(0)
        
    def add_data(self, data):
        # Try to open output file location
        try:
            output = open(self.output_location % data, 'w')
        except Exception, e:
            raise ValueError(e)

        for line in self.template_content.readlines():
            output.write(line % data)

        output.close()

        self.template_content.seek(0)

    def finish(self):
        self.template_content.close()
        del self.template_content


