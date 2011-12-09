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


import urllib
from lxml import etree

__author__ = "Roberto Abdelkader"
__credits__ = ["Roberto Abdelkader"]
__copyright__ = "Copyright (C) 2011 Roberto A. Martinez Perez"
__license__ = "GPL v3"
__version__ = "2.0"
__maintainer__ = "Roberto Abdelkader"
__email__ = "contacto@robertomartinezp.es"

from dataisfun.readers import Reader

class web(Reader):
    """
        Lector de web.
    """

    def __init__(self, config, name, input_files):
       
        super(web, self).__init__(config, name)


        # Url parameter overwrite input_files from core
        self.url = self.config.get(self.name, "url", "string", None)
        assert(self.url!=None)        

        # XPath base
        self.base = self.config.get(self.name, "base", "string", None)
        assert(self.base!=None)        

        # Restart base in each iteration
        self.rebase = self.config.get(self.name, "rebase", "boolean", False)

        # Get data xpaths
        data_xpaths = self.config.c.items(self.name)
        self.data_xpaths = dict(map(lambda x: (x[0].replace('data_','',1), x[1]), filter(lambda x: x[0].startswith('data_'), data_xpaths)))

        self.current_url = None
        self.last_url = None
        self.current_base = None
        self.last_base = None

    def start(self):
        super(web, self).start()

    def next(self, extra_data = None):
        try:
            self.current_url = self.url % extra_data
        except TypeError:
            self.current_url = self.url

        if self.current_url != self.last_url:
            self.last_url = self.current_url
            try:
                self.web_file.close()
            except:
                pass
            finally:
                self.log.debug("Fetching url (%s)" % self.current_url)
                self.web_file = urllib.urlopen(self.current_url)
                self.web_content = self.web_file.read()
                #self.log.debug("Url Content: %s" % self.web_content)
                self.web_tree = etree.HTML(self.web_content)
                self.last_base = None

        try:
            self.current_base = self.base % extra_data
        except TypeError:
            self.current_base = self.base

        if self.current_base != self.last_base or self.rebase:
            self.last_base = self.current_base
            self.base_nodes = self.web_tree.xpath(self.current_base)
            self.base_nodes.reverse()

        try:
            self.current_base_node = self.base_nodes.pop()
        except IndexError:
            if not self.cyclic:
                raise StopIteration
            else:
                return extra_data

        data = dict() 

        for data_name, data_xpath in self.data_xpaths.iteritems():
            current_data = self.current_base_node.xpath(data_xpath)
            try:
                data[data_name] = current_data[0]
            except IndexError:
                data[data_name] = None

        if extra_data != None:
            data.update(extra_data)

        return data
            
        
    def finish(self):
        try:
            self.web_file.close()
        except:
            pass

