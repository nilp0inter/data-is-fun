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

"""data-is-fun Agent

"""


import sys
import logging
import psutil
import platform

from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler

__author__ = "Roberto Abdelkader"
__credits__ = ["Roberto Abdelkader"]
__copyright__ = "Copyright (C) 2011 Roberto A. Martinez Perez"
__license__ = "GPL v3"
__version__ = "2.0"
__maintainer__ = "Roberto Abdelkader"
__email__ = "contacto@robertomartinezp.es"

class Agent(object):

    def __init__(self, config):

        self.log = logging.getLogger('agent')
        self.config = config

        self.version = (3, 0, 0)

        self.log.info("Agent starting...")

        # XML-RPC Server config
        self.bind_addr = self.config.get("main", "bind_addr", "string", "127.0.0.1")
        self.port = self.config.get("main", "bind_port", "int", "12700")

        self.server = SimpleXMLRPCServer((self.bind_addr, self.port))
        self.server.register_function(platform.uname,"system")
        self.server.register_function(self.cpu_percent, "cpu")
        self.server.register_function(self.phymem_usage, "mem")
        self.server.register_function(self.modules, "modules")

        self.server.serve_forever()

    def cpu_percent(self):
        return psutil.cpu_percent(percpu=True)

    def phymem_usage(self):
        m = psutil.phymem_usage()
        return (m.total, m.used, m.free, m.percent)

    def modules(self):
        # Return a list of system installed modules
        return filter(lambda x: x.find('.')==-1, sys.modules.keys())

    def version(self):
        # Return agent version
        return self.version


if __name__ == '__main__':
    import logging
    from util.config import Config
    log = logging.getLogger('main')
    a = Agent(Config(sys.argv[1]))
