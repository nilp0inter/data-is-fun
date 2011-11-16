#!/usr/bin/env python
"""Text2DB. Genera bases de datos a partir de otros formatos.

....................                       .....................
.                  .                       .                   .
.                  .                       .                   .
.     READERS      .                       .      WRITERS      .
.                  .                       .                   .
.                  .                       .                   .
....................                       .....................
        ^          ..                    ..         |           
        |            ..                ..           v           
                       ................            ___          
       /-/|            .              .           (___)         
      /_/ |            .              .           |   |         
      | |/|            .     CORE     .           |   |         
      | | |            .              .           |   |         
      |_|/             .              .           |___|         
     PLAIN             ................          DATABASE       
       FORMAT
"""


import os
import sys
import logging
import getopt

# Add class dir to the path
BASE_DIR = os.path.dirname(__file__) or '.'
CLASS_DIR = os.path.join(BASE_DIR, 'classes')
sys.path.insert(0, CLASS_DIR)

from reader import reader
from dbwriter import dbwriter
from dbinspector import dbinspector
from config import Config

try:
    import progressbar
    progress = True
except:
    progress = False

__author__ = "Roberto Abdelkader"
__credits__ = ["Roberto Abdelkader"]
__license__ = "GPL"
__version__ = "1.0"
__maintainer__ = "Roberto Abdelkader"
__email__ = "contacto@robertomartinezp.es"
__status__ = "Production"

    
def file_len(fname):
    try:
        f = open(fname)
        for i, l in enumerate(f):
            pass
        return i + 1
    except:
        return 0

def usage():
    print "usage: text2db [-c|--config] <configfile> [-h|--help] [-q|--quiet] [-d|--debug] [file1...fileN]"
    print "   -c|--config\tSet config file"
    print "   -h|--help\tShow this help and exit"
    print "   -q|--quiet\tSuppress messages"
    print "   -d|--debug\tEnable debug mode"
    print ""
    sys.exit(2)

if __name__ == '__main__':
    version = "1.0"

    #
    # Parse command line options
    #
    try:
        opts, files_to_read = getopt.getopt(sys.argv[1:], "hqc:d", ["help", "quiet", "config=", "debug"])
    except getopt.GetoptError, err:
        # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        usage()

    verbose_level = None
    config_file = None

    for optlist, arglist in opts:
        if optlist in ("-q", "--quiet"):
            verbose_level = 0 
        elif optlist in ("-d", "--debug"):
            verbose_level = 3
        elif optlist in ("-h", "--help"):
            usage()
        elif optlist in ("-c", "--config"):
            config_file = arglist
        else:
            assert False, "unhandled option"

    if not config_file:
        usage()
        
    #
    # Load config from file
    #
    c = Config(config_file)


    # 
    # Start logger
    #
    if verbose_level == None:
        verbose_level = c.get("main", "verbose", "int", 2)

    if verbose_level == 0: # TOTALY QUIET
        verbose_level = logging.CRITICAL
        progress = False
    elif verbose_level == 1:
        verbose_level = logging.ERROR
    elif verbose_level == 2:
        verbose_level = logging.INFO
    else:
        verbose_level = logging.DEBUG
        progress = False

    log_file = c.get("main", "log_file", "string", None)
    log_format = '%(asctime)s [%(levelname)s] - %(message)s'

    log = logging.getLogger('main')
    if log_file:
        logging.basicConfig(filename=log_file, level=verbose_level, format=log_format)
    else:
        logging.basicConfig(level=verbose_level, format=log_format)


    log.info("Text2DB Started! v.%s", version)

    on_error = c.get("main", "on_error", "string", "rollback")
    inspect = c.get("main", "inspect", "boolean", True)

    #
    # File inspection
    #
    if inspect:
        # Inspect files before insert
        try:
            w = dbinspector(c)
##            w = dbinspector(c.get("writer", "hostname"), \
##                        c.get("writer", "database"), \
##                        c.get("writer", "username"), \
##                        c.get("writer", "password"), \
##                        c.get("writer", "table"), \
##                        skip_columns=map(lambda x: x.strip(), c.get("writer", "skip_columns", "string", "").split(",")), \
##                        force_text_fields=map(lambda x: x.strip(), c.get("writer", "force_text_fields", "string", "").split(","))\
##                        )
        except Exception, e:
            log.error("Error starting database inspector")
            log.exception(e)
            sys.exit(1)

        for filename in files_to_read:
            try:
                del r
            except:
                pass

            log.info("Inspecting file: " + filename)
            if progress: # First count lines
                widgets = [os.path.basename(filename) + ': ', progressbar.Percentage(), ' ', progressbar.Bar(marker='*',left='[',right=']'), ' ', progressbar.ETA(), ' ', progressbar.FileTransferSpeed()]
                pbar = progressbar.ProgressBar(widgets=widgets, maxval=file_len(filename))
                pbar.start()

            r = reader(c, filename)
#            r = reader(filename, \
#                        c.get("reader", "regexp"), \
#                        skip_empty_lines=c.get("reader", "skip_empty_lines", "boolean", True), \
#                        skip_first_line=c.get("reader", "skip_first_line", "boolean", False), \
#                        delete_extra_spaces=c.get("reader", "delete_extra_spaces", "boolean", True),\
#                        static_fields=c.get("reader", "static_fields")\
#                        )

            query_type = c.get("writer", "query_type", "string", default="insert") 
            query_where = c.get("writer", "query_where", "string", default="")
            try:
                for data in r:
                    w.add_data(data)
                    if progress:
                        pbar.update(w.added)
            
                log.info("File " + filename + " successfully processed.")
                if progress:
                    pbar.finish()
            except (KeyboardInterrupt, SystemExit):
                log.info("User exit while inspecting")
                sys.exit(2)
            except Exception, e:
                log.exception(e)
                log.info("Error detected while inspecting")
                sys.exit(1)

        # Commit changes
        w.finish()
        del w

    #
    # Insert data
    #
    try:
        w = dbwriter(c)
#        w = dbwriter(c.get("writer", "hostname"), \
#                    c.get("writer", "database"), \
#                    c.get("writer", "username"), \
#                    c.get("writer", "password"), \
#                    c.get("writer", "table"), \
#                    strict_column_checking=c.get("writer", "strict_column_checking", "boolean"), \
#                    skip_columns=map(lambda x: x.strip(), c.get("writer", "skip_columns", "string", "").split(",")), \
#                    pretend_queries=c.get("writer", "pretend_queries", "boolean"),\
#                    flexible_schema=c.get("writer", "flexible_schema", "boolean"),\
#                    force_text_fields=map(lambda x: x.strip(), c.get("writer", "force_text_fields", "string", "").split(","))\
#                    )
    except Exception, e:
        log.error("Error starting database writer")
        log.exception(e)
        sys.exit(1)

    for filename in files_to_read:
        try:
            del r
        except:
            pass

        log.info("Processing file: " + filename)
        if progress: # First count lines
            widgets = [os.path.basename(filename) + ': ', progressbar.Percentage(), ' ', progressbar.Bar(marker='*',left='[',right=']'), ' ', progressbar.ETA(), ' ', progressbar.FileTransferSpeed()]
            pbar = progressbar.ProgressBar(widgets=widgets, maxval=file_len(filename))
            pbar.start()

        r = reader(c, filename)
#        r = reader(filename, \
#                    c.get("reader", "regexp"), \
#                    skip_empty_lines=c.get("reader", "skip_empty_lines", "boolean", True), \
#                    skip_first_line=c.get("reader", "skip_first_line", "boolean", False), \
#                    delete_extra_spaces=c.get("reader", "delete_extra_spaces", "boolean", True),\
#                    static_fields=c.get("reader", "static_fields")\
#                    )

        query_type = c.get("writer", "query_type", "string", default="insert") 
        query_where = c.get("writer", "query_where", "string", default="")
        try:
            for data in r:
                sql_query = w.make_query(data, query_type = query_type, query_where = query_where)
                if sql_query:
                    w.do_query(sql_query)
                elif on_error == "pass":
                    log.warning("Invalid query, not inserting! Maybe malformed regexp or malformed line?")
                else:
                    raise Exception("Empty query!, maybe malformed regexp?")
                if progress:
                    pbar.update(w.added)
        
            log.info("File " + filename + " successfully processed.")
            if progress:
                pbar.finish()
            w.do_commit()
        except (KeyboardInterrupt, SystemExit):
            log.info("User exit, doing action: " + on_error)
            w.do_rollback()
            sys.exit(2)
        except Exception, e:
            log.exception(e)
            if on_error == "pass":
                log.error("Error detected, doing action: " + on_error)
                pass
            else:
                log.error("Error detected, doing default action: rollback")
                w.do_rollback()
                sys.exit(1)

    log.info("Done!, exiting...")
    sys.exit(0)
