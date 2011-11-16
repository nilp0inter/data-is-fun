import os
import sys
import logging
import getopt
import ConfigParser

from reader import reader
from dbwriter import dbwriter
from dbinspector import dbinspector
from table_maker import table_maker

try:
    import progressbar
    progress = True
except:
    progress = False

def get_config(section, option, option_type="string", default=None):
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
    revision = "$Date$"
    # $Date$

    c = ConfigParser.RawConfigParser()

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
        
    c.read(config_file)

    if verbose_level == None:
        verbose_level = get_config("main", "verbose", "int", 2)

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

    log_file = get_config("main", "log_file", "string", None)
    log_format = '%(asctime)s [%(levelname)s] - %(message)s'

    log = logging.getLogger('main')
    if log_file:
        logging.basicConfig(filename=log_file, level=verbose_level, format=log_format)
    else:
        logging.basicConfig(level=verbose_level, format=log_format)


    log.info("Text2DB Started! v." + version + " rev." + revision)


    on_error = get_config("main", "on_error", "string", "rollback")
    inspect = get_config("main", "inspect", "boolean", True)


    if inspect:
        # Inspect files before insert
        try:
            w = dbinspector(get_config("writer", "hostname"), \
                        get_config("writer", "database"), \
                        get_config("writer", "username"), \
                        get_config("writer", "password"), \
                        get_config("writer", "table"), \
                        skip_columns=map(lambda x: x.strip(), get_config("writer", "skip_columns", "string", "").split(",")), \
                        force_text_fields=map(lambda x: x.strip(), get_config("writer", "force_text_fields", "string", "").split(","))\
                        )
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

            r = reader(filename, \
                        get_config("reader", "regexp"), \
                        skip_empty_lines=get_config("reader", "skip_empty_lines", "boolean", True), \
                        skip_first_line=get_config("reader", "skip_first_line", "boolean", False), \
                        delete_extra_spaces=get_config("reader", "delete_extra_spaces", "boolean", True),\
                        static_fields=get_config("reader", "static_fields")\
                        )

            query_type = get_config("writer", "query_type", "string", default="insert") 
            query_where = get_config("writer", "query_where", "string", default="")
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

    # Insert data
    try:
        w = dbwriter(get_config("writer", "hostname"), \
                    get_config("writer", "database"), \
                    get_config("writer", "username"), \
                    get_config("writer", "password"), \
                    get_config("writer", "table"), \
                    strict_column_checking=get_config("writer", "strict_column_checking", "boolean"), \
                    skip_columns=map(lambda x: x.strip(), get_config("writer", "skip_columns", "string", "").split(",")), \
                    pretend_queries=get_config("writer", "pretend_queries", "boolean"),\
                    flexible_schema=get_config("writer", "flexible_schema", "boolean"),\
                    force_text_fields=map(lambda x: x.strip(), get_config("writer", "force_text_fields", "string", "").split(","))\
                    )
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

        r = reader(filename, \
                    get_config("reader", "regexp"), \
                    skip_empty_lines=get_config("reader", "skip_empty_lines", "boolean", True), \
                    skip_first_line=get_config("reader", "skip_first_line", "boolean", False), \
                    delete_extra_spaces=get_config("reader", "delete_extra_spaces", "boolean", True),\
                    static_fields=get_config("reader", "static_fields")\
                    )

        query_type = get_config("writer", "query_type", "string", default="insert") 
        query_where = get_config("writer", "query_where", "string", default="")
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
