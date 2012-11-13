#!/usr/bin/env python
from logging.handlers import SysLogHandler
from optparse import OptionParser
import StringIO
from string import rfind
import ConfigParser
import os
import random
import socket
import stat
from subprocess import PIPE
import subprocess
import sys
import logging
import time
import signal
from itertools import ifilter


READ_LINE_BUF = 256

MAIN_SECTION = "Main"
TIMEOUT_KEY = "timeout"
CHECKSDIR_KEY = "checksdir"
CACHEDIR_KEY = "cachedir"
CONFIGDIR_KEY = "configdir"
HOSTS_KEY = "hosts"

TAGS_SECTION = "Tags"

LOG = logging.getLogger('opentsdb_checks')
default_config = os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])),
    'opentsdb_checks.defaults')
timestamp = str(time.time())
hostname = socket.gethostname()


class Alarm(Exception):
    pass


def alarm_handler(signum, frame):
    raise Alarm


def parse_cmdline(argv):
    """Parses the command-line."""

    # get arguments
    parser = OptionParser(description='Manages checks which gather '
                                      'data and report back.')
    parser.add_option('-c', '--config', dest='config', metavar='CONF',
        default="./opentsdb_checks.conf",
        help='Directory where the checks are located.')
    parser.add_option('-l', '--run-checks', dest='run_checks', metavar='RUN_CHECKS',
        default=None,
        help='Run only specified checks (basenames without full path, i.e. dfstat.py)')
    parser.add_option('-d', '--dry-run', dest='dryrun', action='store_true',
        default=False,
        help='Don\'t actually send anything to the TSD, '
             'just print the datapoints.')
    parser.add_option('-o', '--check-only', dest='check_only',
        action="store_true", default=False, help='Don\'t send anything, only get stats')
    parser.add_option('-t', '--send-only', dest='send_only',
        action="store_true", default=False, help='Don\'t check anything, send only')
    parser.add_option('-s', '--syslog', dest='use_syslog',
        action="store_true", default=False, help='Log to syslog.')
    parser.add_option('-v', dest='verbose', action='store_true', default=False,
        help='Verbose mode (log debug messages).')
    (options, args) = parser.parse_args(args=argv[1:])
    return options, args


def setup_logging(use_syslog=False):
    """Sets up logging and associated handlers."""

    LOG.setLevel(logging.INFO)
    if use_syslog:
        ch = SysLogHandler()
    else:
        ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter('%(asctime)s %(name)s[%(process)d] '
                                      '%(levelname)s: %(message)s'))
    LOG.addHandler(ch)


def read_config(config_file):
    config = ConfigParser.SafeConfigParser({
        "basedir": os.path.dirname(os.path.realpath(sys.argv[0])),
        "hostname": hostname
    })
    config.read(default_config)
    config.read(config_file)
    return config


def get_cache_dir(config):
    return config.get(MAIN_SECTION, CACHEDIR_KEY)


def get_servers(config):
    return config.get(MAIN_SECTION, HOSTS_KEY).split(',')

def get_tags(config):
    LOG.debug("lookup for tags")
    items = filter(lambda (key, value): not config.has_option("DEFAULT", key),
                   config.items(TAGS_SECTION))
    LOG.debug("got tags %s" % items)
    return reduce(lambda accum, (key, value): accum + " " + key + "=" + value, items, "")


def init_caches(config):
    cache_dir = get_cache_dir(config)
    LOG.debug("cache: %s" % cache_dir)
    try:
        os.mkdir(cache_dir)
    except:
        pass


def call_checks(run_checks, config):
    checks_dir = config.get(MAIN_SECTION, CHECKSDIR_KEY)
    confs_dir = config.get(MAIN_SECTION, CONFIGDIR_KEY)
    timeout = config.getint(MAIN_SECTION, TIMEOUT_KEY)
    tags = get_tags(config)
    LOG.debug("lookup for checks in: %s" % checks_dir)
    if os.path.exists(checks_dir):
        checks =  os.listdir(checks_dir)
        if run_checks:
            checks = list(ifilter(lambda f: (run_checks and f in run_checks), checks))
        LOG.debug("run checks: %s" % checks)
        n = 0
        ch = {}
        p_name = {}
        for check in checks:
            file = checks_dir + "/" + check
            if not os.path.isfile(file):
                continue
            if not os.access(file, os.X_OK):
                try:
                    os.chmod(file, stat.S_IEXEC)
                except:
                    continue
            n += 1
            try:
                ch[n] = subprocess.Popen([file, confs_dir], stdout=PIPE)
                p_name[n] = file
                LOG.debug("%d:check: %s" % (n, file))
            except:
                print "Unable to run check:", file

        for i in range(1, n + 1):
            signal.signal(signal.SIGALRM, alarm_handler)
            signal.alarm(timeout)
            uniq = random.randint(1, 10^20)
            try:
                result = StringIO.StringIO(ch[i].communicate()[0])
                file_name_target = get_cache_dir(config) + "/" + timestamp + "-" + str(uniq)
                file_name_tmp = file_name_target + ".part"
                try:
                    f = open(file_name_tmp, 'a')
                    while True:
                        line = result.readline().rstrip()
                        if not line:
                            break
                        f.write(line + tags + "\n")
                    f.close()
                    if os.stat(file_name_tmp).st_size == 0:
                        os.unlink(file_name_tmp)
                    else:
                        os.rename(file_name_tmp, file_name_target)
                except:
                    LOG.exception("Failed")
                    if os.path.exists(file_name_tmp):
                        os.unlink(file_name_tmp)
                    if os.path.exists(file_name_target):
                        os.unlink(file_name_target)
                signal.alarm(0)
            except:
                LOG.exception("Failed")
                try:
                    print "Task", p_name[i], "killed due to unable to "\
                                             "parse and store output in",\
                    timeout, "sec"
                    ch[i].kill()
                except:
                    continue
                continue


def send_outstanding(config):
    timeout = config.getint(MAIN_SECTION, TIMEOUT_KEY)
    servers = get_servers(config)
    random.shuffle(servers)
    cache_dir = get_cache_dir(config)
    chunks = sorted(os.listdir(cache_dir))
    LOG.debug("Found chunks %s for %s" % (chunks, servers))
    chunks = list(ifilter(lambda fname: rfind(fname, '.part') != 5, chunks))
    if not chunks or len(chunks) == 0:
        return
    LOG.debug("Sending %s to %s" % (chunks, servers))
    for server in servers:
        try:
            con = mk_conn(server, timeout)
            if not con:
                continue
            try:
                verify_conn(con)
                for chunk in chunks:
                    send_file(con, cache_dir + "/" + chunk)
                    LOG.debug("Done %s to %s" % (chunk, server))
                verify_conn(con)

            finally:
                con.close()
        except:
            LOG.exception("Can't send to %s" % server)
            continue
        break


def mk_conn(server, timeout):
    # Now actually try the connection.
    host = server.split(':')[0]
    port = int(server.split(':')[1].rstrip())
    try:
        con = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        con.settimeout(timeout)
        con.setblocking(True)
        con.connect((host, port))
        # if we get here it connected
    except socket.error, msg:
        con = None
        LOG.warning('Connection attempt failed to %s:%s: %s' % (host, port, msg))
    if not con:
        LOG.error('Failed to connect to %s:%d' % (host, port))
    else:
        LOG.debug('Connected to %s:%d' % (host, port))
    return con


def verify_conn(con):
    LOG.debug('verifying TSD is alive')
    send_msg(con, "version\n")


def send_file(con, filename):
    f = open(filename)
    while True:
        message = f.readline()
        if not message:
            break
        message = "put " + message
        con.sendall(message)
    f.close()
    os.unlink(filename)


def send_msg(socket, msg):
    socket.sendall(msg)
    return recv_line(socket)

def recv_line(socket):
    buffer = socket.recv(1)
    while True:
        if "\n" in buffer:
            (line, buffer) = buffer.split("\n", 1)
            return line
        else:
            more = socket.recv(1)
            if not more:
                break
            else:
                buffer = buffer + more
    if buffer:
        return buffer

def main(argv):
    """The main entry point and loop."""

    options, args = parse_cmdline(argv)
    setup_logging(options.use_syslog)

    if options.verbose:
        LOG.setLevel(logging.DEBUG)  # up our level
    LOG.debug("Initializing checks with config %s" % options.config)
    config = read_config(options.config)
    init_caches(config)

    if not options.send_only:
        if options.run_checks:
            checks_set = set(options.run_checks.split(","))
        else:
            checks_set = None
        call_checks(checks_set, config)

    if not options.check_only:
        time.sleep(random.randint(0, 5))
        send_outstanding(config)
    else:
        LOG.debug("Send prohibited.")

if __name__ == '__main__':
    sys.exit(main(sys.argv))
