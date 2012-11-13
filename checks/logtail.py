#!/usr/bin/env python

import os
import subprocess
from string import rstrip
import sys

def main(argv):
    """The main entry point and loop."""

    config_path = argv[1] + "/logtail.d"
    configs = []
    for dirname, dirnames, filenames in os.walk(config_path):
        for filename in filenames:
            configs.append(os.path.join(dirname, filename))

    files = []
    for config in configs:
        f = open(config)
        for logfile in f:
            files.append(rstrip(logfile))
        f.close()

    for logfile in files:
        print logfile
        if os.path.exists(logfile):
            subprocess.call(['logtail', '-f', logfile])

if __name__ == '__main__':
    sys.exit(main(sys.argv))

