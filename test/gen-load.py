#!/usr/bin/python

import sys
import time
import random
import getopt
import socket

def main(argv):
    optlist, args = getopt.getopt(argv, 'i:')
    hostname = socket.gethostname()
    for i in range(1, int(args[1])):
        ts = int(time.time())
        print ("put test.mertric %d %s cluster_name=fake test.m1=%s host=%s"
               % (ts, random.randint(1, 100), "v1", hostname))
        print ("put test.mertric %d %s cluster_name=fake test.m1=%s host=%s"
               % (ts, random.randint(1, 20) + i, "v2", hostname))
    sys.stdout.flush()

if __name__ == "__main__":
    main(sys.argv)

