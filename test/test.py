#!/usr/bin/python

import sys
import time
import random

def main():
        ts = int(time.time())
        print ("test.mertric %d %s test.m1=%s"
               % (ts, random.randint(1, 5), "v1"))
        print ("test.mertric %d %s test.m2=%s"
               % (ts, random.randint(1, 5), "v2"))
        sys.stdout.flush()

if __name__ == "__main__":
    main()

