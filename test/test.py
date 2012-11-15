#!/usr/bin/python

import sys
import time
import random

def main():
        ts = int(time.time())
        for i in range(1, 100):
            print ("test.mertric %d %s test.m1=%s"
                   % (ts, random.randint(1, 5) + i, "v1"))
            print ("test.mertric %d %s test.m2=%s"
                   % (ts, random.randint(1, 5) + i, "v2"))
        sys.stdout.flush()

if __name__ == "__main__":
    main()

