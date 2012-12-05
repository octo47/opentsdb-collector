#!/usr/bin/env python
import ConfigParser
import json
import string
import sys
import os.path
import urllib2
import re
from string import split, join
import itertools
import time

#!/usr/bin/python
# This file is part of tcollector.
# Copyright (C) 2012  Yandex, Inc.
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.  This program is distributed in the hope that it
# will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
# General Public License for more details.  You should have received a copy
# of the GNU Lesser General Public License along with this program.  If not,
# see <http://www.gnu.org/licenses/>.

DEFAULT_TIMEOUT = 30

class Bean:
    def __init__(self, bean):
        (sys, sub_name) = split(bean['name'], ':', 1)
        tags = map(lambda t: (t[0], t[1]),
            map(lambda x: split(x, '=', 1), split(sub_name, ',')))
        self.bean = bean
        self.sys = sys
        self.tags = dict(tags)

    def __str__(self):
        return self.bean['name'] + ":" + self.sys + "," + str(self.tags)

class JmxParser:
    def __init__(self, url, conf):
        self.url = url
        self.conf = conf
        f = urllib2.urlopen(self.url, timeout=DEFAULT_TIMEOUT)
        self.beans = map(lambda x: Bean(x), json.load(f)['beans'])

    def find_beans(self, sys, **kwargs):
        return filter(lambda x: (x.sys == sys and self.has_tags(x, **kwargs)), self.beans)

    def has_tags(self, bean, **kwargs):
        for (k, v) in kwargs.iteritems():
            tag_value = bean.tags.get(k)
            if not tag_value or not re.match(v, tag_value):
                return False
        return True

    def iter_attrs(self, bean, *regexps):
        for (attr, v) in bean.iteritems():
            for attr_regex in regexps:
                if re.match(attr_regex, attr):
                    yield (attr, v)

    def filter_attrs(self, bean, attr_regexps):
        for (attr, value) in bean.bean.iteritems():
            for regex in attr_regexps:
                if re.match(regex, attr):
                    yield (attr.encode('latin'), value)


class HDFSNameNode(JmxParser):

    what = { '.*': [
        '.*AvgTime',
        '.*NumOps',
        '^Total[A-Z]+',
        '^Files[A-Z]+',
        '^Capacity.*GB',
        '^Threads',
        '.*Blocks$'
    ] }

    def get_metrics(self):
        for (name, attr_regexps) in self.what.iteritems():
            for bean in self.find_beans('Hadoop', service='NameNode', name=name):
                for (attr, value) in self.filter_attrs(bean, attr_regexps):
                    tags = {}
                    if attr.endswith('NumOps'):
                        tags['op'] = attr[:-6]
                        attr = 'numOps'
                    elif attr.endswith('AvgTime'):
                        tags['op'] = attr[:-7]
                        attr = 'avgTime'
                    elif attr.endswith('Blocks'):
                        tags['state'] = attr[:-6]
                        attr = 'blocks'
                    elif attr.startswith('Total'):
                        if attr[5:] == 'Load':
                            continue
                        tags['type'] = attr[5:]
                        attr = 'count'
                    elif attr.startswith('Files'):
                        if attr[5:] == 'Total':
                            continue
                        tags['op'] = attr[5:]
                        attr = 'files'
                    elif attr.startswith('Threads'):
                        if attr[7:] == '':
                            continue
                        tags['type'] = attr[7:]
                        attr = 'threads'
                    elif attr.startswith('Capacity'):
                        if attr[8:] == 'TotalGB':
                            attr = 'capacity.total'
                        else:
                            tags['type'] = attr[8:]
                            attr = 'capacity'

                    yield 'hadoop.namenode.' + attr, tags, str(value)

class HDFSDataNode(JmxParser):

    what = { '.*': [
        '.*AvgTime$',
        '.*NumOps$',
        '.*Client$',
        '^Threads',
        '^Blocks',
        '^Bytes'
    ] }

    def get_metrics(self):
        for (name, attr_regexps) in self.what.iteritems():
            for bean in self.find_beans('Hadoop', service='DataNode', name=name):
                for (attr, value) in self.filter_attrs(bean, attr_regexps):
                    tags = {}
                    if attr.endswith('NumOps'):
                        tags['op'] = attr[:-6]
                        attr = 'numOps'
                    elif attr.endswith('AvgTime'):
                        tags['op'] = attr[:-7]
                        attr = 'avgTime'
                    elif attr.endswith('Client'):
                        tags['op'] = attr[6:]
                        attr = 'client'
                    elif attr.startswith('Blocks'):
                        tags['type'] = attr[6:]
                        attr = 'blocks'
                    elif attr.startswith('Bytes'):
                        tags['type'] = attr[5:]
                        attr = 'bytes'
                    elif attr.startswith('Threads'):
                        if attr[7:] == '':
                            continue
                        tags['type'] = attr[7:]
                        attr = 'threads'

                    yield 'hadoop.datanode.' + attr, tags, str(value)

class HBaseRegionServer(JmxParser):

    what = { 'RegionServerStatistics': ['.*'] }

    def get_metrics(self):
        for (name, attr_regexps) in self.what.iteritems():
            for bean in self.find_beans('hadoop', service='RegionServer', name=name):
                for (attr, value) in self.filter_attrs(bean, attr_regexps):
                    tags = {}
                    if attr.endswith('NumOps'):
                        tags['op'] = attr[:-6]
                        attr = 'numOps'
                    elif attr.endswith('AvgTime'):
                        tags['op'] = attr[:-7]
                        attr = 'avgTime'
                    elif attr.endswith('MaxTime'):
                        tags['op'] = attr[:-7]
                        attr = 'maxTime'
                    elif attr.endswith('MinTime'):
                        continue
                    elif 'Latency' in attr:
                        parts = attr.split('_')
                        if len(parts) != 2:
                            continue
                        attr = parts[0].replace('Latency','')
                        tags['type'] = parts[1]

                    yield 'hbase.regionserver.' + attr, tags, str(value)


def format_tags(tags):
    return string.join(map(
        lambda (tag, tagv): "%s=%s" % (tag, tagv),
        tags.items()), ' ')

def print_metrics(config, ts, section, clz):
    metrics = list(clz(config.get(section, 'url'), config).get_metrics())
    lines = map(lambda (metric, tags, value): "%s %s %d %s" % (metric, value, ts, format_tags(tags)), metrics)
    lines.sort()
    for line in lines:
        print line


def main(argv):
    """The main entry point and loop."""

    confpath = argv[1] + "/hadoop.conf"
    if not os.path.exists(confpath):
        return 0
    config = ConfigParser.SafeConfigParser()
    config.read(confpath)
    ts = int(time.time())

    for sec, clz in {'Namenode' : HDFSNameNode,
                     'Datanode' : HDFSDataNode,
                     'HBaseRegionServer' : HBaseRegionServer
    }.iteritems():
        if config.has_section(sec):
            print_metrics(config, ts, sec, clz)


def get_or_default(config, section, key, default_value):
    if not config.has_section(section):
        raise 'Empty or wrong config file: no such section %s in file %s' % (section, confpath)
    if config.has_key(section, key):
        return config.get(section, key)
    else:
        return default_value

if __name__ == '__main__':
    sys.exit(main(sys.argv))
