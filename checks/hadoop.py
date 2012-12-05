#!/usr/bin/env python
import ConfigParser
import json
import sys
import os.path
import urllib2
import re
from string import split, join
import itertools

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


def fetch_metrics(config, section, clz):
    metrics = list(clz(config.get(section, 'url'), config).get_metrics())
    metrics.sort(key=lambda (x, y, z): x)
    print(join(
        map(lambda (x, y, z): str((x, y, z)), metrics), '\n'))


def main(argv):
    """The main entry point and loop."""

    confpath = argv[1] + "/hadoop.conf"
    print('Using %s config' % confpath)
    if not os.path.exists(confpath):
        return 0
    config = ConfigParser.SafeConfigParser()
    config.read(confpath)

    for sec, clz in {'Namenode' : HDFSNameNode,
                     'Datanode' : HDFSDataNode,
                     'HBaseRegionServer' : HBaseRegionServer
    }.iteritems():
        if config.has_section(sec):
            fetch_metrics(config, sec, clz)


def get_or_default(config, section, key, default_value):
    if not config.has_section(section):
        raise 'Empty or wrong config file: no such section %s in file %s' % (section, confpath)
    if config.has_key(section, key):
        return config.get(section, key)
    else:
        return default_value

if __name__ == '__main__':
    sys.exit(main(sys.argv))
