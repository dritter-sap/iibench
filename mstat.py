#!/usr/bin/python
#
# Copyright (C) 2009 Google Inc.
# Copyright (C) 2009 Facebook Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Originally published by Google.
# Additional improvements from Facebook.
#

"""Multiple-stat reporter

This gathers performance statistics from multiple data sources and reports
them over the same interval. It supports iostat and vmstat on modern Linux
distributions and SHOW GLOBAL STATUS output from MySQL. It is a convenient way
to collect data during benchmarks and can perform some computations on the
collected data including 'rate', 'avg' and 'max'. It can also aggregate
data from multiple devices from iostat.

This reports all data from vmstat, iostat and SHOW GLOBAL STATUS as counters
and as rates. Other stats can be derived from them and defined on the command
line or via file named by the --sources option.

Run it like:
  mstat.py --loops 1000000 --interval 60
"""

__author__ = 'Mark Callaghan (mdcallag@gmail.com)'

import itertools
import optparse
import subprocess
import sys
import time
import re
import pdb

import MySQLdb

class MyIterable:
  def __iter__(self):
    return self

class Timestamper(MyIterable):
  def next(self):
    return time.strftime('%Y-%m-%d_%H:%M:%S')

class Counter(MyIterable):
  def __init__(self, interval):
    self.interval = interval
    self.value = 0

  def next(self):
    result = str(self.value)
    self.value += self.interval
    return result

class DiskSizer(MyIterable):
  def __init__(self, path):
    self.path = path

  def next(self):
    return subprocess.Popen(['du', '-s', '-b', self.path],
                            stdout=subprocess.PIPE).communicate()[0].split()[0]

class ScanMysql(MyIterable):
  def __init__(self, db_user, db_password, db_host, db_port, db_name, sql,
               retries, err_data):
    self.db_user = db_user
    self.db_password = db_password
    self.db_host = db_host
    self.db_port = db_port
    self.db_name = db_name
    self.sql = sql
    self.retries = retries
    self.err_data = err_data

  def next(self):
    r = self.retries
    while r >= 0:
      connect = None
      try:
        connect = MySQLdb.connect(host=self.db_host, port=self.db_port,
                                  user=self.db_user, passwd=self.db_password,
                                  db = self.db_name)
        cursor = connect.cursor()
        cursor.execute(self.sql)
        result = []
        for row in cursor.fetchall():
          result.append(' '.join(row))
        connect.close()
        # print 'Connectdb'
        if result:
          return '\n'.join(result)
        else:
          return self.err_data
      except MySQLdb.Error, e:
        print 'sql (%s) fails (%s)' % (self.sql, e)
        if connect is not None:
          try:
            connect.close()
          except MySQLdb.Error, e:
            pass
        time.sleep(0.1)
      r -= 1
    return self.err_data

class ScanFork(MyIterable):
  def __init__(self, cmdline, skiplines):
    self.proc = subprocess.Popen(cmdline, shell=True, bufsize=1,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
    self.cmdline = cmdline
    self.lines_to_skip = skiplines
    print >>sys.stderr, "forked pid %d for (%s)" % (
      self.proc.pid, cmdline)

  def next(self):
    while True:
      line = self.proc.stdout.readline()
      if not line:
        raise StopIteration
      elif self.lines_to_skip > 0:
        self.lines_to_skip -= 1
        continue
      else:
        return line

class ScanDiskstats(MyIterable):
  def next(self):
    proc = subprocess.Popen(['cat', '/proc/diskstats'],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    output, err = proc.communicate()
    return output

class ScanFio(MyIterable):
  def __init__(self, devices):
    self.devices = devices

  def next(self):
    result = []
    non_decimal = re.compile(r'[^\d.]+')

    for dev in self.devices:
      result.append('%s' % dev)
      proc = subprocess.Popen(['fio-status', '-a', '/dev/%s' % dev],
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
      output, err = proc.communicate()

      res_len = len(result)
      for line in output.split('\n'):
        line = line.strip()

        if line.startswith('Physical bytes written'):
          result.append(non_decimal.sub('', line.split(':')[1]))
        elif line.startswith('Physical bytes read'):
          result.append(non_decimal.sub('', line.split(':')[1]))
          break
      
      if len(result) == res_len:
        print >>sys.stderr, "Unable to get fio-status"
        return "fio-status error" 

      proc = subprocess.Popen(['cat', '/proc/fusion/fio/%s/data/groomer/stats' % dev],
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
      output, err = proc.communicate()
      res_len = len(result)
      for line in output.split('\n'):
        if line.startswith('Blocks Erased:'):
          ar = line.split()
          result.append(ar[2])
          result.append(ar[4])
        elif line.startswith('Data copied:'):
          result.append(line.split()[2])
          break
      
      if len(result) == res_len:
        print >>sys.stderr, "Unable to get groomer stats"
        return "groomer error" 

      result.append('\n')

    return ' '.join(result)

class ScanMicron(MyIterable):
  def __init__(self, devices):
    self.devices = devices

  def next(self):
    result = []

    for dev in self.devices:
      result.append('%s' % dev)
      proc = subprocess.Popen(['smartctl', '-A', '-l', 'devstat,1', '/dev/%s' % dev],
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
      output, err = proc.communicate()

      res_len = len(result)
      for line in output.split('\n'):
        line = line.strip()

        if line.startswith('247 Unknown_Attribute') or line.startswith('248 Unknown_Attribute'):
          field = 9
          factor = 1
        elif line.endswith('Sectors Written') or line.endswith('Sectors Read'):
          field = 4
          factor = 512
        else:
          continue

        ar = line.split("  ")
        found = None
        for col in ar:
          if len(col) > 0:
            field = field - 1
          if field == 0:
            found = col.strip()
            break
        if found is None:
          print >>sys.stderr, "Unable to get micron stats for %s" % dev
          return "micron stat parsing error"
        result.append(str(int(found) * factor))
      
      if len(result) == res_len:
        print >>sys.stderr, "Unable to get smartctl data"
        return "smartctl error" 

      # bytes copied is not exported yet
      result.append('0')
      result.append('\n')

    return ' '.join(result)

class FilterEquals(MyIterable):
  def __init__(self, pos, value, iterable, iostat_hack=False):
    self.pos = pos
    self.value = value
    self.iter = iterable
    self.iostat_hack = iostat_hack

  def next(self):
    while True:
      lines = self.iter.next()
      for line in lines.split('\n'):
        cols = line.split()
        if len(cols) >= (self.pos + 1) and cols[self.pos] == self.value:
          return line
        # Ugly hack for long device name split over 2 lines
        # elif self.iostat_hack and len(cols) == 1 and cols[self.pos] == self.value:
        # return '%s %s' % (self.value, self.iter.next())

class Project(MyIterable):
  def  __init__(self, pos, iterable):
    self.pos = pos
    self.iter = iter(iterable)

  def next(self):
    line = self.iter.next()
    cols = line.split()
    try:
      v = float(cols[self.pos])
      return cols[self.pos]
    except ValueError, e:
      return 0.0

class ExprAbsToRel(MyIterable):
  def __init__(self, interval, iterable):
    self.interval = interval
    self.iter = iter(iterable)
    self.prev = None

  def next(self):
    current = float(self.iter.next())
    if self.prev is None:
      self.prev = current
      return '0'
    else:
      diff = current - self.prev
      rate = diff / self.interval
      self.prev = current
      return str(rate)

class ExprFunc(MyIterable):
  def __init__(self, func, iterables):
    self.func = func
    self.iters = [iter(i) for i in iterables]

  def next(self):
    return str(self.func([float(i.next()) for i in self.iters]))

class ExprAvg(MyIterable):
  def __init__(self, iterables):
    self.iters = [iter(i) for i in iterables]

  def next(self):
    return str(sum([float(i.next()) for i in self.iters]) / len(self.iters))

vmstat_cols = { 'swpd':2, 'free':3, 'buff':4, 'cache':5, 'si':6, 'so':7,
                'bi':8, 'bo':9, 'in':10, 'cs':11, 'us':12, 'sy':13,
                'id':14, 'wa':15 }

iostat_cols = { 'rrqm/s':1, 'wrqm/s':2, 'r/s':3, 'w/s':4, 'rkB/s':5,
                'wkB/s':6,  'avgrq-sz':7, 'avgqu-sz':8, 'await':9,
                'svctm':10, '%util':11 }

fio_cols = { 'bytes_written':1, 'bytes_read':2, 'blocks_erased':3,
             'bytes_erased':4, 'bytes_copied':5 }

micron_cols = { 'host_pages':1, 'copy_pages':2, 'bytes_written':3,
                'bytes_read':4 }

# du_cols = [ '/data/mysql/rmy/data/rocksdb' ]
du_cols = [ '/data/mysql/toku5539/data' ]

agg_funcs = [ 'sum', 'rate', 'ratesum', 'max', 'avg' ]

diskstats_cols = { 'reads completed successfully':3,
                   'reads merged':4,
                   'sectors read':5,
                   'time spent reaing (ms)':6,
                   'writes completed':7,
                   'writes merged':8,
                   'sectors written':9,
                   'time spent writing (ms)':10,
                   'I/Os currently in progress':11,
                   'time spent doing I/Os (ms)':12,
                   'weighted time spent doing I/Os (ms)':13
                 }   

def iostat_get_devices():
  scan_iostat = ScanFork('iostat -x 1 1', 0)
  saw_device = False
  devices = []
  for line in scan_iostat:
    if line.startswith('Device:'):
      saw_device = True
    elif saw_device:
      cols = line.split()
      if cols:
        devices.append(cols[0])
  return devices

def fio_get_devices():
  scan_fio = ScanFork('ls -d /dev/fio?', 0)
  saw_device = False
  devices = []
  for line in scan_fio:
    devices.append(line.split('/')[2].strip())
  return devices

def micron_get_devices():
  scan_micron = ScanFork('ls -d /sys/block/sd*', 0)
  devices = []
  for line in scan_micron:
    model = ScanFork('cat %s/device/model' % line.strip(), 0)
    for name in model:
      if name.strip() == 'Micron_M500_MTFD':
        devices.append(line.split('/')[3].strip())
  return devices

def get_matched_devices(prefix, devices):
  assert prefix[-1] == '*'
  matched = []
  for d in devices:
    if d.startswith(prefix[:-1]) and len(d) > len(prefix[:-1]):
      matched.append(d)
  return matched

def get_my_cols(db_user, db_password, db_host, db_port, db_name):
  names = []
  try:
    connect = MySQLdb.connect(host=db_host, port=db_port, user=db_user,
                              passwd=db_password, db=db_name)
    cursor = connect.cursor()
    cursor.execute('SHOW GLOBAL STATUS')
    for row in cursor.fetchall():
      if len(row) == 2:
        try:
          v = float(row[1])
          names.append(row[0])
        except ValueError, e:
          pass
    connect.close()
    return names
  except MySQLdb.Error, e:
    print 'SHOW GLOBAL STATUS fails (%s)' % e
    return []

def parse_args(arg, counters, expanded_args, devices, fio_devices, micron_devices):
  # print 'parse_args(%s)' % arg
  parts = arg.split('.')
  pix = 0
  pend = len(parts)
  use_agg = False
  expand = False
  ignore = False

  if parts[pix] in agg_funcs:
    pix += 1
    use_agg = True

  while pix != pend:
    if parts[pix] == 'timer':
      pix += 1
      assert pix == pend
    elif parts[pix] == 'timestamp':
      pix += 1
      assert pix == pend
    elif parts[pix] == 'counter':
      pix += 1
      assert pix == pend
    elif parts[pix] == 'vmstat':
      assert pend - pix >= 2
      assert parts[pix+1] in vmstat_cols
      counters['vmstat'] += 1
      pix += 2
    elif parts[pix] == 'iostat':
      assert pend - pix >= 3
      assert parts[pix+2] in iostat_cols

      if parts[pix+1][-1] == '*':
        assert pix + 3 == pend
        if use_agg:
          assert pix == 1
          expand = True
        else:
          assert pix == 0
          expand = True
      else:
        if parts[pix+1] in devices:
          counters['iostat'] += 1
        else:
          ignore = True
      pix += 3
    elif parts[pix] == 'fio':
      assert pend - pix >= 3
      assert parts[pix+2] in fio_cols
      counters['fio'] += 1
      pix += 3
    elif parts[pix] == 'micron':
      assert pend - pix >= 3
      assert parts[pix+2] in micron_cols
      counters['micron'] += 1
      pix += 3
    elif parts[pix] == 'my':
      assert parts[pix+1] == 'status'
      assert pend - pix >= 3
      counters['my.status'] += 1
      pix += 3
    elif parts[pix] == 'du':
      assert pend - pix == 2
      assert parts[pix+1] in du_cols
      pix += 2
    elif parts[pix] == 'diskstats':
      assert pend - pix == 3
      assert parts[pix+2] in diskstats_cols
      counters['diskstats'] += 1
      pix += 3
    else:
      # print 'pix %d, pend %d, parts :: %s' % (pix, pend, parts)
      assert False

  if expand:
    if use_agg:
      new_parts = [parts[0]]
      matched = get_matched_devices(parts[2], devices)
      if matched:
        counters['iostat'] += len(matched)
        for m in matched:
          new_parts.extend([parts[1], m, parts[3]])
        expanded_args.append('.'.join(new_parts))
    else:
      matched = get_matched_devices(parts[1], devices)
      if matched:
        counters['iostat'] += len(matched)
        for m in matched:
          expanded_args.append('.'.join([parts[0], m, parts[2]]))
  elif not ignore:
    expanded_args.append(arg)
  else:
    print 'Ignoring %s' % arg

def make_data_inputs(arg, inputs, counters, interval, db_user,
                     db_password, db_host, db_port, db_name, db_retries,
                     tee_vmstat, tee_iostat, tee_fio, tee_micron, tee_mystat, tee_diskstats):
  parts = arg.split('.')
  pix = 0
  pend = len(parts)
  use_agg = None

  if parts[pix] in agg_funcs:
    use_agg = parts[pix]
    pix += 1

  sources = []
  while pix != pend:
    if parts[pix] == 'timer':
      sources.append((arg, Counter(interval)))
      pix += 1
    elif parts[pix] == 'timestamp':
      sources.append((arg, Timestamper()))
      pix += 1
    elif parts[pix] == 'counter':
      sources.append((arg, Counter(1)))
      pix += 1
    elif parts[pix] == 'vmstat':
      sources.append((arg, Project(vmstat_cols[parts[pix+1]],
                                   tee_vmstat[counters['vmstat']])))
      counters['vmstat'] += 1
      pix += 2
    elif parts[pix] == 'iostat':
      f = FilterEquals(0, parts[pix+1], tee_iostat[counters['iostat']], True)
      sources.append((arg, Project(iostat_cols[parts[pix+2]], f)))
      counters['iostat'] += 1
      pix += 3
    elif parts[pix] == 'fio':
      f = FilterEquals(0, parts[pix+1], tee_fio[counters['fio']], True)
      sources.append((arg, Project(fio_cols[parts[pix+2]], f)))
      counters['fio'] += 1
      pix += 3
    elif parts[pix] == 'micron':
      f = FilterEquals(0, parts[pix+1], tee_micron[counters['micron']], True)
      sources.append((arg, Project(micron_cols[parts[pix+2]], f)))
      counters['micron'] += 1
      pix += 3
    elif parts[pix] == 'my':
      assert parts[pix+1] == 'status'
      # print 'use my.status tee %d for %s' % (counters['my.status'], arg)
      f = FilterEquals(0, parts[pix+2], tee_mystat[counters['my.status']], True)
      sources.append((arg, Project(1, f)))
      counters['my.status'] += 1
      pix += 3
    elif parts[pix] == 'du':
      sources.append((arg, DiskSizer(parts[pix+1])))
      pix += 2
    elif parts[pix] == 'diskstats':
      f = FilterEquals(2, parts[pix+1], tee_diskstats[counters['diskstats']], True)
      sources.append((arg, Project(diskstats_cols[parts[pix+2]], f)))
      counters['diskstats'] += 1
      pix += 3
    else:
      assert False

  if use_agg is None:
    assert len(sources) == 1
    inputs.append(sources[0])
  elif use_agg == 'rate':
    assert len(sources) == 1
    inputs.append((arg, ExprAbsToRel(interval, sources[0][1])))
  elif use_agg == 'sum':
    assert len(sources) >= 1
    inputs.append((arg, ExprFunc(sum, [s[1] for s in sources])))
  elif use_agg == 'ratesum':
    assert len(sources) >= 1
    sum_iter = ExprFunc(sum, [s[1] for s in sources])
    inputs.append((arg, ExprAbsToRel(interval, sum_iter)))
  elif use_agg == 'avg':
    assert len(sources) >= 1
    inputs.append((arg, ExprAvg([s[1] for s in sources])))
  elif use_agg == 'max':
    assert len(sources) >= 1
    inputs.append((arg, ExprFunc(max, [s[1] for s in sources])))
  else:
    assert False

def build_inputs(args, interval, loops, db_user, db_password, db_host, db_port,
                 db_name, db_retries, data_sources):
  scan_vmstat = None
  scan_iostat = None
  scan_fio = None
  scan_micron = None
  scan_diskstats = None
  inputs = []
  devices = iostat_get_devices()
  fio_devices = fio_get_devices()
  micron_devices = micron_get_devices()
  parse_counters = { 'iostat' : 0, 'vmstat' : 0, 'fio' : 0, 'micron': 0, 'my.status' : 0, 'du' : 0, 'diskstats' : 0 }

  if data_sources:
    f = open(data_sources)
    args.extend([l[:-1] for l in f.xreadlines()])

  expanded_args = []

  for arg in ['timestamp', 'timer', 'counter']:
    parse_args(arg, parse_counters, expanded_args, devices, fio_devices, micron_devices)

  for dev in devices:
   for col in iostat_cols:
     parse_args('iostat.%s.%s' % (dev, col), parse_counters, expanded_args, devices, fio_devices, micron_devices)
     parse_args('rate.iostat.%s.%s' % (dev, col), parse_counters, expanded_args, devices, fio_devices, micron_devices)

  for col in vmstat_cols:
    parse_args('vmstat.%s' % col, parse_counters, expanded_args, devices, fio_devices, micron_devices)
    parse_args('rate.vmstat.%s' % col, parse_counters, expanded_args, devices, fio_devices, micron_devices)

  for dev in fio_devices:
    for col in fio_cols:
      parse_args('fio.%s.%s' % (dev, col), parse_counters, expanded_args, devices, fio_devices, micron_devices)
      parse_args('rate.fio.%s.%s' % (dev, col), parse_counters, expanded_args, devices, fio_devices, micron_devices)
    
  for dev in micron_devices:
    for col in micron_cols:
      parse_args('micron.%s.%s' % (dev, col), parse_counters, expanded_args, devices, fio_devices, micron_devices)
      #parse_args('rate.micron.%s.%s' % (dev, col), parse_counters, expanded_args, devices, fio_devices, micron_devices)

  for col in get_my_cols(db_user, db_password, db_host, db_port, db_name):
    parse_args('my.status.%s' % col, parse_counters, expanded_args, devices, fio_devices, micron_devices)
    parse_args('rate.my.status.%s' % col, parse_counters, expanded_args, devices, fio_devices, micron_devices)

  for col in du_cols:
    parse_args('du.%s' % col, parse_counters, expanded_args, devices, fio_devices, micron_devices)

  for col in diskstats_cols:
    parse_args('diskstats.%s.%s' % (dev,col), parse_counters, expanded_args, devices, fio_devices, micron_devices)

  for arg in args:
    parse_args(arg, parse_counters, expanded_args, devices, fio_devices, micron_devices)

  tee_vmstat, tee_iostat, tee_fio, tee_micron, tee_mystat, tee_diskstats = None, None, None, None, None, None

  if parse_counters['vmstat']:
    scan_vmstat = ScanFork('vmstat -n %d %d' % (interval, loops+1), 2)
    tee_vmstat = itertools.tee(scan_vmstat, parse_counters['vmstat'])

  if parse_counters['iostat']:
    scan_iostat = ScanFork('iostat -kx %d %d' % (interval, loops+1), 0)
    tee_iostat = itertools.tee(scan_iostat, parse_counters['iostat'])

  if parse_counters['fio']:
    scan_fio = ScanFio(fio_devices)
    tee_fio = itertools.tee(scan_fio, parse_counters['fio'])

  if parse_counters['micron']:
    scan_micron = ScanMicron(micron_devices)
    tee_micron = itertools.tee(scan_micron, parse_counters['micron'])

  if parse_counters['my.status']:
    scan_mystat = ScanMysql(db_user, db_password, db_host, db_port, db_name,
                            'SHOW GLOBAL STATUS', db_retries, 'Foo 0')
    tee_mystat = itertools.tee(scan_mystat, parse_counters['my.status'])

  if parse_counters['diskstats']:
    scan_diskstats = ScanDiskstats()
    tee_diskstats = itertools.tee(scan_diskstats, parse_counters['diskstats'])

  # print expanded_args

  source_counters = { 'iostat' : 0, 'vmstat' : 0, 'fio' : 0, 'micron' : 0,
                      'my.status' : 0, 'diskstats' : 0 }
  for arg in expanded_args:
    make_data_inputs(arg, inputs, source_counters, interval, db_user,
                     db_password, db_host, db_port, db_name, db_retries,
                     tee_vmstat, tee_iostat, tee_fio, tee_micron, tee_mystat, tee_diskstats)

  return inputs

def parse_opts(args):
  parser = optparse.OptionParser()
  parser.add_option("--db_user", action="store",
                    type="string",dest="db_user",
                    default="root",
                    help="Username for database")
  parser.add_option("--db_password", action="store",
                    type="string",dest="db_password",
                    default="",
                    help="Password for database")
  parser.add_option("--db_host", action="store",
                    type="string",dest="db_host",
                    default="localhost",
                    help="Hostname for database")
  parser.add_option("--db_port", action="store",
                    type="int",dest="db_port",
                    default="0",
                    help="Port for database")
  parser.add_option("--db_name", action="store",
                    type="string",dest="db_name",
                    default="test",
                    help="Database name")
  parser.add_option("--db_retries", action="store",
                    type="int",dest="db_retries",
                    default="3",
                    help="Number of times to retry failed queries")
  parser.add_option("--sources", action="store",
                    type="string",dest="data_sources",
                    default="",
                    help="File that lists data sources to plot in addition"
                    "to those listed on the command line")
  parser.add_option("--interval", action="store",
                    type="int", dest="interval",
                    default="60",
                    help="Report every interval seconds")
  parser.add_option("--loops", action="store",
                    type="int", dest="loops",
                    default="2880000",
                    help="Stop after this number of intervals")
  return parser.parse_args(args)

def main(argv=None):
  if argv is None:
    argv = sys.argv
  options, args = parse_opts(argv[1:])

  inputs = build_inputs(args, options.interval, options.loops,
                        options.db_user, options.db_password,
                        options.db_host, options.db_port,
                        options.db_name, options.db_retries,
                        options.data_sources)
  for i,v in enumerate(inputs):
    print i+1, v[0]

  print 'START'

  iters = [iter(i[1]) for i in inputs]
  try:
    for x in xrange(options.loops):
      print ' '.join([i.next() for i in iters])
      sys.stdout.flush()
      time.sleep(options.interval)
  except StopIteration, e:
    pass

if __name__ == "__main__":
    sys.exit(main())
