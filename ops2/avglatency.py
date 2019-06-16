#!/usr/bin/python
import re
import os
import time
import getopt
import sys


class AvgHist:
    ns = ""
    hist = ""
    timestamp = ""
    ms = 0
    max_ms = 0
    items = 0
    _latencies = {}
    latencies = []
    prev_hist = {}
    all_items = 0
    result = []
    first_histogram = {}

    def __init__(self, filename, callbackfunc=None, namespace=None, histogram=None, percentile=None, ignore_first=False):
        self.callbackFunc = callbackfunc
        self.fileName = filename
        self.match_ns = namespace
        self.match_hist = histogram
        if ignore_first is not None:
            self.ignore_first = ignore_first
        else:
            self.ignore_first = False
        if percentile is not None:
            self.max_percent = float(percentile)
        else:
            self.max_percent = 100

    def tail(self):
        self.latencies = []
        stat = os.stat(self.fileName)
        mtime = stat.st_mtime
        size = stat.st_size
        try:
            while True:
                time.sleep(0.5)
                stat = os.stat(self.fileName)
                if mtime < stat.st_mtime:
                    mtime = stat.st_mtime
                    with open(self.fileName, "r") as ff:
                        ff.seek(size)
                        for line in ff:
                            self._do_work(str(line))
                    size = stat.st_size
        except KeyboardInterrupt:
            self._final_finish()
            return self.latencies

    def run(self):
        self.latencies = []
        with open(self.fileName) as ff:
            for line in ff:
                self._do_work(line)
            self._final_finish()
            return self.latencies

    def _divide_ms_items(self, ms, items):
        if items > 0:
            return ms / items
        else:
            return 0

    def _final_finish(self):
        self._hist_finished()
        for hist, nsvalue in self._latencies.items():
            for ns, value in nsvalue.items():
                self.latencies.append(
                    {'histogram': hist, 'namespace': ns, 'average': self._divide_ms_items(value['ms'], value['items']), 'max_ms': value['max_ms']})

    def _do_work(self, line):
                if "<><><><>" in line:
                    self.prev_hist = {}
                elif "hist.c:" in line:
                    if "histogram dump:" in line:
                        self._hist_finished()
                        if "histogram dump: {" in line:
                            result = re.search('histogram dump: {(.*)}-(.*) \(', line)
                            self.ns = result.group(1)
                            self.hist = result.group(2)
                        else:
                            result = re.search('histogram dump: (.*) \(', line)
                            self.ns = ""
                            self.hist = result.group(1)
                        self.timestamp = line[0:28]
                    else:
                        if (self.match_ns == self.ns or self.match_ns is None) and \
                                (self.match_hist == self.hist or self.match_hist is None):
                            result = re.findall('\(([0-9]{2}): ([0-9]{10})\)', line)
                            self.result = self.result + result
                            for nItem in result:
                                try:
                                    self.all_items = self.all_items + int(nItem[1]) - \
                                                     self.prev_hist[self.hist][self.ns][2**int(nItem[0])]
                                except KeyError:
                                    self.all_items = self.all_items + int(nItem[1])
                else:
                    self._hist_finished()

    def _hist_finished(self):
        if self.hist != "" and (self.match_ns == self.ns or self.match_ns is None) and \
                (self.match_hist == self.hist or self.match_hist is None):
            nquit = False
            self.all_items = self.all_items / 100 * self.max_percent
            for nItem in self.result:
                try:
                    self.prev_hist[self.hist]
                except KeyError:
                    self.prev_hist[self.hist] = {}
                try:
                    self.prev_hist[self.hist][self.ns]
                except KeyError:
                    self.prev_hist[self.hist][self.ns] = {}
                try:
                    items = int(nItem[1]) - self.prev_hist[self.hist][self.ns][2 ** int(nItem[0])]
                except KeyError:
                    items = int(nItem[1])
                if self.all_items - items < 0:
                    items = self.all_items
                    nquit = True
                self.all_items = self.all_items - items
                if items > 0:
                    self.max_ms = 2 ** int(nItem[0])
                self.prev_hist[self.hist][self.ns][2 ** int(nItem[0])] = int(nItem[1])
                self.items = self.items + items
                self.ms = self.ms + (2 ** int(nItem[0])) * items
                if self.max_percent == 100 and nquit is True:
                    break
            process = True
            if self.ignore_first is True:
                try:
                    self.first_histogram[self.hist][self.ns]
                except KeyError:
                    process = False
                    try:
                        self.first_histogram[self.hist][self.ns] = 0
                    except KeyError:
                        self.first_histogram[self.hist] = {self.ns: 0}
            if process is True:
                try:
                    self._latencies[self.hist][self.ns]['ms'] = self._latencies[self.hist][self.ns]['ms'] + self.ms
                    self._latencies[self.hist][self.ns]['items'] = self._latencies[self.hist][self.ns]['items'] + self.items
                except KeyError:
                    try:
                        self._latencies[self.hist][self.ns] = {'ms': self.ms, 'items': self.items}
                    except KeyError:
                        self._latencies[self.hist] = {self.ns: {'ms': self.ms, 'items': self.items}}
                try:
                    if self._latencies[self.hist][self.ns]['max_ms'] < self.max_ms:
                        self._latencies[self.hist][self.ns]['max_ms'] = self.max_ms
                except KeyError:
                    self._latencies[self.hist][self.ns]['max_ms'] = self.max_ms
                if callable(self.callbackFunc):
                    self.callbackFunc(self.ns, self.hist, self.items, self._divide_ms_items(self.ms, self.items), self.timestamp, self.max_ms)
            self.hist = ""
            self.ms = 0
            self.max_ms = 0
            self.items = 0
            self.result = []
            self.all_items = 0

# =========================
# ----- USAGE EXAMPLE -----
# =========================


detail_format = "{0: <30} {1: <20} {2: <20} {3: <14} {4: <15} {5: <20}"


# callback function for the class - this will print as the file is being processed
def avg_callback(namespace, histogram, items, average, timestamp, max_ms):
    data = [timestamp, namespace, histogram, int(items), "%s ms" % int(average), "%s < x < %s" % (int(max_ms / 2), max_ms)]
    print(detail_format.format(*data))


def print_summary(latency):
    if latency['namespace'] != "":
        print("{%s}-%s: average < %d ms ; max transaction range: %d ms < x < %d ms" % (
        latency['namespace'], latency['histogram'], latency['average'], latency['max_ms'] / 2, latency['max_ms']))
    else:
        print("%s: average < %d ms ; max transaction range: %dms < x < %dms" % (
        latency['histogram'], latency['average'], latency['max_ms'] / 2, latency['max_ms']))


usage = "\nUsage: %s [-n|--namespace=] [-o|--histogram=] [-p|--percentile=] [-r|--run] [-t|--tail] [-d|--no-detail] [-s|--no-summary] [-f|--file=] [-h|--help]" % sys.argv[0]
usage = "%s\n\n\tn | namespace\t\tif set, will only show histograms for that namespace" % usage
usage = "%s\n\to | histogram\t\tif set, will only show that histogram" % usage
usage = "%s\n\tp | percentile\t\tif set, will discard slowest transactions, only count up to this percent of total" % usage
usage = "%s\n\tr | run\t\t\tif set, will run through the whole file. If run and tail are not set, run will execute by default" % usage
usage = "%s\n\tt | tail\t\tif set, will tail the file. If run and tail are not set, run will execute by default" % usage
usage = "%s\n\ti | ignore-first\tif set, ignore each first histogram for each namespace (since they are cumulative)" % usage
usage = "%s\n\td | no-detail\t\tif set, will not produce per histogram tick detail, only summary" % usage
usage = "%s\n\ts | no-summary\t\tif set, will not produce summary at the end of the run/tail" % usage
usage = "%s\n\tf | file\t\tif set, will process this file. Default: /var/log/aerospike/aerospike.log" % usage
usage = "%s\n\th | help\t\tthis help screen\n" % usage

# let's run it
if __name__ == "__main__":
    namespace = None
    histogram = None
    percentile = None
    run = False
    tail = False
    detail = avg_callback
    summary = True
    ignore_first = False
    file = "/var/log/aerospike/aerospike.log"
    try:
        opts, args = getopt.getopt(sys.argv[1:], "n:o:p:rtdsf:ih",
                                   ["namespace=", "histogram=", "percentile=", "run", "tail",
                                    "no-detail", "no-summary", "file=", "ignore-first", "help"])
    except getopt.GetoptError as err:
        print("Error parsing command line arguments: ", str(err))
        print(usage)
        sys.exit(2)
    for o, a in opts:
        if o in ("-i", "--ignore-first"):
            ignore_first = True
        elif o in ("-n", "--namespace"):
            namespace = a
        elif o in ("-o", "--histogram"):
            histogram = a
        elif o in ("-p", "--percentile"):
            percentile = a
        elif o in ("-f", "--file"):
            file = a
        elif o in ("-r", "--run"):
            run = True
        elif o in ("-t", "--tail"):
            tail = True
        elif o in ("-d", "--no-detail"):
            detail = None
        elif o in ("-s", "--no-summary"):
            summary = False
        elif o in ("-h", "--help"):
            print(usage)
            sys.exit(1)
        else:
            assert False, "unhandled option: %s" %o
    if run is False and tail is False:
        run = True
    if detail is not None:
        data = ["TIMESTAMP", "NAMESPACE", "HISTOGRAM", "TRANSACTIONS", "<AVERAGE", "MAX TRANS. RANGE(ms)"]
        print(detail_format.format(*data))
    myAvg = AvgHist(file, callbackfunc=detail, namespace=namespace, histogram=histogram, percentile=percentile, ignore_first=ignore_first)

    # run and print averages for the whole file once avgHist finishes
    if run is True:
        if summary is True:
            for latency in myAvg.run():
                print_summary(latency)
        else:
            myAvg.run()

    # keep processing the file with tail, print summaries on CTRL+C
    if tail is True:
        if summary is True:
            for latency in myAvg.tail():
                print_summary(latency)
        else:
            myAvg.tail()
