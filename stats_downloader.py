from scanner_v2 import SikluCommandParserBase, SikluUnit, SikluCommandParam
import datetime
from db_wrapper import *
import os, sys, pexpect, time
from subprocess import call
import re
import pandas as pd
import collections
import sys
from multiprocessing import Pool
try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser  # ver. < 3.0


ETH_STATS_TABLE_NAME = 'eth_stats_table'
RF_STATS_TABLE_NAME = 'rf_stats_table'

db_engine = local_db().engine


class ShowEthStatisticsSummary(SikluCommandParserBase):
    multiline = True
    columns = ['interval', 'start_ts', 'interface', 'in-octets', 'out-octets', 'in-rate', 'out-rate', 'util']

    def __init__(self, connection = None, eth = 'eth1'):
        self.cmd = 'show eth %s statistics-summary' % eth
        SikluCommandParserBase.__init__(self, connection)
        self.cmd_params = [item for sublist in [self.gen_cmd_params(time_interval) for time_interval in range(0, 96)] for item in sublist]

    def gen_cmd_params(self, time_interval):
        return [
                SikluCommandParam('record_%d' % time_interval, '', r"^(%d\s+[\.\d]+\s+[:\d]+\s+eth\d\s+[\d]+\s+[\d]+\s+[\d]+\s+[\d]+\s+[\d]+)" % time_interval),
                ]

    def parse_reply(self):
        stats = []
        # print self.reply
        for param in self.cmd_params:
            interval_values = self.find_value(param.regex, self.reply)
            # print param.regex
            # import pdb; pdb.set_trace()
            if interval_values:
                r = re.search(r"(\d+)\s+([\.\d]+\s+[:\d]+)\s+(eth[\d])\s+([\d]+)\s+([\d]+)\s+([\d]+)\s+([\d]+)\s+([\d]+)", interval_values)
                if r:
                    r = r.groups()
                    stats.append([int(r[0]), datetime.datetime.strptime(r[1], '%Y.%m.%d %H:%M:%S'), r[2], int(r[3]), int(r[4]), int(r[5]), int(r[6]), int(r[7])])
                else:
                    continue

        return pd.DataFrame(stats, columns=self.columns)

    def __str__(self):
        return ','.join(columns)

class ShowRfStatisticsSummary(SikluCommandParserBase):
    multiline = True
    columns = ['interval', 'start_ts', 'min-rssi', 'max-rssi', 'min-cinr', 'max-cinr', 'min-mod', 'max-mod']

    def __init__(self, connection = None):
        self.cmd = 'show rf statistics-summary'
        SikluCommandParserBase.__init__(self, connection)
        self.cmd_params = [item for sublist in [self.gen_cmd_params(time_interval) for time_interval in range(0, 96)] for item in sublist]

    def gen_cmd_params(self, time_interval):
        return [
                SikluCommandParam('record_%d' % time_interval, '', r"^(%d\s+[\.\d]+\s+[:\d]+\s+[-\d]+\s+[-\d]+\s+[-\d]+\s+[-\d]+\s+[\w\d]+\s+[\w\d]+\s+[yesno]+)" % time_interval),
                ]

    def parse_reply(self):
        stats = []
        for param in self.cmd_params:
            interval_values = self.find_value(param.regex, self.reply)
            # import pdb; pdb.set_trace()
            if interval_values:
                r = re.search(r"(\d+)\s+([\.\d]+\s+[:\d]+)\s+([-\d]+)\s+([-\d]+)\s+([-\d]+)\s+([-\d]+)\s+([\w\d]+)\s+([\w\d]+)", interval_values)
                if r:
                    r = r.groups()
                    stats.append([int(r[0]), datetime.datetime.strptime(r[1], '%Y.%m.%d %H:%M:%S'), int(r[2]), int(r[3]), int(r[4]), int(r[5]), r[6], r[7]])
                else:
                    continue

        return pd.DataFrame(stats, columns=self.columns)

    def __str__(self):
        return ','.join(columns)


def run_command(unit_):
    unit = unit_['unit']
    # db_engine = unit_['db_engine']

    eth_stats = pd.DataFrame([])
    rf_stats = pd.DataFrame([])

    unit.connect()
    if unit.connected:
        # command = ShowEthStatisticsSummary(unit, 'eth1')
        # db_table = ETH_STATS_TABLE_NAME
        eth_stats = ShowEthStatisticsSummary(unit, 'eth1').parse()
        eth_stats['host'] = unit.host
        # command = ShowRfStatisticsSummary(unit)
        # db_table = RF_STATS_TABLE_NAME
        rf_stats = ShowRfStatisticsSummary(unit).parse()
        rf_stats['host'] = unit.host

    return (eth_stats, rf_stats)

def get_stats(command):
    # ts = time.strftime('%d-%m-%Y_%H-%M-%S', time.localtime())
    return command.parse()


def units_manager_parallel(cfg_file, n_processes):
    hosts = pd.read_csv(cfg_file, comment='#')
    units = []

    for i, host in hosts.iterrows():
        unit = SikluUnit(host['ip'], host['user'], host['password'])
        units.append({'unit':unit, 'command':host['command']})

    # for unit in units:
    #     run_command(unit)
    pool = Pool(processes=n_processes)
    (eth_stats, rf_stats) = zip(*pool.map(run_command, units))
    # res = pool.map(run_command, units)

    # import pdb; pdb.set_trace()
    print('Updating db...')
    db_table = ETH_STATS_TABLE_NAME
    eth_stats_ = pd.concat([eth_stat for eth_stat in eth_stats if ~eth_stat.empty])
    eth_stats_.to_sql(db_table, db_engine, if_exists='append', index=False)

    db_table = RF_STATS_TABLE_NAME
    rf_stats_ = pd.concat([rf_stat for rf_stat in rf_stats if ~rf_stat.empty])
    rf_stats_.to_sql(db_table, db_engine, if_exists='append', index=False)
    print('Done...')

if __name__ == '__main__':
    # scan
    # copy
    # run
    # accept

    config = ConfigParser()

    if len(sys.argv) == 2:
        config.read(sys.argv[1])
    else:
        config.add_section('DEFAULT')
        config.set('DEFAULT', 'RINGS', 0)
        config.set('DEFAULT', 'MH_ENABLED', False)
        config.set('DEFAULT', 'N_PROCESSES', 10)
        config.set('DEFAULT', 'CONNECTION_TIMEOUT_SEC', 12)
        config.set('DEFAULT', 'CSV_FILENAME', 'cfg.csv')


    RINGS = config.getint('DEFAULT', 'RINGS')
    MH_ENABLED = config.getboolean('DEFAULT', 'MH_ENABLED')
    N_PROCESSES = config.getint('DEFAULT', 'N_PROCESSES')
    CONNECTION_TIMEOUT_SEC = config.getint('DEFAULT', 'CONNECTION_TIMEOUT_SEC')
    CSV_FILENAME = config.get('DEFAULT', 'CSV_FILENAME')

    units_manager_parallel(CSV_FILENAME, N_PROCESSES)



