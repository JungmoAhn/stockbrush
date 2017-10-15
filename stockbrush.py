# -*- coding:utf-8 -*

import requests
import unicodedata
from bs4 import BeautifulSoup
import pandas as pd
from pandas.compat import(
    StringIO, bytes_to_str, range, lmap, zip
)
from io import StringIO
import io
import csv
import sys
import locale
from pandas import DataFrame
from pandas import Series
import bs4
from datetime import date
import datetime
from collections import deque
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
import statistics as stats
import math
import copy
import os
import glob
import glob
from dominate import document
from dominate.tags import *
import matplotlib.pyplot as plt
import matplotlib
sys.path.append('Users/jman/work/instracker')
from pandas_dbms import write_frame, read_db
import sqlite3
from google_stock_data import GoogleStockData
from naver_stock_data import NaverStockData
from collections import Iterable

'''
a dataframe_kosdaq
[index] code  info last_close_period  last_inst_deal_period_volume  max_close_period max_inst_deal_period_volume       name  
               DF
               DF
               |
              | | dataframes in a series
[index by date] change  close  date deal_volume foreign_deal_volume foreign_volume foreign_volume_rate inst_deal_volume
'''

printlen = int()
def overwrite(string):
    import sys
    backspaces = ''
    printlen = len(string)
    for x in range(printlen):
        backspaces += '\b'    
    sys.stdout.write(backspaces+string)
    sys.stdout.flush()

class StockBrush :
    deal_volume_file = 'deal_volume_file'

    def __init__(self, nosave_bypass_latest=0) :
        self.url = str()
        self.inst_tickets = list()
        self.max_tickets = list()
        self.max_week_tickets = list()
        self.new_lines = list()
        self.new_max_tickets = set()
        self.new_inst_tickets = set()
        self.weeks = 0
        self.exchanges = dict()
        symbols = list()
        symbols_name = list()

        self.kosdaq_tickets = pd.DataFrame({'code': [], 'info':[], 'max_inst_deal_period_volume':[],
                                            'last_inst_deal_period_volume':[]})        
        self.sql_table = pd.DataFrame({'code': [], 'price': [], 'stocks': [], 'week_close': []})

        gsd_krx = GoogleStockData('KRX')
        krx = gsd_krx.get_screener_data()
        self.exchanges.update({'krx':krx.dropna().copy()})
        gsd_kosdaq = GoogleStockData('KOSDAQ')
        kosdaq = gsd_kosdaq.get_screener_data()
        self.exchanges.update({'kosdaq':kosdaq.dropna().copy()})
        
        for key, exchange in self.exchanges.items() :
#            self.exchanges[key]['MarketCap'].dropna('0', inplace=True)            
            self.exchanges[key]['MarketCap'] = exchange['MarketCap'].str.replace('T', '0.000.000.000', case=False)
            self.exchanges[key]['MarketCap'] = exchange['MarketCap'].str.replace('B', '0.000.000', case=False)
            self.exchanges[key]['MarketCap'] = exchange['MarketCap'].str.replace('M', '0.000', case=False)
            self.exchanges[key]['MarketCap'] = exchange['MarketCap'].str.replace('\.', '', case=False)
            self.exchanges[key]['MarketCap'] = exchange['MarketCap'].astype(int)
            self.exchanges[key]['Volume'] = exchange['Volume'].str.replace('T', '0,000,000,000.00', case=False)
            self.exchanges[key]['Volume'] = exchange['Volume'].str.replace('B', '0,000,000.00', case=False)
            self.exchanges[key]['Volume'] = exchange['Volume'].str.replace('M', '0,000.00', case=False)
            self.exchanges[key]['Volume'] = exchange['Volume'].str.replace('\.00', '', case=False)
            self.exchanges[key]['Volume'] = exchange['Volume'].str.replace('\.', '', case=False)
            self.exchanges[key]['Volume'] = exchange['Volume'].str.replace('\,', '', case=False)
            self.exchanges[key]['Volume'] = exchange['Volume'].astype(int)
            self.exchanges[key]['QuoteLast'] = exchange['QuoteLast'].str.replace('T', '0,000,000,000.00', case=False)
            self.exchanges[key]['QuoteLast'] = exchange['QuoteLast'].str.replace('B', '0,000,000.00', case=False)
            self.exchanges[key]['QuoteLast'] = exchange['QuoteLast'].str.replace('M', '0,000.00', case=False)            
            self.exchanges[key]['QuoteLast'] = exchange['QuoteLast'].str.replace('\.00', '', case=False)
            self.exchanges[key]['QuoteLast'] = exchange['QuoteLast'].str.replace('\.', '', case=False)            
            self.exchanges[key]['QuoteLast'] = exchange['QuoteLast'].str.replace(',', '', case=False)
            self.exchanges[key]['QuoteLast'] = exchange['QuoteLast'].astype(int)
            self.exchanges[key]['overwall'] = 0
            self.exchanges[key]['turnaround'] = 0
            self.exchanges[key]['strait_markup_weeks'] = 0
            self.exchanges[key]['strait_markdown_weeks'] = 0
            self.exchanges[key]['weeks'] = 0
            self.exchanges[key]['week_close'] = 0
            self.exchanges[key]['max_inst_deal_volume'] = 0
            self.exchanges[key]['last_inst_deal_volume'] = 0
            self.exchanges[key]['trade_info'] = [[] for key in exchange.iterrows()]            
            self.exchanges[key]['shares'] = [int(symbol['MarketCap'] / symbol['QuoteLast']) for idx, symbol in exchange.iterrows()]
 
        for key, exchange in self.exchanges.items() :
            nsd = NaverStockData(exchange)
            self.exchanges[key]['trade_info'] = nsd.get_trade_info(can_bypass=nosave_bypass_latest)

    def get_inst_info(self, period=3, skip=0) :
        for key, exchange in self.exchanges.items() :
            max_inst_deal_volumes = list()
            last_inst_deal_volumes = list()
            
            for idx, symbol in exchange.iterrows() :
                try :
                    period_volumes = deque()
                    skip_inst = skip
                    for inst_deal_volume in symbol['trade_info']['inst_deal_volume'] :
                        skip_inst = skip_inst - 1
                        if skip_inst > 0 :
                            continue
                        if len(period_volumes) < period :
                            period_volumes.append(inst_deal_volume)
                            continue
                        period_volumes.popleft()
                        period_volumes.append(inst_deal_volume)
                    
                        if symbol['max_inst_deal_volume'] < sum(period_volumes) :
                            symbol['max_inst_deal_volume'] = sum(period_volumes)

                    max_inst_deal_volumes.append(symbol['max_inst_deal_volume'])
                    last_inst_deal_volumes.append(sum(period_volumes))
                
                    del period_volumes
                except Exception as e :
                    del period_volumes
                    last_inst_deal_volumes.append(0)                
                    max_inst_deal_volumes.append(0)
                    
            exchange['max_inst_deal_volume'] = max_inst_deal_volumes
            exchange['last_inst_deal_volume'] = last_inst_deal_volumes

    def get_week_info(self) :
        print('getting week informations..')
        for key, exchange in self.exchanges.items() :
            weekclose_list = []
            weeks_list = []
            turnaround_list = []
            overwall_list = []            
            strait_markup_weeks_list = []
            strait_markdown_weeks_list = []

            for idx, trade_info in exchange['trade_info'].iteritems() :
                weekclose = []
                last_week_close = 0
                turnaround = False
                overwall = False                
                strait_markup_weeks = 0
                strait_markdown_weeks = 0
                
                try :
                    last_weekday = trade_info['close'].index[0].weekday()
                    last_close = trade_info['close'][0]
                except Exception as e :
                    weekclose_list.append(None)
                    weeks_list.append(0)
                    turnaround_list.append(0)
                    overwall_list.append(0)
                    strait_markdown_weeks_list.append(0)
                    strait_markup_weeks_list.append(0)
                    continue

                for close_idx, close in enumerate(trade_info['close']) :
                    if last_weekday > trade_info['close'].index[close_idx].weekday() :
                        turnaround = False

                        if last_close > last_week_close :
                            strait_markup_weeks = strait_markup_weeks + 1
                            if strait_markdown_weeks > 4:
                                turnaround = True
                            strait_markdown_weeks = 0
                        elif last_close < last_week_close :
                            strait_markdown_weeks = strait_markdown_weeks + 1
                            strait_markup_weeks = 0
                        else :
                            strait_markup_weeks = 0
                            srait_markdown_weeks = 0
                        weekclose.append(last_close)
                        last_week_close = last_close

                    last_weekday = trade_info['close'].index[close_idx].weekday()
                    last_close = close
                max_week_close = max(weekclose)
                weeks = len(weekclose)

                if max_week_close < last_week_close :
                    overwall = True

                weekclose_list.append(weekclose)
                weeks_list.append(weeks)
                turnaround_list.append(turnaround)
                overwall_list.append(overwall)                
                strait_markup_weeks_list.append(strait_markup_weeks)
                strait_markdown_weeks_list.append(strait_markdown_weeks)
            exchange['week_close'] = weekclose_list
            exchange['weeks'] = weeks_list            
            exchange['turnaround'] = turnaround_list
            exchange['overwall'] = overwall_list            
            exchange['strait_markup_weeks'] = strait_markup_weeks_list
            exchange['strait_markdown_weeks'] = strait_markdown_weeks_list

    def get_info(self) :
        self.get_week_info()
        self.get_inst_info()

    def make_sql_table(self, name='info') :
        self.sql_table['code'] = self.kosdaq['code']
        self.sql_table['week_close'] = self.kosdaq['week_close']
        self.sql_table['price'] = self.kosdaq['price']
        self.sql_table['stocks'] = self.kosdaq['stocks']

        with sqlite3.connect('instrack.db', detect_types=sqlite3.PARSE_DECLTYPES) as conn:
            write_frame(self.sql_table, name, con=conn, flavor='sqlite', if_exists='replace')

    def load_sql_table(self, name='kosdaq') :
        with sqlite3.connect('instrack.db', detect_types=sqlite3.PARSE_DECLTYPES) as conn:        
            self.sql_table = read_db('select * from '+name, con=conn)    

if  __name__ == '__main__' :
    sb = StockBrush()
    sb.get_info()
#    sb.get_week_info()
#    sb.get_inst_info()
#    paint.get_info()
#    sb.load_info()
#    inst.get_week_close()
#    inst.get_stocks_info(from_year=2016, from_month=3)
#    inst.make_sql_table()
#    inst.load_sql_table()



