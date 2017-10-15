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
from google_screener_data_extract import GoogleStockDataExtract

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
    info_url = 'http://real-chart.finance.yahoo.com/table.csv?'
    symbol_url = 'http://marketdata.krx.co.kr/contents/MKD/04/0406/04060200/MKD04060200.jsp'
    deal_volume_file = 'deal_volume_file'

    def __init__(self) :
        self.url = str()
        self.inst_tickets = list()
        self.max_tickets = list()
        self.max_week_tickets = list()
        self.new_lines = list()
        self.new_max_tickets = set()
        self.new_inst_tickets = set()
        self.weeks = 0
        symbols = list()
        symbols_name = list()

        self.kosdaq = pd.DataFrame({'code': [], 'name': [], 'stocks': [], 'price': [],
                                    'info':[], 'max_close': [], 'max_inst_deal_period_volume':[],
                                    'last_inst_deal_period_volume':[], 'last_close_period':[],
                                    'week_close': [], 'weeks': [],
                                    'strait_markup_weeks': [], 'strait_markdown_weeks': [],
                                    'overwall': [], 'turnaround': [], 'turnon': []})
        self.kosdaq_tickets = pd.DataFrame({'code': [], 'info':[], 'max_inst_deal_period_volume':[],
                                            'last_inst_deal_period_volume':[]})        
        self.sql_table = pd.DataFrame({'code': [], 'price': [], 'stocks': [], 'week_close': []})
        self.info = pd.DataFrame({'date' :[], 
                                  'close' : [],   
                                  'change' : [],
                                  'deal_volume' : [],
                                  'inst_deal_volume' : [],
                                  'foreign_deal_volume' : [],
                                  'foreign_volume' : [],
                                  'foreign_volume_rate' : []})
        self.info.set_index('date', inplace=True)
        self.info_list = list()

        gsd = GoogleStockDataExtract()
        gsd.retrieve_all_stock_data()
        gsd.result_google_ext_df.to_csv(r'./gsd_result.csv', index =False)

        data_df = pd.read_csv('gsd_result.csv')

        print(data_df)
        code_list = []
        name_list = []
        stocks_list = []

        print(data_df)
        self.kosdaq['name'] = [symbol['GS_CompanyName'] for idx, symbol in data_df.iterrows()]
        self.kosdaq['code'] = [symbol['SYMBOL'] for idx, symbol in data_df.iterrows()]
        self.kosdaq['stocks'] = [symbol['SYMBOL'] for idx, symbol in data_df.iterrows()]                
        
            try :
                if isinstance(int(symbol['종목코드']), int)  and  len(symbol['종목코드']) == 6 :
                    code_list.append(symbol['종목코드'])
                    name_list.append(symbol['기업명'])
                    stocks_list.append(int(symbol['상장주식수(주)'].replace(',', '')))
            except Exception as e:
                print('cut out unknown symbol')
        
        self.kosdaq['code'] = code_list
        self.kosdaq['name'] = name_list
        self.kosdaq['stocks'] = stocks_list

        self.kosdaq['strait_markup_weeks'] = [0 for idx, symbol in self.kosdaq.iterrows()]
        self.kosdaq['strait_markdown_weeks'] = [0 for idx, symbol in self.kosdaq.iterrows()]
        self.kosdaq['turnon'] = [0 for idx, symbol in self.kosdaq.iterrows()]
        self.kosdaq['overwall'] = [0 for idx, symbol in self.kosdaq.iterrows()]
        self.kosdaq['weeks'] = [0 for idx, symbol in self.kosdaq.iterrows()]
        self.kosdaq['price'] = [0 for idx, symbol in self.kosdaq.iterrows()]        
        
    def _make_url(self, symbol, year_start='2014', year_end='2015', month_start='6', month_end='3', day_start='1', day_end='1', period='d') :
        self.url = self.info_url + 's=' + symbol +'.KQ' + '&a=' + month_start + '&b=' + day_start + '&c=' + year_start + '&d=' + month_end + '&e=' + day_end + '&f=' + year_end + '&g=' + period
        
    def _get_stock_info(self, index) :
        end_data = 0
        f = open(self.deal_volume_file, 'r')
        gigan_html = str()
        get_line = 0

        ''' get tables'''
        while True :
            line = f.readline()

            if line == '':
                break        

            result = line.find('h4 class="tlline2"')
            if result != -1 :
                get_line = 1

            if get_line == 1 :
                gigan_html = gigan_html + line        

            result = line.find('추정기관')
            if result != -1 :
                break

        gigan_html = gigan_html.replace('\t', '')
        gigan_html = gigan_html.replace('\n', '')

        soup = BeautifulSoup(gigan_html, 'lxml')
        trs = soup.find_all('tr')

        columns = []
        for i in range(0, 10) :
            columns.append([])

        for tr in trs :
            column_exist = 0
            tds = tr.find_all('td')

            for column in tds :
                if isinstance(column.string, type(None)) :
                    break
                column_exist = 1
                break

            if not column_exist :
                continue

            '''0:날짜 : date
            1:종가 : close
            2:전일비 : before_rate
            3:등락율 : change
            4:거래량 : deal_volume
            5:기관 순매매량 : inst_deal_volume
            6:외국인 순매매량 : foreign_deal_volume
            7:외국인 보유주수 : foreign_volume
            8:보유율 : foreign_volume_rate
            '''

            for idx, column in enumerate(tds) :
                if column.string == '\xa0' :
                    end_data = 1
                    break

                if idx == 0 :
                    try :
                        if self.kosdaq['info'][index]['date'].index[-1].strftime('%F') >= pd.to_datetime(column.string.replace('.', '-')).strftime('%F') :
                            f.close()
                            info = pd.DataFrame({'date' :columns[0], 
                                                 'close' : columns[1],   
                                                 'change' : columns[3],
                                                 'deal_volume' : columns[4],
                                                 'inst_deal_volume' : columns[5],
                                                 'foreign_deal_volume' : columns[6],
                                                 'foreign_volume' : columns[7],
                                                 'foreign_volume_rate' : columns[8]})
                            info.set_index('date', inplace=True)
                            info.sort_index()
                            self.info = self.info.append(info)
                            return 1
                    except Exception as e:
                        pass
#                        print('there are no dates')
                            
                    columns[idx].append(pd.to_datetime(column.string.replace('.', '-')))
                elif idx == 1 or idx == 4 or idx == 5 or idx == 6 or idx == 7 :
                    try :
                        columns[idx].append(int(column.string.replace(',', '')))
                    except Exception as e :
                        print(type(column.string))
                        if isinstance(column.string, type(None)) :
                            print('there is no symbol in kosdaq')
                            f.close()                            
                            return 3
                        else :
                            columns[idx].append(int(column.string))
                else :
                    columns[idx].append(column.string)

        info = pd.DataFrame({'date' :columns[0], 
                             'close' : columns[1],   
                             'change' : columns[3],
                             'deal_volume' : columns[4],
                             'inst_deal_volume' : columns[5],
                             'foreign_deal_volume' : columns[6],
                             'foreign_volume' : columns[7],
                             'foreign_volume_rate' : columns[8]})
        info.set_index('date', inplace=True)

        if not end_data :
            try :
                if columns[0][0] in self.info.index.tolist() :
                    end_data = 1
                else :
                    self.info = self.info.append(info)
            except Exception as e :
                ''' fault symbol'''
                print('fault')
                return 2

        f.close()
        if end_data :
            return 1
        else :
            return 0

    def get_stocks_info(self, from_day=1, from_month=1, from_year=2015, stocks=0) :
        info_list = [[] for idx, symbol in self.kosdaq.iterrows()]
        price_list = [0 for idx, symbol in self.kosdaq.iterrows()] 
        for idx, symbol in self.kosdaq.iterrows() :
            pagenum =  1
            if stocks != 0 :
                if idx >= stocks :
                    break

            overwrite('%d/%d...getting a symbol(%s)' % (idx, len(self.kosdaq), symbol['code']))

            self.info = pd.DataFrame({'date' :[], 
                                      'close' : [],   
                                      'change' : [],
                                      'deal_volume' : [],
                                      'inst_deal_volume' : [],
                                      'foreign_deal_volume' : [],
                                      'foreign_volume' : [],
                                      'foreign_volume_rate' : []})            
            
            while True :
                url = 'http://finance.naver.com/item/frgn.nhn?code=' + str(symbol['code']).zfill(6) + '&page=' + str(pagenum)

                try :
                    r = requests.get(url,timeout=5)
                except Exception as e :
                    print('try to get a request again')
                    continue
                with io.open(self.deal_volume_file, 'w') as f:
                    f.write(str(r.text))

                out_of_data = self._get_stock_info(idx)
                over_date = 0

                if not out_of_data :
                    if self.info['date'].index[-1].year < from_year :
                        over_date = 1
                    elif self.info['date'].index[-1].month < from_month :
                        over_date = 1
                    elif self.info['date'].index[-1].day < from_day :
                        over_date = 1

                if out_of_data or over_date :
                    break

                pagenum = pagenum + 1

            ''' there is the lastest informations'''
            if not self.info.empty :
                ''' there is no cached informations '''
                try :
                    if math.isnan(self.kosdaq['info'][idx]) :
                         del self.kosdaq['info'][idx]
                    info_list[idx] = self.info.copy()
                except Exception as e :
#                    symbol['info'] = symbolself.kosdaq['info'][idx].append(self.info)
                    info_list[idx] = symbol['info'].append(self.info)
                finally :
                    info_list[idx] = info_list[idx].sort_index()
                price_list[idx] = symbol['info']['close'][-1]

        self.kosdaq['price'] = price_list
        self.kosdaq['info'] = info_list
    def get_inst_info(self, period=3) :
        max_inst_deal_period_volumes = list()
        last_inst_deal_period_volumes = list()        
        for idx, symbol in self.kosdaq.iterrows() :
            symbol['max_inst_deal_period_volume'] = 0
            
            try :
                period_volumes = deque()                            
                for inst_deal_volume in symbol['info']['inst_deal_volume'] :
                    if len(period_volumes) < period :
                        period_volumes.append(inst_deal_volume)
                        continue
                    period_volumes.popleft()
                    period_volumes.append(inst_deal_volume)
                    
                    if symbol['max_inst_deal_period_volume'] < sum(period_volumes) :
                        symbol['max_inst_deal_period_volume'] = sum(period_volumes)

                max_inst_deal_period_volumes.append(symbol['max_inst_deal_period_volume'])
                last_inst_deal_period_volumes.append(sum(period_volumes))
                
                del period_volumes

            except Exception as e :
                del period_volumes
                last_inst_deal_period_volumes.append(None)                
                max_inst_deal_period_volumes.append(None)
        self.kosdaq['max_inst_deal_period_volume'] = max_inst_deal_period_volumes
        self.kosdaq['last_inst_deal_period_volume'] = last_inst_deal_period_volumes

        for idx, symbol in self.kosdaq.iterrows() :
            if symbol['max_inst_deal_period_volume'] <= symbol['last_inst_deal_period_volume'] and not symbol['max_inst_deal_period_volume'] <= 0:
                self.inst_tickets.append(symbol['name'])

        with  open('inst_tickets.csv', 'w') as f :
            writer = csv.writer(f)
            writer.writerow(self.inst_tickets)

    def get_week_info(self) :
        #        self.kosdaq['info'][index]['date'].index[-1].strftime('%F')
        weekclose_list = [[] for idx, symbol in self.kosdaq.iterrows()]
        turnaround_list = [0 for idx, symbol in self.kosdaq.iterrows()]
        strait_markup_weeks_list = [0 for idx, symbol in self.kosdaq.iterrows()]
        strait_markdown_weeks_list = [0 for idx, symbol in self.kosdaq.iterrows()]
        weeks_list = [0 for idx, symbol in self.kosdaq.iterrows()]
        overwall_list = [0 for idx, symbol in self.kosdaq.iterrows()]
   
        for idx, symbol in self.kosdaq.iterrows() :
            weekclose = 0
            try :                
                weekday = symbol['info']['close'].index[0].weekday()
            except Exception as e :
                weekclose_list.append(None)    
                continue

            symbol['week_close'] = list()

            last_week_close = 0
            strait_markup_weeks = 0
            strait_markdown_weeks = 0
            
            try :
                for info_idx, info_symbol in enumerate(symbol['info']['close']) :
#                    print(weekday, symbol['info']['close'].index[info_idx].weekday(), symbol['info']['close'].index[info_idx])
                    if(weekday >= symbol['info']['close'].index[info_idx].weekday()) :
                        symbol['week_close'].append(weekclose)
                        turnaround_list[idx] = False
                        
                        if weekclose > last_week_close :
                            strait_markup_weeks = strait_markup_weeks + 1
                            if strait_markdown_weeks > 4:
                                turnaround_list[idx] = True
                            strait_markdown_weeks = 0
                        elif weekclose < last_week_close :
                            strait_markdown_weeks = strait_markdown_weeks + 1
                            strait_markup_weeks = 0
                        else :
                            strait_markup_weeks = 0
                            strait_markdown_weeks = 0
                        last_week_close = weekclose
                    
                    weekday = symbol['info']['close'].index[info_idx].weekday()
                    weekclose = info_symbol
            except Exception as e :
                strait_markup_weeks = 0
                strait_markdown_weeks = 0
                print('what')
            symbol['week_close'].remove(0)

            strait_markup_weeks_list[idx] = strait_markup_weeks
            strait_markdown_weeks_list[idx] = strait_markdown_weeks
            weeks_list[idx] = len(symbol['week_close'])
            max_week_close = max(symbol['week_close'])
            weekclose_list[idx] = symbol['week_close']

            if max_week_close < last_week_close :
                overwall_list[idx] = True

        self.kosdaq['week_close'] = weekclose_list
        self.kosdaq['turnaround'] = turnaround_list
        self.kosdaq['strait_markup_weeks'] = strait_markup_weeks_list
        self.kosdaq['strait_markdown_weeks'] = strait_markup_weeks_list
        self.kosdaq['weeks'] = weeks_list
        self.kosdaq['overwall'] = overwall_list

    def get_info(self) :
        self.load_info()
        self.get_stocks_info(from_year=2015)
        self.save_info()
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

    def save_info(self) :
        for idx, symbol in self.kosdaq.iterrows() :        
            try :
                self.kosdaq['info'][idx].to_json('kosdaq_info_'+self.kosdaq['code'][idx])
                print('save'+'kosdaq_info_'+self.kosdaq['code'][idx] + self.kosdaq['name'][idx])
            except Exception as e:
                print('no infodata:'+str(idx))
                pass
                    
    def load_info(self) :
        newinfo = []
        '''it requires that numeric code is to be zero-filled by str().zfill(6)'''
        print('loading kosdaq infomations..\n')
        for idx, symbol in self.kosdaq.iterrows() :
            try :
                info = pd.read_json('kosdaq_info_' + self.kosdaq['code'][idx])
                del self.kosdaq['info'][idx]
                self.kosdaq['info'][idx] = copy.deepcopy(info)
                del info
#                key = self.kosdaq.loc[self.kosdaq['code'].isin([self.kosdaq['code'][idx]])].index.tolist()[0]                
            except Exception as e:
                pass

        self.kosdaq['info'] = self.kosdaq['info'].sort_index()

    def read_csv(self) :
        with  open('kosdaq_symbol.csv', 'r') as f :
            reader = csv.reader(f)
            for row in reader :
                print(row)

    def pd_read_csv(self) :
        data_df = pd.read_csv('kosdaq_symbol.csv')
        print(data_df)

if  __name__ == '__main__' :
    sb = StockBrush()
#    paint.get_info()
    sb.load_info()
#    inst.get_week_close()
#    inst.get_stocks_info(from_year=2016, from_month=3)
#    inst.make_sql_table()
#    inst.load_sql_table()



