"""
    Extract info from google finance stock screener.
    Author: Tan Kok Hua
    Blog: simplypython@wordpress.com

    Notes:
        Make use of the google stock screener to retrieve all the data
        To make sure all the data is retrieved, open the criterial of each to max.
        As each time can only process 12 crteria, will have to make several calls to naver finance and join the data.
        
        Some modification have to make to the json file download, there are some unicode that cannot be processed,
        as they are not curcial to the data. \X are being replaced by ' '

    Updates:
        Jun 01 2015: Add in rename columns name functions
        May 20 2015: Update mid url as txt file

    ToDo:
        Change some of paramters names 
    
"""
import os, re, sys, time, datetime, copy, calendar, io
import pandas as pd
from jsonwebretrieve import WebJsonRetrieval
from bs4 import BeautifulSoup
import simplejson as json
from urllib.parse import quote, unquote
from urllib.request import Request, urlopen
import requests
from google_stock_data import GoogleStockData

def overwrite(string):
    import sys
    backspaces = ''
    printlen = len(string)
    for x in range(printlen):
        backspaces += '\b'    
    sys.stdout.write(backspaces+string)
    sys.stdout.flush()

class NaverStockData(object):
    def __init__(self, exchange):
        """ 

        """
        exchange['trade_info'] = [pd.DataFrame for idx in exchange.iterrows()]
        self.exchange = exchange[pd.notnull(exchange.MarketCap)].copy()
        self.web_data_file = 'web_data'        

        self.trade_info = pd.DataFrame({'date' :[], 
                                  'close' : [],   
                                  'change' : [],
                                  'deal_volume' : [],
                                  'inst_deal_volume' : [],
                                  'foreign_deal_volume' : [],
                                  'foreign_volume' : [],
                                  'foreign_volume_rate' : []})
        self.trade_info.set_index('date', inplace=True)
        self.trade_info_list = list()        
        self.url_base = 'http://finance.naver.com/item/frgn.nhn?code='

    def save_symbol(self, trade_info, symbol) :
        print('saving trade infomation..\n')
        filename = 'naver_stock_data/trade_info_'

        try :
            trade_info.to_json(filename + symbol)
        except Exception as e:
            print(symbol + ' is no trade info ')        

    def save(self) :
        print('saving trade infomations..\n')
        filename = 'naver_stock_data/trade_info_'
        for idx, symbol in self.exchange.iterrows() :
            try :
                self.exchange['trade_info'][idx].to_json(filename + symbol['SYMBOL'])
            except Exception as e:
                print(symbol['SYMBOL'] + ' is no trade info ')
        
    def load(self) :
        print('loading trade infomations..\n')
        #       filename = 'naver_stock_data/' + 'kosdaq_info_'
        filename = 'naver_stock_data/trade_info_'        

        #it needs to check that new symbols are added in the exchange
        self.exchange['trade_info'] = [pd.read_json(filename + symbol['SYMBOL']) \
                          if os.path.isfile(filename + symbol['SYMBOL']) else pd.DataFrame() \
                          for idx, symbol in self.exchange.iterrows()]
        
    def crawl_web_data(self, index, new_trade_info) :
        res = 0
        f = open(self.web_data_file, 'r')
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
                    res = 1
                    break

                if idx == 0 :
                    try :
                        if self.exchange['trade_info'][index]['date'].index[-1].strftime('%F') \
                           >= pd.to_datetime(column.string.replace('.', '-')).strftime('%F') :
                            f.close()
                            res = 2
                            table_info = pd.DataFrame({'date' :columns[0], 
                                                 'close' : columns[1],   
                                                 'change' : columns[3],
                                                 'deal_volume' : columns[4],
                                                 'inst_deal_volume' : columns[5],
                                                 'foreign_deal_volume' : columns[6],
                                                 'foreign_volume' : columns[7],
                                                 'foreign_volume_rate' : columns[8]})
                            table_info.set_index('date', inplace=True)
                            table_info.sort_index()
                            new_trade_info = new_trade_info.append(table_info)
                            
                            return res, new_trade_info
                    except Exception as e:
                        pass
                            
                    columns[idx].append(pd.to_datetime(column.string.replace('.', '-')))
                elif idx == 1 or idx == 4 or idx == 5 or idx == 6 or idx == 7 :
                    try :
                        columns[idx].append(int(column.string.replace(',', '')))
                    except Exception as e :
                        if isinstance(column.string, type(None)) :
                            print('there is no trade data in exchange!!!!, maybe delisting..')
                            res = 1
                            f.close()                            
                            return res, new_trade_info
                        else :
                            columns[idx].append(int(column.string))
                else :
                    columns[idx].append(column.string)

        table_info = pd.DataFrame({'date' :columns[0], 
                             'close' : columns[1],   
                             'change' : columns[3],
                             'deal_volume' : columns[4],
                             'inst_deal_volume' : columns[5],
                             'foreign_deal_volume' : columns[6],
                             'foreign_volume' : columns[7],
                             'foreign_volume_rate' : columns[8]})
        table_info.set_index('date', inplace=True)

        if not res :
            try :
                if columns[0][0] in new_trade_info.index.tolist() :
                    res = 1
                else :
                    new_trade_info = new_trade_info.append(table_info)
            except Exception as e :
                ''' fault symbol'''
                print('fault!!!!!')
                res = 3

        f.close()
        return res, new_trade_info

    def extract_new_trade_info(self, idx, symbol, from_day=1, from_month=1, from_year=2015) :
        new_trade_info = pd.DataFrame({'date' :[], 
                                           'close' : [],   
                                           'change' : [],
                                           'deal_volume' : [],
                                           'inst_deal_volume' : [],
                                           'foreign_deal_volume' : [],
                                           'foreign_volume' : [],
                                           'foreign_volume_rate' : []})
        pagenum = 1
        out_of_date = 0
            
        while True :
            url = self.url_base + str(symbol['SYMBOL']) + '&page=' + str(pagenum)                

            try :
                r = requests.get(url,timeout=5)
            except Exception as e :
                print('trying to get a request again')
                continue
                
            with io.open(self.web_data_file, 'w') as f:
                f.write(str(r.text))
            try :
                res, new_trade_info = self.crawl_web_data(idx, new_trade_info)
                if res != 1 :
                    if new_trade_info['date'].index[-1].year < from_year :
                        res = 2
                    elif new_trade_info['date'].index[-1].month < from_month :
                        res = 2
                    elif new_trade_info['date'].index[-1].day < from_day :
                        res = 2
            except Exception as e:
                #print('it could not crawl new trade info')
                pass
            
            if res != 0 :
                break
            pagenum = pagenum + 1
#        print(res)
        return res, new_trade_info

    def get_trade_info(self, from_day=1, from_month=1, from_year=2015, ext_limit=0, can_bypass=1) :
        bypass_extract = 0
        count = 0
        trade_info_list = []

        self.load()

        for idx, symbol in self.exchange.iterrows() :
            if bypass_extract :
                break
            
            if ext_limit != 0 :
                if idx >= ext_limit :
                    break
            
            overwrite('%d/%d...getting a symbol(%s)' % (count, len(self.exchange), symbol['SYMBOL']))
            res, new_trade_info = self.extract_new_trade_info(idx, symbol)

            # there is the lastest informations
            if not new_trade_info.empty :
                #there is no cached informations 
                if not symbol.trade_info.empty :
                    trade_info_list.append(symbol['trade_info'].append(new_trade_info))
#                    print(new_trade_info)
                else :
                    self.save_symbol(new_trade_info, symbol['SYMBOL'])
                    trade_info_list.append(new_trade_info)

                trade_info_list[-1] = trade_info_list[-1].sort_index()
            else :
                if res != 1 :
                    print('trade informations are already lastest data')
                    if can_bypass :
                        print('break extracting data.')
                        bypass_extract = 1
                    trade_info_list.append(symbol['trade_info'])
                else :
                    print('there is no data')
                    trade_info_list.append(pd.DataFrame)
                #trade_info_list.append(symbol['trade_info'])
            count = count + 1

        if not bypass_extract :
            self.exchange['trade_info'] = trade_info_list
            self.save()

        return self.exchange['trade_info']

if __name__ == '__main__':
    choice  = 2

    gsd_kosdaq = GoogleStockData('KOSDAQ')
    exchange = gsd_kosdaq.get_screener_data()

    if choice == 2:
        nsd = NaverStockData(exchange)
        nsd.load()
        nsd.get_trade_info()
        nsd.save()

#        nsd.get_tra_trade_data()
#        nsd.save_data()
