"""
    Extract info from google finance stock screener.
    Author: Tan Kok Hua
    Blog: simplypython@wordpress.com

    Notes:
        Make use of the google stock screener to retrieve all the data
        To make sure all the data is retrieved, open the criterial of each to max.
        As each time can only process 12 crteria, will have to make several calls to google finance and join the data.
        
        Some modification have to make to the json file download, there are some unicode that cannot be processed,
        as they are not curcial to the data. \X are being replaced by ' '

    Updates:
        Jun 01 2015: Add in rename columns name functions
        May 20 2015: Update mid url as txt file

    ToDo:
        Change some of paramters names 
    
"""
import os, re, sys, time, datetime, copy, calendar
import pandas
from jsonwebretrieve import WebJsonRetrieval
import simplejson as json
from urllib.parse import quote, unquote
from urllib.request import Request, urlopen

googleFinanceKeyToFullName = {
    u'id'     : u'ID',
    u't'      : u'StockSymbol',
    u'e'      : u'Index',
    u'l'      : u'LastTradePrice',
    u'l_cur'  : u'LastTradeWithCurrency',
    u'ltt'    : u'LastTradeTime',
    u'lt_dts' : u'LastTradeDateTime',
    u'lt'     : u'LastTradeDateTimeLong',
    u'div'    : u'Dividend',
    u'yld'    : u'Yield'
}

class GoogleStockData(object):
    """
        Target url need to be rotate and join separately.
    """
    def __init__(self, exchange):
        """ 

        """
        ## url parameters for joining
        self.exchange = exchange
        self.target_url_start = 'https://www.google.com/finance?output=json&start=0&num=3000&noIL=1&q=['
        self.target_url_end = ']&restype=company&ei=BjE7VZmkG8XwuASFn4CoDg'
        self.temp_url_mid = '(exchange == \"{0}\") & (net_income_growth_rate_5years >= -72.79) & (net_income_growth_rate_5years <= 268) & (revenue_growth_rate_5years >= -68.28) & (revenue_growth_rate_5years <= 384) & (revenue_growth_rate_10years >= -42.88) & (revenue_growth_rate_10years <= 135) & (eps_growth_rate_5years >= -88.66) & (eps_growth_rate_5years <= 268) & (eps_growth_rate_10years >= -46.25) & (eps_growth_rate_10years <= 70.71) & (gross_margin_trailing_12months >= -353) & (gross_margin_trailing_12months <= 586) & (ebitd_margin_trailing_12months >= -4617) & (ebitd_margin_trailing_12months <= 535) & (operating_margin_trailing_12months >= -8505) & (operating_margin_trailing_12months <= 1851) & (net_profit_margin_percent_trailing_12months >= -8963) & (net_profit_margin_percent_trailing_12months <= 1851) & (average_volume >= 0) & (average_volume <= 73040000) & (volume >= 0) & (volume <= 115280000) & (shares_floating >= 0) & (shares_floating <= 24226)'.format(self.exchange)
        self.temp_url_mid = '(exchange == \"{0}\") & (volume >= 0) & (volume <= 1000000000000) & (market_cap >= 0) & (market_cap <= 1000000000000000)'.format(self.exchange)
        self.target_full_url = ''
        self.temp_url_mid_encode = quote(self.temp_url_mid)

        self.temp_rt_url = 'http://finance.google.com/finance/info?client=ig&q='

        ## parameters
        self.saved_json_file = r'./gsd_screener.json'
        self.target_tag = 'searchresults' #use to identify the json data needed

        ## Result dataframe
        self.result_google_ext_df = pandas.DataFrame()

    def form_full_screener_url(self):
        """ Form the url"""
        self.target_full_url = self.target_url_start + self.temp_url_mid_encode + self.target_url_end
        
    def get_json_obj_fr_file(self):
        """ Return the json object from the .json file download.        
            Returns:
                (json obj): modified json object fr file.
        """

        with open(self.saved_json_file) as f:
            data_str = f.read() 
        # replace all the / then save back the file
        update_str = re.sub(r"\\",'',data_str)
        json_raw_data = json.loads(update_str)
        return json_raw_data

    def convert_json_to_df(self):
        """ Convert the retrieved data to dataframe
            Returns:
                (Dataframe obj): df formed from the json extact.
        """
        json_raw_data = self.get_json_obj_fr_file()
        
        new_data_list = []
        for n in json_raw_data['searchresults']:
            temp_stock_dict={'SYMBOL':n['ticker'],
                             'CompanyName':n['title'],
                            }
            for col_dict in n['columns']:
                if not col_dict['value'] == '-':
                    temp_stock_dict[col_dict['field']] = col_dict['value']
                
            new_data_list.append(temp_stock_dict)
            
        return pandas.DataFrame(new_data_list)        

    def get_screener_data(self):
        """ Retreive all the stock data. Iterate all the target_url_mid1 """
        self.form_full_screener_url()

        """ Retrieve the json file based on the self.target_full_url"""
        wj = WebJsonRetrieval(r'./gsd_screener.json')
        wj.set_url(self.target_full_url)
        print(self.target_full_url)
        wj.download_json() # default r'c:\data\temptryyql.json'        
        
        temp_data_df = self.convert_json_to_df()
        if len(self.result_google_ext_df) == 0:
            self.result_google_ext_df = temp_data_df
        else:
            self.result_google_ext_df =  pandas.merge(self.result_google_ext_df, temp_data_df, on=['SYMBOL','CompanyName'])

        return self.result_google_ext_df

        #self.rename_screener_df_columns() 

    def rename_screener_df_columns(self):
        """ Rename some of columns to avoid confusion as from where the data is pulled.
            Some of names added the GS prefix to indicate resutls from google screener.
            Set to self.result_google_ext_df
        """
        self.result_google_ext_df = self.result_google_ext_df.rename(columns={'CompanyName':'GS_CompanyName',
                                                                                 'AverageVolume':'GS_AverageVolume',
                                                                                 'Volume':'GS_Volume',
                                                                                 'AINTCOV':'Interest_coverage',
                                                                                })
        
    def build_quotes_url(self, exchange, symbols):
        symbol_list = exchange.join([symbol for symbol in symbols])
        symbol_list = ','.join([symbol for symbol in symbols])
        # a deprecated but still active & correct api
        return 'http://finance.google.com/finance/info?client=ig&q=' + symbol_list

    def request_quotes(self, exchange, symbols):
        url = self.build_quotes_url(exchange, symbols)
        req = Request(url)
        resp = urlopen(req)
        # remove special symbols such as the pound symbol
        content = resp.read().decode('ascii', 'ignore').strip()
        content = content[3:]
            
        return content

    def replace_quotes_keys(self, quotes):
        global googleFinanceKeyToFullName
        quotesWithReadableKey = []
        for q in quotes:
            qReadableKey = {}
            for k in googleFinanceKeyToFullName:
                if k in q:
                    qReadableKey[googleFinanceKeyToFullName[k]] = q[k]
                        
            quotesWithReadableKey.append(qReadableKey)
        return quotesWithReadableKey

    def get_quotes(self, exchange, symbols) :
        '''
        quotes = get_quotes('KOSDAQ', ['035420', '069080'])
        :param exchange:
        :param symbols: a single symbol or a list of stock symbols
        :return: real-time quotes list
        '''
        if type(symbols) == type('str'):
            symbols = [symbols]
        content = json.loads(self.request_quotes(exchange, symbols))
        return self.replace_quotes_keys(content);        
        
if __name__ == '__main__':
    choice  = 2

    if choice == 2:
        gsd = GoogleStockData('KOSDAQ')
        gsd.get_screener_data()
#        quotes = gsd.get_quotes('KOSDAQ', ['069080', '200670'])
#        print(quotes)

#        gsd.result_google_ext_df.to_csv(r'./result.csv', index =False)
