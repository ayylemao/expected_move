from const.keys import api_key
import numpy as np
from google.cloud import bigquery
import pandas as pd
import datetime
import pandas
import pandas_gbq
from polygon import RESTClient
from polygon.exceptions import BadResponse


CLIENT = bigquery.Client()
RESTCLIENT = RESTClient(api_key=api_key)
PROJECT_ID = 'impvoltracker'

class ExpMove():
    def __init__(self, ticker) -> None:
        self.ticker = ticker

#written on Monday, Wednesday and Friday
#             0         2          4
#
        
    def get_earnings_cal(self):
        query = f'select * from market_data.earnings_cal where act_symbol = \'{self.ticker}\''
        self.earnings_cal = pandas_gbq.read_gbq(query, project_id=PROJECT_ID)
        self.earnings_cal['dow'] = self.earnings_cal['date'].dt.weekday
        self.earnings_cal['pre_eval_date'] = np.nan
        self.earnings_cal['post_eval_date'] = np.nan
        for index, row in self.earnings_cal.iterrows():
            if row['when'] == 'After market close':
                self.earnings_cal.loc[index, 'pre_eval_date'] = self.earnings_cal.loc[index, 'date']
                if row['dow'] != 4:
                    self.earnings_cal.loc[index, 'post_eval_date'] = self.earnings_cal.loc[index, 'date'] + datetime.timedelta(days=1)
                if row['dow'] == 4:
                    self.earnings_cal.loc[index, 'post_eval_date'] = self.earnings_cal.loc[index, 'date'] + datetime.timedelta(days=3)
            elif row['when'] == 'Before market open':
                self.earnings_cal.loc[index, 'post_eval_date'] = self.earnings_cal.loc[index, 'date']
                if row['dow'] != 0:
                    self.earnings_cal.loc[index, 'pre_eval_date'] = self.earnings_cal.loc[index, 'date'] - datetime.timedelta(days=1)
                if row['dow'] == 0:
                    self.earnings_cal.loc[index, 'pre_eval_date'] = self.earnings_cal.loc[index, 'date'] - datetime.timedelta(days=3)
            elif row['when'] == None:
                self.earnings_cal.drop(index, inplace=True)
                   
        self.earnings_cal['pre_eval_date'] = pd.to_datetime(self.earnings_cal['pre_eval_date'], format='%Y-%m-%d') 
        self.earnings_cal['post_eval_date'] = pd.to_datetime(self.earnings_cal['post_eval_date'], format='%Y-%m-%d') 
        self.earnings_cal = self.earnings_cal.sort_values(by='date')

    def get_close_df(self):        
        query_dates = ''
        for index, row in self.earnings_cal.iterrows():
            temp_date = (self.earnings_cal.loc[index, 'pre_eval_date'].strftime('%Y-%m-%d'))
            query_dates += f'"{temp_date}", '
            temp_date = (self.earnings_cal.loc[index, 'post_eval_date'].strftime('%Y-%m-%d'))
            query_dates += f'"{temp_date}", '
        query_dates = query_dates[:-2]
        query = f'select * from market_data.stock_ohlcv where date in ({query_dates}) and act_symbol = "{self.ticker}"'

        df = pandas_gbq.read_gbq(query, PROJECT_ID)
        self.earnings_cal = pd.merge(left=self.earnings_cal, right=df[['date', 'close']].rename(columns={'date':'pre_eval_date', 'close':'pre_close'}), how='left')
        self.earnings_cal = pd.merge(left=self.earnings_cal, right=df[['date', 'close']].rename(columns={'date':'post_eval_date', 'close':'post_close'}), how='left')

    def get_options_data(self):
        self.earnings_cal['implied_move'] = np.nan
        for index, row in self.earnings_cal.iterrows():
            pre_eval_date = self.earnings_cal.loc[index, 'pre_eval_date']
            pre_close = self.earnings_cal.loc[index, 'pre_close']
            pre_expiration = self.next_exp(pre_eval_date)

            response = RESTCLIENT.list_options_contracts(
                                                        underlying_ticker=self.ticker,
                                                        expiration_date=pre_expiration.strftime('%Y-%m-%d'),
                                                        strike_price_gt=pre_close*0.9,
                                                        strike_price_lt=pre_close*1.1,
                                                        expired=True
                                                        ) 
            dfcontracts = pd.DataFrame(response)
            dfcall = dfcontracts[dfcontracts['contract_type']=='call'].sort_values(by='strike_price').reset_index(drop=True)
            dfput = dfcontracts[dfcontracts['contract_type']=='put'].sort_values(by='strike_price').reset_index(drop=True)
            atm_idx = dfcall['strike_price'].sub(pre_close).abs().idxmin()
            atm_call = self.get_contract_data(dfcall.loc[atm_idx, 'ticker'], pre_eval_date)
            atm_put = self.get_contract_data(dfput.loc[atm_idx, 'ticker'], pre_eval_date)
            fotm_call = self.get_contract_data(dfcall.loc[atm_idx+1, 'ticker'], pre_eval_date)
            fotm_put = self.get_contract_data(dfput.loc[atm_idx-1, 'ticker'], pre_eval_date)
            sotm_call = self.get_contract_data(dfcall.loc[atm_idx+2, 'ticker'], pre_eval_date)
            sotm_put = self.get_contract_data(dfput.loc[atm_idx-2, 'ticker'], pre_eval_date)
            self.earnings_cal.loc[index, 'implied_move'] = (atm_call+atm_put)*0.6 + (fotm_call+fotm_put)*0.3 + (sotm_call+sotm_put)*0.1

    def get_contract_data(self, contract, eval_date):
        eval_date = eval_date.strftime('%Y-%m-%d')
        res = RESTCLIENT.get_daily_open_close_agg(ticker=contract, date=eval_date)
        return res.close

    def calc_moves(self):
        self.earnings_cal['actual_move'] =  self.earnings_cal['post_close'] - self.earnings_cal['pre_close']
        self.earnings_cal['actual_move%'] = ((self.earnings_cal['post_close'] - self.earnings_cal['pre_close']) / self.earnings_cal['pre_close'])*100
        self.earnings_cal['implied_move%'] = ((self.earnings_cal['implied_move']) / self.earnings_cal['pre_close'])*100
    
    def build_contract(self, expiration, strike, cp):
        contract = 'O:'
        contract += self.ticker
        contract += expiration.strftime('%y%m%d')
        contract += 'C' if cp == 'Call' else 'P'
        contract += str(int(strike*1000)).zfill(8)
        return contract

    @staticmethod
    def round_half(number):
        return np.round(number * 2) / 2 

    @staticmethod
    def round_five(number):
        return 5 * np.round(number / 5)

    @staticmethod
    def next_exp(test_date, weekday_idx=4): 
        if test_date.weekday() != 4:
            return test_date + datetime.timedelta(days=(weekday_idx - test_date.weekday() + 7) % 7)
        else:
            return test_date + datetime.timedelta(days=7)


m = ExpMove('UBER')
m.get_earnings_cal()

m.get_close_df()
m.get_options_data()
m.calc_moves()
m.earnings_cal


print('imp avg', abs(m.earnings_cal['implied_move%']).mean())
print('act avg', abs(m.earnings_cal['actual_move%']).mean())

