# -*- coding: utf-8 -*-

import time
import numpy as np
from pandas import DataFrame


import ccxt

# -----------------------------------------------------------------------------

def trade(YOUR_API_KEY, YOUR_SECRET_KEY):
	#登录binance账户
	exchange = ccxt.binance({
    'apiKey': YOUR_API_KEY,
    'secret': YOUR_SECRET_KEY,
    'enableRateLimit': True,
	})

	#设置初始状态为0
	position = 0
	#设置货币对
	symbol = 'ETH/BTC'
	#设置交易间隔
	timeframe = '5m'

	while True:
		#获取历史交易数据
		history_data = exchange.fetch_ohlcv(symbol, timeframe)
		#将历史交易数据转化为数据框
		history_data = DataFrame(history_data,columns = ['day', 'open', 'highest', 'lowest', 'close', 'volumn'])
		row_num = history_data.shape[0]
		#获取最近一阶段的开盘价
		open_price = history_data['open'].ix[row_num - 1]
		close = history_data['close']
		#获取最近一阶段的收盘价
		close_price = close.ix[row_num -1]
		#获取最近35个交易区间的收盘价
		latest35 = close.ix[(row_num - 35):(row_num - 1)]
		#计算中间价
		ave_ma = latest35.mean()
		#计算标准差
		std = latest35.std()
		#计算上界
		upper_bound = ave_ma + 2 * std
		#计算下界
		lower_bound = ave_ma - 2 * std

		#下面是正式交易策略
		if(position != 1 & close_price > upper_bound):
			order = exchange.create_order(symbol, 'MARKET', 'buy', 1.0, open_price)
			position = 1
		if(position != -1 & close_price < lower_bound):
			order = exchange.create_order(symbol, 'MARKET', 'sell', 1.0, open_price)
			position = -1
		if(position == 1 & close_price < ave_ma):
			order = exchange.create_order(symbol, 'MARKET', 'sell', 1.0, open_price)
			position = 0
		if(position == -1 & close_price > ave_ma):
			order = exchange.create_order(symbol, 'MARKET', 'sell', 1.0, open_price)
			position = 0
		#每执行一次交易休眠5分钟
		time.sleep(300)




if __name__ == '__main__':
	#输入账号密码即可执行
	YOUR_API_KEY = ''
	YOUR_SECRET_KEY = ''
	trade(YOUR_API_KEY, YOUR_SECRET_KEY)
