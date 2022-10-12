#-*- coding: utf-8 -*-
import WebSocketLogic
import pandas as pd
import datetime
import sys
# from OptimisticSettingSearch import MultiProcessingForSettingValue
# from MultiprocessingForTrade import Ticker_Screening, MultiProcessingForTrade
# from UpbitDBManager import DB_Manager
from telegram.ext import Updater
from telegram.ext import MessageHandler, Filters, CommandHandler
import telegram
import sqlite3
import os
import multiprocessing
import asyncio
import websockets
import json
import pandas as pd
import time
import pyupbit
import sqlite3
import WebSocketLogic
from UpbitDBManager import DB_Manager
import traceback
import logging
import New_Logic
import nest_asyncio
from Trader_Expert import Trading_Executer, Master

ConToDB = sqlite3.connect("C:/Users/bbs68/PycharmProjects/Bitcoin/DB/UpbitDB.db", check_same_thread=False)
ConToLogicTest = sqlite3.connect("C:/Users/bbs68/PycharmProjects/Bitcoin/DB/LogicValueTest.db", check_same_thread=False)
ConToTemp = sqlite3.connect("C:/Users/bbs68/PycharmProjects/Bitcoin/DB/WebSocketTrading.db", check_same_thread=False)
ConToLogicDB = sqlite3.connect("C:/Users/bbs68/PycharmProjects/Bitcoin/DB/LogicDB.db")
Initial_Sig = 0
Min_Volume = 0
Temp_Volume = 0
Temp_min =0
Buy_Price = 0
Sell_Price = 0
dic = []
List = []
Owned_list = []
k = 0
zz = 0
vv = 0
Position= 0
access = "OZjdzRxBZkwevRJG31METin4EZvyO9t3Uv2G5jEi"  # access key 직접 입력
secret = "bp9vdlMSTyogRO2O9ub9HQlxhcAuWcWJRaqxzeEc"  # secret key 직접 입력

upbit = pyupbit.Upbit(access, secret)

def findNearNum(exList, values):
    minValue = min(exList, key=lambda x: abs(x - values))
    minIndex = exList.index(minValue)
    return minValue

def Ticker_Screening():
    global Screen
    print('Ticker Screening Start!!!!')
    balance = upbit.get_balances()
    for i in range(1, len(balance)):
        Owned_list.append('KRW-%s' % balance[i]['currency'])
    try:
        Owned_list.remove('KRW-LUNC')
    except:
        pass
    try:
        Screen = pd.read_sql("SELECT * FROM '%s'" % ('Ticker_Screen'), ConToTemp, index_col='ticker')
        date_time_str = Screen.tail(1).iloc[0]['date']
        date_time_obj = datetime.datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')
        limit_date = datetime.datetime.now() - datetime.timedelta(hours=1) - datetime.timedelta(minutes=10)

        if date_time_obj >= limit_date:
            print('Aleady Screening Recently!!!!')
            return

    except:pass

    tickers = pyupbit.get_tickers('KRW')
    for tic in tickers:
        orderbook = pyupbit.get_orderbook(tic)
        single_tic_per = round(orderbook['orderbook_units'][1]['ask_price'] - orderbook['orderbook_units'][0]['ask_price'],
                    5) / pyupbit.get_current_price(tic) * 100
        if single_tic_per > 0.2:
            tickers.remove(tic)

    tickers.remove('KRW-BTC')
    Screen = pd.DataFrame()
    for tic in tickers:
        try:
            all_minute60 = pd.DataFrame(pyupbit.get_ohlcv(tic, "minute60", count=24))
            # new_data = {'ticker': tic, 'value_sum': all_minute60['value'].sum()}
            new_data = {'date': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'ticker': tic, 'value_sum': all_minute60['value'].sum()}
            Screen = Screen.append(new_data, ignore_index='ticker')
        except:
            pass
    Screen.reset_index(drop=True)
    Screen.set_index('ticker', inplace=True)
    Screen = Screen.sort_values(by=['value_sum'], ascending=[False])

    Screen.to_sql('Ticker_Screen', ConToTemp, if_exists='replace')
    print('Screening Complete!!')

    # global dic
    # now = datetime.datetime.now()
    # if AVG_BuyPrice != 0:
    #     Earning_Per = (Price - AVG_BuyPrice)/AVG_BuyPrice*100
    # else:Earning_Per = 0
    #
    # if Action == 'Buy':
    #     ret = upbit.buy_limit_order(tic, Price, Balance)
    #     print(ret)
    #     # ret = upbit.buy_market_order(tic, TradingUnit, contain_req=False) ### 시장가주문
    #     telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(
    #         chat_id=1184586349, text='%s매수주문!!!' % (tic))
    #     data = pd.read_sql("SELECT * FROM '%s'" % (tic), ConToLogicDB)
    #     WebSocketLogic.OneStockGraph_DirectFeed(data)
    #     telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').send_photo(chat_id=1184586349, photo=open(
    #         "C:/Users/bbs68/PycharmProjects/Bitcoin/DB/fig.png", 'rb'))
    #
    #     log_data = {'Date': [now], 'Ticker': [tic], 'Action': ['Buy'],
    #                 'Trade_Price': [Price], 'Balance': [Balance]}
    #     log_data = pd.DataFrame(log_data).copy()
    #     log_data.to_sql('Trade_Log', ConToTemp, if_exists='append')
    #     return ret['uuid']
    #
    # if Action == 'Buy_Now':
    #     ret = upbit.buy_market_order(tic, Price, contain_req=False) ### 시장가주문
    #     print(ret)
    #     telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(
    #         chat_id=1184586349, text='%s매수주문!!!' % (tic))
    #     data = pd.read_sql("SELECT * FROM '%s'" % (tic), ConToLogicDB)
    #     WebSocketLogic.OneStockGraph_DirectFeed(data)
    #     telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').send_photo(chat_id=1184586349, photo=open(
    #         "C:/Users/bbs68/PycharmProjects/Bitcoin/DB/fig.png", 'rb'))
    #     while True:
    #         print('채결 대기')
    #         ordered_ret = upbit.get_order(ret['uuid'])
    #         if ordered_ret['state'] == 'cancel':
    #             break
    #     log_data = {'Date': [now], 'Ticker': [tic], 'Action': ['Buy'],
    #                 'Trade_Price': [ordered_ret['trades'][0]['price']], 'Balance': [ordered_ret['trades'][0]['volume']]}
    #     log_data = pd.DataFrame(log_data).copy()
    #     log_data.to_sql('Trade_Log', ConToTemp, if_exists='append')
    #     return ret['uuid']
    # if Action == 'Sell':
    #     pass
    # if Action == 'Sell_Now':
    #     ret = upbit.sell_market_order(tic, Balance)
    #     print(ret)
    #     telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(
    #         chat_id=1184586349,
    #         text='%s매도주문!!! 손익률:%s / 손익금액:%s' % (tic, Earning_Per, (Price - AVG_BuyPrice) * Balance))
    #     data = pd.read_sql("SELECT * FROM '%s'" % (tic), ConToLogicDB)
    #     WebSocketLogic.OneStockGraph_DirectFeed(data)
    #     telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').send_photo(chat_id=1184586349, photo=open(
    #         "C:/Users/bbs68/PycharmProjects/Bitcoin/DB/fig.png", 'rb'))
    #     while True:
    #         print('채결 대기')
    #         ordered_ret = upbit.get_order(ret['uuid'])
    #         if ordered_ret['state'] == 'done':
    #             break
    #     log_data = {'Date': [now], 'Ticker': [tic], 'Action': ['Buy'],
    #                 'Trade_Price': [ordered_ret['trades'][0]['price']], 'Balance': [ordered_ret['trades'][0]['volume']]}
    #     log_data = pd.DataFrame(log_data).copy()
    #     log_data.to_sql('Trade_Log', ConToTemp, if_exists='append')

def Ticker_Screen_Auto(count):
    global dic, List
    now = datetime.datetime.now()
    # await asyncio.sleep((4 - now.minute % 5) * 60 + 60 - now.second + 3)
    try:
        print('Auto Ticker Screener Run')
        # await asyncio.sleep((60*5+59-now.minute)*60 + 60-now.second - 15)
        # await asyncio.sleep((11-now.hour%12)*60*60 + (59-now.minute)*60+ 60-now.second + 4)
        # while True:
        Ticker_Screening()

        Owned_list = []
        balance = upbit.get_balances()
        for i in range(1, len(balance)):
            Owned_list.append('KRW-%s' % balance[i]['currency'])
        Owned_list.remove('KRW-LUNC')

        Screen = pd.read_sql("SELECT * FROM '%s'" % ('Ticker_Screen'), ConToTemp)
        List_Top10 = Screen.head(20)['ticker'].tolist()
        Execute_List = list(set(Owned_list) - set(List_Top10))

        ##### 비인기 종목 처형 #####
        if len(Execute_List) != 0:
            print('비인기 종목 처형 : %s' % (Execute_List))
            for tic in Execute_List:
                for t in range(0, len(balance)):
                    if balance[t]['currency'] == tic[4:]:
                        find_tic_num = t
                Balance = float(balance[find_tic_num]['balance'])
                upbit.sell_market_order(tic, Balance)
                # ret = upbit.sell_market_order(tic, Balance)
                # while True:
                #     time.sleep(0.2)
                #     print('채결 대기')
                #     ordered_ret = upbit.get_order(ret['uuid'])
                #     if ordered_ret['state'] == 'done':
                #         print('채결 확인')
                #         break
            Owned_list = list(set(Owned_list) - set(Execute_List))
            telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
                                                                                       text='비인기 종목 처형 : %s' % (Execute_List))

        Screen_List = Screen.head(count)['ticker'].tolist()
        temp = list(set(Owned_list) - set(Screen_List))
        Screen_List.extend(temp)

        DB_Manager(Screen_List, 5, 2)
        print(Screen_List)
        dic = []
        for i in range(0, len(List)):
            dic.append(
                {'ticker': List[i], 'Initial_Sig': 0, 'Acc_Volume': 0, 'Temp_Volume': 0,'Last_Volume' : 0, 'low': 0,
                 'high': 0, 'open': 0, 'Sell_Price': 0, 'Buy_Price': 0, 'trade_price': 0, 'trade_stop': 0,
                 'last_order_UUID': '', 'Mean_Vol': 0, 'Target_1min': 0, 'Trade_Mode': '5min', 'avg_buy_price': 0,
                 'balance': 0, 'Temp_min': 0, 'UpDown' : 0})

        DB_Manager(List, 5, 2)

        return dic
            # await asyncio.sleep((11-now.hour%12)*60*60 + (59-now.minute)*60+ 60-now.second + 4)
    except:
        telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
                                                                                   text='Ticker Screener Stopped!!!')
        telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
                                                                                   text=traceback.format_exc())

async def WebSocketRuner():
    global List
    print('WebSocket Runner Start!!')
    while True:
        await upbit_ws_client(response_message)
        time.sleep(0.3)
        print('WebSocket Runner Restart!!')

async def upbit_ws_client(callback):
    global List
    uri = 'wss://api.upbit.com/websocket/v1'
    temp_now = datetime.datetime.now()
    try:
        async with websockets.connect(uri) as websocket:
            subscribe_fmt = [{'ticket': 'test'}]
            for i in List:
                subscribe_fmt.append(
                    {
                        'type': 'ticker',
                        'codes': [i],
                        'isOnlyRealtime': True
                    })
            subscribe_data = json.dumps(subscribe_fmt)
            # print(now.minute, temp_now.minute)

            await websocket.send(subscribe_data)

            while True:
                await callback(pd.DataFrame([json.loads(await websocket.recv())]))
    except:
        telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349, text='WebSocket Reciever Stopped!')
        telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
                                                                                   text=traceback.format_exc())

# async def Trader():
#     global Buy_Price, Sell_Price, dic, List
#     initial = 0
#     Ticker1_Banance = 0
#     now = datetime.datetime.now()
#     temp_hour = now.hour
#     NonEcho = 0
#
#     try:
#         while True:
#             now = datetime.datetime.now()
#             try:
#                 switch = pd.read_sql("SELECT * FROM '%s'" % ('log'), ConToTemp, index_col='Date')
#                 OnOff = switch.tail(1).iloc[0]['State']
#             except:OnOff='start'
#
#             balance = upbit.get_balances()
#             KRW_Balance = balance[0]['balance']
#
#             # ########################### 미감시 종목 매도 ##########################################
#             #
#             # if now.hour != temp_hour or initial == 0 :
#             #     print(List)
#             #     for a in range(1, len(balance)):
#             #         if 'KRW-%s'%(balance[a]['currency']) not in List and balance[a]['currency'] != 'LUNC':
#             #             upbit.sell_market_order('KRW-%s' % (balance[a]['currency']), float(balance[a]['balance']))
#             #             print('KRW-%s' % (balance[a]['currency']), '미감시 종목 매도')
#             #         initial += 1
#             #         temp_hour = now.hour
#             #
#             # ########################### 미감시 종목 매도 ##########################################
#
#             if OnOff != 'stop':
#                 NonEcho = 0
#                 for i in range(0, len(dic)):
#                     if dic[i]['Initial_Sig'] == 1:
#                         Buy_Price = dic[i]['Buy_Price']
#                         Sell_Price = dic[i]['Sell_Price']
#                         # print(Buy_Price, Sell_Price)
#                         NowPrice = dic[i]['trade_price']
#                         tic = dic[i]['ticker']
#                         Default_TradingUnit = 50000
#                         Maximum_investing = 200000
#                         Loss_Cut = -0.1
#                         Win_Cut = 0.05
#
#                         OrderBook = []
#
#                         find_tic = tic[4:]
#                         find_tic_num = None
#
#                         for t in range(0, len(balance)):
#                             if balance[t]['currency'] == find_tic:
#                                 find_tic_num = t
#
#                         if find_tic_num != None:
#                             Ticker1_Banance = float(balance[find_tic_num]['balance'])
#                             Ticker1_AVG_BuyPrice = float(balance[find_tic_num]['avg_buy_price'])
#                             Earning_Per = round((NowPrice - Ticker1_AVG_BuyPrice) / Ticker1_AVG_BuyPrice,4)
#
#
#                             if dic[i]['trade_stop'] == 0 and dic[i]['Trade_Mode'] == '5min':
#                                 if Buy_Price != 0  and Ticker1_Banance + Default_TradingUnit < Maximum_investing:
#                                     if ((NowPrice <= Buy_Price) or (Position == 'Side' and NowPrice > Buy_Price)) and KRW_Balance != 0:
#
#                                         ########### 호가창 다운로드 ###############
#                                         Upbit_OrderBook = pyupbit.get_orderbook(ticker=tic)
#                                         lenlist = len(pyupbit.get_orderbook(ticker=tic)['orderbook_units'])
#                                         for k in range(0, lenlist):
#                                             OrderBook.append(Upbit_OrderBook['orderbook_units'][k]['ask_price'])
#                                         ########### 호가창 다운로드 ###############
#
#                                         if Ticker1_Banance == 0:
#                                             TradingUnit = Default_TradingUnit
#                                         else:
#                                             # TradingUnit = round(Ticker1_Banance*Ticker1_AVG_BuyPrice, -3)
#                                             TradingUnit = Default_TradingUnit
#
#                                         ########### 호가창에서 가장 근접한 가격으로 주문 발송 ###############
#
#                                         if float(KRW_Balance) < float(round(TradingUnit / NowPrice, 3)*findNearNum(OrderBook, NowPrice)):
#                                             telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(
#                                                 chat_id=1184586349, text='%s 잔액부족!!!' % (tic))
#                                             dic[i]['trade_stop'] = 1
#                                         else:
#                                             Ask_Price = findNearNum(OrderBook, NowPrice)
#                                             dic[i]['last_order_UUID'] = Trading_Executer('Buy',tic, round(TradingUnit / NowPrice, 3), Ask_Price, Ticker1_AVG_BuyPrice)
#                                             dic[i]['trade_stop'] = 1
#                                             print(tic, ":", '매수주문!!!!!', TradingUnit)
#
#                                         ########### 호가창에서 가장 근접한 가격으로 주문 발송 ###############
#
#                                 if Position == 'Up':
#                                     if Sell_Price != 0:
#                                         if NowPrice >= Sell_Price :
#                                             Trading_Executer('Sell_Now', tic, Ticker1_Banance, Sell_Price, Ticker1_AVG_BuyPrice)
#                                             print(tic, ":", '매도주문!!!!!', Earning_Per)
#                                             dic[i]['trade_stop'] = 1
#
#                                 if Position == 'Side':
#                                     if Earning_Per > Win_Cut :
#                                         print(tic, ":", '익절!!!!!')
#                                         dic[i]['trade_stop'] = 1
#                                         Trading_Executer('Sell_Now', tic, Ticker1_Banance, NowPrice, Ticker1_AVG_BuyPrice)
#
#                                 if Position == 'Down':
#                                     if Sell_Price != 0:
#                                         if NowPrice >= Sell_Price:
#                                             Trading_Executer('Sell_Now', tic, Ticker1_Banance, NowPrice, Ticker1_AVG_BuyPrice)
#                                             print(tic, ":", '매도주문!!!!!', Earning_Per)
#                                             dic[i]['trade_stop'] = 1
#
#                                 if Earning_Per > Win_Cut:
#                                     Trading_Executer('Sell_Now', tic, Ticker1_Banance, NowPrice, Ticker1_AVG_BuyPrice)
#                                     print(tic, ":", '익절!!!!!')
#                                     dic[i]['trade_stop'] = 1
#
#                             if Earning_Per < Loss_Cut :
#                                 print(tic, ":", '손절!!!!!')
#                                 Trading_Executer('Sell_Now', tic, Ticker1_Banance, NowPrice, Ticker1_AVG_BuyPrice)
#                                 dic[i]['trade_stop'] = 1
#
#                             else:
#                                 if Earning_Per < - 0.025:
#                                     dic[i]['trade_stop'] = 0
#             else:
#                 if NonEcho == 0 :
#                     print('Trade Stopped!')
#                     NonEcho = 1
#
#             # print('Trading')
#             await asyncio.sleep(2)
#     except:
#         telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349, text='Trader Stopped!!!')
#         logging.error(traceback.format_exc())
#         telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
#                                                                                    text=traceback.format_exc())
#
# async def Real_Time_Trader():
#     global dic, List
#     now = datetime.datetime.now()
#     NonEcho = 0
#     find_tic_num = None
#     Loss_Cut = -0.05
#     Win_Cut = 0.05
#
#     try:
#         while True:
#             try:
#                 switch = pd.read_sql("SELECT * FROM '%s'" % ('log'), ConToTemp, index_col='Date')
#                 OnOff = switch.tail(1).iloc[0]['State']
#             except:OnOff='start'
#
#             if OnOff != 'stop':
#                 NonEcho = 0
#                 for DIC in dic:
#                     if DIC['Trade_Mode'] == '5min':
#                         if DIC['balance'] != 0 and DIC['trade_price'] != 0:
#                             Earning_Per = (DIC['trade_price'] - DIC['avg_buy_price']) / DIC['avg_buy_price']
#                             if Earning_Per <= Loss_Cut:
#                                 dic = Trading_Executer(dic, 'Sell_Now', DIC['ticker'])
#                             elif Earning_Per >= Win_Cut:
#                                 dic = Trading_Executer(dic,'Sell_Now', DIC['ticker'])
#             else:
#                 if NonEcho == 0 :
#                     print('Trade Stopped!')
#                     NonEcho = 1
#
#             # print('Trading')
#             await asyncio.sleep(5)
#     except:
#         telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349, text='Trader Stopped!!!')
#         logging.error(traceback.format_exc())
#         telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
#                                                                                    text=traceback.format_exc())

def Alert_Watcher(dic):
    global Position
    Up_Signals = 0
    Down_Signals = 0
    for Dict in dic:
        if Dict['UpDown'] == 1:
            Up_Signals += 1
        if Dict['UpDown'] == -1:
            Down_Signals += 1
        Dict['UpDown'] = 0
    if Up_Signals >= count/2 :
        if Position != 'Up' :
            Position = 'Up'
            telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
                                                                                   text='Up Position Alert!!!')
    if Down_Signals >= count/2 :
        if Position != 'Stop' :
            Position = 'Stop'
            telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
                                                                                   text='Down Position Alert!!!')
    return dic

async def Position_Selector():
    global Position
    try:
        while True:
            DB_Manager(['KRW-BTC'], 30, Term_Days=7)

            df = pd.read_sql("SELECT * FROM '%s'" % ('KRW-BTC'), ConToDB)
            df1 = df['close'].head(1).append(df['close'][:-1])
            df1 = df1.values.tolist()
            df['close_Push'] = df1
            df['close_Diff'] = df['close'].sub(df['close_Push'])

            df['position'] = 0
            df['ma60'] = df['close_Push'].rolling(window=60).mean()  # 20일 이동평균
            # df['ma5'] = df['close'].rolling(window=5).mean() # 20일 이동평균
            df['ma20'] = df['close'].rolling(window=20).mean()  # 20일 이동평균
            df['ma20_rate'] = df['close_Push'].rolling(window=20).mean()  # 20일 이동평균
            # df['ma20'] = df['close_Push'].rolling(window=20).mean() # 20일 이동평균
            df['stddev'] = df['close_Push'].rolling(window=20).std()  # 20일 이동표준편차
            df['upper'] = df['ma20_rate'] + 2 * df['stddev']  # 상단밴드
            df['lower'] = df['ma20_rate'] - 2 * df['stddev']  # 하단밴드
            df['close0_005'] = df['close_Push'] * 0.005

            df1 = df['ma20'].head(1).append(df['ma20'][:-1])
            df1 = df1.values.tolist()
            df['ma20_Push'] = df1
            df['ma20_Diff'] = df['ma20'].sub(df['ma20_Push'])
            df['MAangle20'] = df['ma20_Diff'].rolling(window=3).mean()

            # df.loc[df['ma20'] > df['ma60'], 'position'] = 'Up'
            # df.loc[df['ma20'] <= df['ma60'], 'position'] = 'Down'
            df.loc[df['MAangle20'] < 0, 'position'] = 'Down'
            df.loc[df['MAangle20'] >= 0, 'position'] = 'Up'
            df.loc[df['stddev'] < df['close_Push'] * 0.005, 'position'] = 'Side'
            df.loc[(df['low'] < df['lower']) * (df['MAangle20'] <= 0), 'position'] = 'Stop'
            df.loc[(df['high'] < df['upper']) * (df['MAangle20'] >= 0), 'position'] = 'Up'

            df = df.drop(['close_Push', 'close_Diff', 'ma20_Push', 'ma20_Diff', 'ma20_rate'], axis=1)
            df.to_sql('Position', ConToLogicDB, if_exists='replace')

            if Position != df.tail(1)['position'].values[0]:
                Position = df.tail(1)['position'].values[0]
                telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
                                                                                           text='Position %s 발동!!'%(Position))

            print('현재 포지션 :', Position)

            now = datetime.datetime.now()
            # await asyncio.sleep(2 * 60 + 60 - now.second + 2)
            await asyncio.sleep((29 - now.minute % 30) * 60 + 60 - now.second + 4)
    except:
        telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
                                                                                   text='Position Selector Stopeed!!')
        telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
                                                                                   text=traceback.format_exc())

async def Logic_Setting(count):
    global dic, List

    try:
        now = datetime.datetime.now()
        # await asyncio.sleep(60 - now.second+2)
        print('Logic Setting Run')
        await asyncio.sleep(60 - now.second+10)
        while True:
            now = datetime.datetime.now()
            dic = New_Logic.OneMinLogic(dic)
            if now.minute%5 == 0:
                dic = New_Logic.newlogic(dic)
                dic = Master(dic, Position)
                dic = Alert_Watcher(dic)
                balance = upbit.get_balances()
                for i in range(1, len(balance)):
                    Owned_list.append('KRW-%s' % balance[i]['currency'])
                try:
                    Owned_list.remove('KRW-LUNC')
                except:
                    pass
                if len(Owned_list) > 0 :
                    Screen_List = list(Screen.head(count).index)
                    temp = list(set(Owned_list) - set(Screen_List))
                    Screen_List.extend(temp)
                    List = Screen_List
            if now.hour%12 == 9 and now.minute == 0 :
                Ticker_Screen_Auto(count)

            now = datetime.datetime.now()
            await asyncio.sleep(60 - now.second+0.5)
    except:
        telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349, text='LogicSetting Stopped!')
        telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
                                                                                   text=traceback.format_exc())
        logging.error(traceback.format_exc())

async def response_message(Socket):
    global dic, zz
    try:
        now = datetime.datetime.now()
        Loss_Cut = -0.05
        Win_Cut = 0.05
        for i in range(0, len(dic)):

            if dic[i]['ticker'] == Socket.loc[0]['code']:
                if dic[i]['Acc_Volume'] > Socket.loc[0]['acc_trade_volume']:
                    dic[i]['Last_Volume'] = dic[i]['Acc_Volume']

                dic[i]['trade_price'] = Socket.loc[0]['trade_price']
                dic[i]['Acc_Volume'] = Socket.loc[0]['acc_trade_volume']

                if dic[i]['Initial_Sig'] == 0:
                    df = pyupbit.get_ohlcv(Socket.loc[0]['code'], interval='minute1', count=200)
                    df[:-1].to_sql(Socket.loc[0]['code'], ConToTemp, if_exists='replace')

                    dic[i]['Temp_Volume'] = Socket.loc[0]['acc_trade_volume']
                    # dic[i]['Min_Volume'] = df[-1:]['volume'][0]
                    dic[i]['low'] = df[-1:]['low'][0]
                    dic[i]['high'] = df[-1:]['high'][0]
                    dic[i]['open'] = df[-1:]['open'][0]
                    dic[i]['Initial_Sig'] += 1
                    dic[i]['Temp_min'] = now.minute

                if dic[i]['Initial_Sig'] == 1:
                    if dic[i]['Acc_Volume'] - dic[i]['Temp_Volume'] > dic[i]['Mean_Vol']*6 and dic[i]['Mean_Vol'] != 0 and dic[i]['Trade_Mode'] == '5min' and dic[i]['balance'] != 0:
                        if Socket.loc[0]['trade_price'] >= dic[i]['open']*1.015 :
                            dic = Trading_Executer(dic, 'Buy_Now', dic[i]['ticker'], 30000, 0)
                            print(dic[i]['ticker'], '급등주 매수!!')
                            telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(
                                chat_id=1184586349, text='급등주 매수!!')
                            dic[i]['Trade_Mode'] = now

                    if dic[i]['avg_buy_price'] != 0:
                        if dic[i]['Trade_Mode'] != '5min':
                            Earning_Per = (dic[i]['trade_price'] - dic[i]['avg_buy_price'])/dic[i]['avg_buy_price']
                            if dic[i]['Trade_Mode'].replace(second=0) + datetime.timedelta(minutes=1) <= now and Socket.loc[0]['trade_price'] < dic[i]['Target_1min'] and Earning_Per > 0.01:
                                dic = Trading_Executer(dic,'Sell_Now', dic[i]['ticker'])
                                print(dic[i]['ticker'], '급등주 매도!! 손익률 : %s'%(Earning_Per))
                                telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(
                                    chat_id=1184586349, text='급등주 매도!! 손익률 : %s'%(Earning_Per))
                                dic[i]['Trade_Mode'] = '5min'

                    if dic[i]['Trade_Mode'] == '5min':
                        if dic[i]['balance'] != 0 and dic[i]['trade_price'] != 0:
                            Earning_Per = (dic[i]['trade_price'] - dic[i]['avg_buy_price']) / dic[i]['avg_buy_price']
                            if Earning_Per <= Loss_Cut:
                                dic = Trading_Executer(dic, 'Sell_Now', dic[i]['ticker'])
                                telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(
                                    chat_id=1184586349,
                                    text='%s Loss_Cut 손절주문!! 손익률:%s / 손익금액:%s' % (
                                    dic[i]['ticker'], round(Earning_Per,5), Earning_Per*dic[i]['trade_price']*dic[i]['balance']))
                                print('손절')
                            elif Earning_Per >= Win_Cut:
                                dic = Trading_Executer(dic,'Sell_Now', dic[i]['ticker'])
                                telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(
                                    chat_id=1184586349,
                                    text='%s Win_Cut 익절주문!! 손익률:%s / 손익금액:%s' % (
                                    dic[i]['ticker'], round(Earning_Per,5), Earning_Per*dic[i]['trade_price']*dic[i]['balance']))
                                print('익절')

                    if Socket.loc[0]['trade_price'] >= dic[i]['open']*1.005 :
                        dic[i]['UpDown'] = 1
                    if Socket.loc[0]['trade_price'] <= dic[i]['open'] * 0.995 :
                        dic[i]['UpDown'] = -1

                if abs(Socket.loc[0]['trade_price']-dic[i]['trade_price'])/dic[i]['trade_price']>0.05 and dic[i]['trade_price'] != 0:
                    telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
                                                                                               text='이상한 수치 감지 %s %s %s'%(dic[i]['ticker'], Socket.loc[0]['trade_price'], dic[i]['trade_price']))
                    pass
                else:
                    if dic[i]['low'] > Socket.loc[0]['trade_price']:
                        dic[i]['low'] = Socket.loc[0]['trade_price']
                    if dic[i]['high'] < Socket.loc[0]['trade_price']:
                        dic[i]['high'] = Socket.loc[0]['trade_price']

    except:
        telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
                                                                                   text=traceback.format_exc())
        telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349, text='Chart Recorder Stopped!')
        telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349, text=dic)

def OnOffSwitch(update, context):
    ConToTemp = sqlite3.connect("C:/Users/bbs68/PycharmProjects/Bitcoin/DB/WebSocketTrading.db")
    user_id = update.effective_chat.id
    user_text = update.message.text
    if user_text == 'start':
        context.bot.send_message(chat_id=user_id, text='Trade Start')
        dd = {'Date': [datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')], 'State': [user_text]}
        Final_Data = pd.DataFrame(dd)
        Final_Data.to_sql('log', ConToTemp, if_exists='append')

    if user_text == 'stop':
        context.bot.send_message(chat_id=user_id, text='Trade Stop')
        dd = {'Date': [datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')], 'State': [user_text]}
        Final_Data = pd.DataFrame(dd)
        Final_Data.to_sql('log', ConToTemp, if_exists='append')

def TelegrmaMain(token):
    # updater
    updater = Updater(token=token, use_context=True)
    dispatcher = updater.dispatcher

    OnOffSwitch_handler = MessageHandler(Filters.text & (~Filters.command), OnOffSwitch)
    dispatcher.add_handler(OnOffSwitch_handler)

    # polling
    updater.start_polling()

async def Reviver():
    now = datetime.datetime.now()
    await asyncio.sleep(300 - now.second + 30)
    while True :
        try:
            now = datetime.datetime.now()
            await asyncio.sleep(60 - now.second + 30)
            Zero_List = []
            Stunned_List = []
            for tic in List:
                try:
                    df = pd.read_sql("SELECT * FROM '%s'" % (tic), ConToTemp)

                    if df.tail(1)['volume'].values[0] == 0:
                        Zero_List.append(tic)

                    if sum(df.tail(5)['volume']) == 0:
                        Stunned_List.append(tic)

                except pd.io.sql.DatabaseError :
                    telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
                                                                                               text='%s : No Table'%(tic))
                    print(tic, 'No Table')
                    os.execl(sys.executable, sys.executable, *sys.argv)

            if len(Zero_List) >= count/2 :
                print('restart due to Some Stunneds!!')
                for t in Zero_List:
                    cur = ConToTemp.cursor()
                    cur.execute("DELETE FROM '%s' WHERE volume = 0" % t)
                    ConToTemp.commit()
                telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
                                                                                           text='Stunned Some Collector!! : %s'%Zero_List)
                os.execl(sys.executable, sys.executable, *sys.argv)
            if len(Stunned_List) >= 1 :
                print('restart due to one Stunned!!')
                telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
                                                                                           text='Stunned One Collector!! : %s'%Stunned_List)
                for tic in Stunned_List:
                    cur = ConToTemp.cursor()
                    cur.execute("DELETE FROM '%s' WHERE volume = 0" % tic)
                    ConToTemp.commit()
                os.execl(sys.executable, sys.executable, *sys.argv)
        except:
            telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
                                                                                       text=traceback.format_exc())
            telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
                                                                                       text='Reviver Stopped!!')
            print('reviver stopped!!')

def AsyncioTraderMain(count):
    global dic, List

    Ticker_Screening()

    Owned_list = []
    balance = upbit.get_balances()
    for i in range(1, len(balance)):
        Owned_list.append('KRW-%s' % balance[i]['currency'])
    Owned_list.remove('KRW-LUNC')

    Screen = pd.read_sql("SELECT * FROM '%s'" % ('Ticker_Screen'), ConToTemp)
    List_Top10 = Screen.head(15)['ticker'].tolist()
    Execute_List = list(set(Owned_list) - set(List_Top10))

    ##### 비인기 종목 처형 #####
    if len(Execute_List) != 0:
        print('비인기 종목 처형 : %s' %(Execute_List))
        for tic in Execute_List:
            for t in range(0, len(balance)):
                if balance[t]['currency'] == tic[4:]:
                    find_tic_num = t
            Balance = float(balance[find_tic_num]['balance'])
            upbit.sell_market_order(tic, Balance)

        Owned_list = list(set(Owned_list) - set(Execute_List))

    List = Screen.head(count)['ticker'].tolist()
    temp = list(set(Owned_list) - set(List))
    List.extend(temp)

    print(List)

    dic = []
    for i in range(0, len(List)):
        dic.append({'ticker': List[i], 'Initial_Sig': 0, 'Acc_Volume': 0, 'Temp_Volume': 0, 'low': 0, 'high': 0, 'open': 0,
           'Sell_Price':0, 'Buy_Price':0, 'trade_price': 0, 'trade_stop': 0, 'last_order_UUID':'', 'Mean_Vol':0,
                    'Target_1min':0, 'Trade_Mode' : '5min', 'avg_buy_price': 0, 'balance':0, 'Temp_min': 0,
                    'Last_Volume' : 0, 'UpDown' : 0})

    TelegrmaMain("5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk")

    balance = upbit.get_balances()
    for t in range(0, len(balance)):
        find_dic_num = None
        for i in range(0, len(dic)):
            if 'KRW-%s'%(balance[t]['currency']) == dic[i]['ticker']:
                find_dic_num = i
        if find_dic_num != None:
            dic[find_dic_num]['balance'] = float(balance[t]['balance'])
            dic[find_dic_num]['avg_buy_price'] = float(balance[t]['avg_buy_price'])

    DB_Manager(List, 5, 1)

    tasks = [
        asyncio.ensure_future(upbit_ws_client(response_message)),
        # asyncio.ensure_future(WebSocketRuner()),
        # asyncio.ensure_future(Real_Time_Trader()),
        asyncio.ensure_future(Logic_Setting(count)),
        # asyncio.ensure_future(Ticker_Screen_Auto(count)),
        asyncio.ensure_future(Position_Selector()),
        asyncio.ensure_future(Reviver())
        # asyncio.ensure_future(min1_Logic_Setting())
        # asyncio.ensure_future(Logic_Setting(TelegrmaMain("5520527221:AAFo5NbAfbqtFmHzvsmt-KgRYW4g91pHcRU"))),
        # asyncio.ensure_future(Logic_Setting(TelegrmaMain("5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk")))
    ]
    asyncio.get_event_loop().run_until_complete(asyncio.wait(tasks))

if __name__ == '__main__':
    count = 10
    AsyncioTraderMain(count)
