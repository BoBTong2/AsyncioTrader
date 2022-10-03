# # -*- coding: utf-8 -*-
import sqlite3
import pandas as pd
from datetime import datetime, time, date, timedelta
import time
import telegram
import traceback
import WebSocketLogic
import pyupbit
import logging

# 오류(SettingWithCopyError 발생)
pd.set_option('mode.chained_assignment', 'raise') # SettingWithCopyError
# 경고(SettingWithCopyWarning 발생, 기본 값입니다)
pd.set_option('mode.chained_assignment', 'warn') # SettingWithCopyWarning
# 무시
pd.set_option('mode.chained_assignment',  None) # <==== 경고를 끈다

ConToLogicDB_1Min = sqlite3.connect("C:/Users/bbs68/PycharmProjects/Bitcoin/DB/LogicDB_1Min.db")
ConToLogicDB = sqlite3.connect("C:/Users/bbs68/PycharmProjects/Bitcoin/DB/LogicDB.db")
ConToDB = sqlite3.connect("C:/Users/bbs68/PycharmProjects/Bitcoin/DB/UpbitDB.db")
ConToLogicTest = sqlite3.connect("C:/Users/bbs68/PycharmProjects/Bitcoin/DB/LogicValueTest.db")
ConToTemp = sqlite3.connect("C:/Users/bbs68/PycharmProjects/Bitcoin/DB/WebSocketTrading.db", check_same_thread=False)

access = "OZjdzRxBZkwevRJG31METin4EZvyO9t3Uv2G5jEi"  # access key 직접 입력
secret = "bp9vdlMSTyogRO2O9ub9HQlxhcAuWcWJRaqxzeEc"  # secret key 직접 입력

upbit = pyupbit.Upbit(access, secret)

def Append_Owned_List():
    Owned_list = []
    balance = upbit.get_balances()
    for i in range(1, len(balance)):
        Owned_list.append('KRW-%s' % balance[i]['currency'])
    Owned_list.remove('KRW-LUNC')


def Excuttion_List():
    Owned_list = []
    balance = upbit.get_balances()
    for i in range(1, len(balance)):
        Owned_list.append('KRW-%s' % balance[i]['currency'])
    Owned_list.remove('KRW-LUNC')

    Screen = pd.read_sql("SELECT * FROM '%s'" % ('Ticker_Screen'), ConToTemp)
    List_Top10 = Screen.head(10)['ticker'].tolist()
    Execute_List = list(set(Owned_list) - set(List_Top10))

    for tic in Execute_List:
        for t in range(0, len(balance)):
            if balance[t]['currency'] == tic[4:]:
                find_tic_num = t
        Balance = float(balance[find_tic_num]['balance'])
        ret = upbit.sell_market_order(tic, Balance)

def Sell_All():
    Owned_list = []
    balance = upbit.get_balances()
    for i in range(1, len(balance)):
        Owned_list.append('KRW-%s' % balance[i]['currency'])

    for tic in Owned_list:
        for t in range(0, len(balance)):
            if balance[t]['currency'] == tic[4:]:
                find_tic_num = t
        Balance = float(balance[find_tic_num]['balance'])
        ret = upbit.sell_market_order(tic, Balance)


def findNearNum(exList, values):
    minValue = min(exList, key=lambda x: abs(x - values))
    minIndex = exList.index(minValue)
    return minValue

def BuyLogic_PreviBTMTouch(df):
    try:
        r = len(df) - 1
        dfdf = df['PreviBTM60']
        dfdf = dfdf.dropna(axis=0)
        dfdf = dfdf.drop_duplicates(keep='last')
        BTM_List = dfdf.values.tolist()
        BTM_List.remove(0.0)

        LOW10 = df.iloc[r - 10:r]['low']
        CLOSE10 = df.iloc[r - 10:r]['close']

        list = []
        for i in BTM_List:
            if i > df.loc[LOW10.idxmin(), 'low']*0.99:
                list.append(i)
        if len(list) != 0:
            if min(list) > df.loc[r, 'low'] and min(list) < df.loc[r, 'close']  \
                    and df.loc[r, 'close'] < df.loc[CLOSE10.idxmax(), 'close']\
                    and (abs(min(list) - df.loc[LOW10.idxmin(), 'low'])/df.loc[LOW10.idxmin(), 'low']) <= 0.01:
                BuyPrice = df.loc[r, 'close']
            else:
                BuyPrice = 0
        else:
            BuyPrice = 0
    except:
        BuyPrice = 0

    return BuyPrice

def BuyLogic_MACDSignal(df):
    try:
        last_row = len(df) - 1
        if df.loc[last_row - 1, 'MACD_Signal'] > df.loc[last_row - 1, 'MACD'] \
        and df.loc[last_row, 'MACD_Signal'] < df.loc[last_row, 'MACD']:
            BuyPrice = df.loc[last_row, 'close']
        else:
            BuyPrice = 0
    except:
        BuyPrice = 0

    return BuyPrice

def SellLogic_MACDSignal(df):
    try:
        last_row = len(df) - 1
        if df.loc[last_row - 1, 'MACD_Signal'] < df.loc[last_row - 1, 'MACD'] \
        and df.loc[last_row, 'MACD_Signal'] > df.loc[last_row, 'MACD']:
            SellPrice = df.loc[last_row, 'close']
        else:
            SellPrice = 0
    except:
        SellPrice = 0

    return SellPrice

def BuyLogic_3TicRule(df):
    last_row = len(df) - 1
    fillna = df.tail(30)['DownCount']
    DC20 = fillna.fillna(0).values.tolist()
    DC20.reverse()
    try:
        if sum(DC20[0:DC20.index(1)]) <= -3 :
            if df.tail(1)['open'].values[0] < df.tail(1)['close'].values[0] :
                BuyPrice = df.tail(1)['close'].values[0]
            else:
                BuyPrice = 0
        else:
            BuyPrice = 0
    except:
        BuyPrice = 0
        pass ## DC20.index(1) <= 여기서 1이 없는 경우

    return BuyPrice

def SellLgic_Close_Touch_MA20(df):
    try:
        if df.tail(1)['close'].values[0] < df.tail(1)['MA20'].values[0] and df.tail(1)['open'].values[0] >= df.tail(1)['MA20'].values[0] :
            SellPrice = df.tail(1)['close'].values[0]
        else:
            SellPrice = 0
    except:
        SellPrice = 0

    return SellPrice

def BuyLgic_GoldenCloss_MA5_MA20(df):

    try:
        last_row = len(df) - 1
        if df.loc[last_row - 1, 'MA5'] < df.loc[last_row - 1, 'MA20'] and df.loc[last_row, 'MA5'] > df.loc[last_row, 'MA20']:
            BuyPrice = df.loc[last_row, 'close']
        else:
            BuyPrice = 0
    except:
        BuyPrice = 0

    return BuyPrice

def SellLgic_DeadCloss_MA5_MA10(df):
    try:
        last_row = len(df) - 1
        if df.loc[last_row - 1, 'MA5'] > df.loc[last_row - 1, 'MA10'] and df.loc[last_row, 'MA5'] < df.loc[last_row, 'MA10']:
            SellPrice = df.loc[last_row, 'close']
        else:
            SellPrice = 0
    except:
        SellPrice = 0

    return SellPrice

def Logic_Seletctor(action, position, df):
    if action == 'buy':
        if position == 'Up':
            price = BuyLogic_MACDSignal(df)
        elif position == 'Down':
            price = BuyLogic_PreviBTMTouch(df)
        elif position == 'Side':
            price = BuyLogic_3TicRule(df)

    elif action == 'sell':
        if position == 'Up':
            price = SellLogic_MACDSignal(df)
        elif position == 'Down':
            price=SellLgic_Close_Touch_MA20(df)
        elif position == 'Side':
            price=SellLgic_Close_Touch_MA20(df)

    return price


def Master(dict, position):
    try:
        for dic in dict:
            tic = dic['ticker']

            df_law = pd.read_sql("SELECT * FROM '%s'" % (tic), ConToLogicDB)
            # log_law = pd.read_sql("SELECT * FROM '%s'" % ('Trade_Log'), ConToTemp)
            # log = log_law[log_law['Ticker']==tic]
            # print(log)

            Default_TradingUnit = 20000
            Maximum_investing = Default_TradingUnit*16

            balance = upbit.get_balances()
            KRW_Balance = float(balance[0]['balance'])

            find_tic = tic[4:]
            find_tic_num = None

            for t in range(0, len(balance)):
                if balance[t]['currency'] == find_tic:
                    find_tic_num = t

            if find_tic_num != None:
                Tic_Balance = float(balance[find_tic_num]['balance'])
                Tic_AVG_BuyPrice = float(balance[find_tic_num]['avg_buy_price'])
                Tic_Value = round(Tic_Balance*Tic_AVG_BuyPrice)
                NowPrice = dic['trade_price']
                Earning_Per = round((NowPrice - Tic_AVG_BuyPrice) / Tic_AVG_BuyPrice, 4)
            else:
                Tic_Balance = 0
                Tic_AVG_BuyPrice = 0
                Earning_Per = 0
                Tic_Value = 0

            if Tic_Value != 0 :
                if Earning_Per < -0.02 or Earning_Per > 0.02:
                    dic['trade_stop'] = 0
                else:
                    dic['trade_stop'] = 1
            else:dic['trade_stop'] = 0
            if position != 'Stop':
                if dic['trade_stop'] == 0:
                    ##### 초기진입 ######

                    if Tic_Value == 0 and KRW_Balance >= Default_TradingUnit:
                        if KRW_Balance < Default_TradingUnit:
                            telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
                                                                                                       text='잔고 부족 [현금 : %s / 쓰려는 금액 : %s' % (
                                                                                                       KRW_Balance,
                                                                                                       Default_TradingUnit))
                            print('잔고 부족')
                        else:
                            Buy_Price = Logic_Seletctor('buy', position, df_law)
                            if Buy_Price != 0:
                                dict = Trading_Executer(dict, 'Buy_Now', tic, Default_TradingUnit)
                                pass
                    ##### 물타기 ######
                    elif Tic_Value > 0 and Tic_Value < Maximum_investing - Tic_Value:
                        if KRW_Balance < float(Tic_Value):
                            telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
                                                                                                       text='잔고 부족 [현금 : %s / 쓰려는 금액 : %s' % (
                                                                                                       KRW_Balance,
                                                                                                       Tic_Value))
                            print('잔고 부족')
                        else:
                            Buy_Price = Logic_Seletctor('buy', position, df_law)
                            if Buy_Price != 0:
                                dict = Trading_Executer(dict, 'Buy_Now', tic, Tic_Value)
                                pass

                    ### 탈출 ###
                    if Tic_Value > 0 and Earning_Per > 0.015 and dic['Trade_Mode'] == '5min':
                        SellPrice = Logic_Seletctor('sell', position, df_law)
                        if SellPrice != 0:
                            dict = Trading_Executer(dict, 'Sell_Now', tic)
                            telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
                                                                                                       text=(
                                                                                                                   '탈출 손익률 : %s' % (
                                                                                                               Earning_Per)))
                            print('탈출')

                ## 순환매도 ###
                if dic['Trade_Mode'] == '5min':
                    tic_balance_sum = 0
                    for i in range(1, len(balance)):
                        tic_balance_sum += round(float(balance[i]['balance']) * float(balance[i]['avg_buy_price']), 1)
                    # ratio_KRW = KRW_Balance / (KRW_Balance + tic_balance_sum)
                    # if ratio_KRW < 0.5:
                    if Earning_Per >= 0.005 and Tic_Value > Default_TradingUnit * 1.2:  ## Default_TradingUnit*1.5 <-- 사자마자 순환매도 진입 방지
                        round_num = Tic_Value / Default_TradingUnit
                        Sell_Balance = Tic_Balance*(round_num -1) / round_num
                        dict = Trading_Executer(dict, 'Sell_Now', tic, Sell_Balance)
                        print('순환매도 Balance : %s -> %s' % (round_num, 1))
                        telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,text=('순환매도 Balance : %s -> %s' % (round_num, round((Tic_Balance-Sell_Balance)*dic['trade_price']/Default_TradingUnit,2))))
    except:
        telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349, text='Trading_Executer Stopped!')
        telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
                                                                                   text=traceback.format_exc())
        logging.error(traceback.format_exc())
    return dict


def Trading_Executer(dic, Action, tic, How_Much=None, Ask_Price = None):
    stay_time = 0
    now = datetime.now()
    OrderBook = []
    find_tic_num = None
    find_dic_num = None

    try:
        if Action == 'Buy':
            Order_Balance = float(How_Much/Ask_Price)
            ########### 호가창 다운로드 ###############
            Upbit_OrderBook = pyupbit.get_orderbook(ticker=tic)
            lenlist = len(pyupbit.get_orderbook(ticker=tic)['orderbook_units'])
            for k in range(0, lenlist):
                OrderBook.append(Upbit_OrderBook['orderbook_units'][k]['ask_price'])
            ########### 호가창 다운로드 ###############
            Find_Price = findNearNum(OrderBook, Ask_Price)
            ret = upbit.buy_limit_order(tic, Find_Price, Order_Balance)
            print(ret)
            # ret = upbit.buy_market_order(tic, TradingUnit, contain_req=False) ### 시장가주문
            telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(
                chat_id=1184586349, text='%s매수주문!!!' % (tic))
            data = pd.read_sql("SELECT * FROM '%s'" % (tic), ConToLogicDB)
            WebSocketLogic.OneStockGraph_DirectFeed(data)
            telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').send_document(chat_id=1184586349, document=open(
                "C:/Users/bbs68/PycharmProjects/Bitcoin/DB/fig.html", 'rb'))
            telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').send_photo(chat_id=1184586349, photo=open(
                "C:/Users/bbs68/PycharmProjects/Bitcoin/DB/fig.png", 'rb'))

            log_data = {'Date': [now], 'Ticker': [tic], 'Action': ['Buy'],
                        'Trade_Price': [Ask_Price], 'Balance': [Order_Balance]}
            log_data = pd.DataFrame(log_data).copy()
            log_data.to_sql('Trade_Log', ConToTemp, if_exists='append')

            balance = upbit.get_balances()
            for t in range(0, len(balance)):
                if balance[t]['currency'] == tic[4:]:
                    find_tic_num = t
            for i in range(0, len(dic)):
                if dic[i]['ticker'] == tic:
                    find_dic_num = i
            if find_tic_num != None and find_dic_num != None:
                dic[find_dic_num]['balance'] = float(balance[find_tic_num]['balance'])
                dic[find_dic_num]['avg_buy_price'] = float(balance[find_tic_num]['avg_buy_price'])

            dic[find_dic_num]['last_order_UUID'] = ret['uuid']

        if Action == 'Buy_Now':

            ret = upbit.buy_market_order(tic, How_Much, contain_req=False) ### 시장가주문
            print(ret)
            telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(
                chat_id=1184586349, text='%s매수주문!!!' % (tic))
            data = pd.read_sql("SELECT * FROM '%s'" % (tic), ConToLogicDB)
            WebSocketLogic.OneStockGraph_DirectFeed(data)
            telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').send_document(chat_id=1184586349, document=open(
                "C:/Users/bbs68/PycharmProjects/Bitcoin/DB/fig.html", 'rb'))
            telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').send_photo(chat_id=1184586349, photo=open(
                "C:/Users/bbs68/PycharmProjects/Bitcoin/DB/fig.png", 'rb'))
            while True:
                print('채결 대기...')
                time.sleep(0.2)
                stay_time += 0.2
                ordered_ret = upbit.get_order(ret['uuid'])
                if ordered_ret['state'] == 'cancel':
                    print('채결 완료!!')
                    break
                elif stay_time == 5 :
                    print('체결오류')
                    telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(
                        chat_id=1184586349, text='%s 체결오류' % (tic))
                    break
            log_data = {'Date': [now], 'Ticker': [tic], 'Action': ['Buy'],
                        'Trade_Price': [ordered_ret['trades'][0]['price']], 'Balance': [ordered_ret['trades'][0]['volume']]}
            log_data = pd.DataFrame(log_data).copy()
            log_data.to_sql('Trade_Log', ConToTemp, if_exists='append')

            balance = upbit.get_balances()
            for t in range(0, len(balance)):
                if balance[t]['currency'] == tic[4:]:
                    find_tic_num = t
            for i in range(0, len(dic)):
                if dic[i]['ticker'] == tic:
                    find_dic_num = i
            if find_tic_num != None and find_dic_num != None:
                dic[find_dic_num]['balance'] = float(balance[find_tic_num]['balance'])
                dic[find_dic_num]['avg_buy_price'] = float(balance[find_tic_num]['avg_buy_price'])

            dic[find_dic_num]['last_order_UUID'] = ret['uuid']

        if Action == 'Sell':
            pass

        if Action == 'Sell_Now':
            balance = upbit.get_balances()

            for t in range(0, len(balance)):
                if balance[t]['currency'] == tic[4:]:
                    find_tic_num = t
            if How_Much == None :
                Balance = float(balance[find_tic_num]['balance'])
            else:
                Balance = float(How_Much)

            AVG_BuyPrice = float(balance[find_tic_num]['avg_buy_price'])
            ret = upbit.sell_market_order(tic, Balance)

            print(ret)
            if Ask_Price != None:
                Earning_Per = round((Ask_Price - AVG_BuyPrice)/AVG_BuyPrice,2)
                telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(
                    chat_id=1184586349,
                    text='%s매도주문!!! 손익률:%s / 손익금액:%s' % (tic, Earning_Per, (Ask_Price - AVG_BuyPrice) * Balance))
            else:
                telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(
                    chat_id=1184586349,
                    text='%s 시장가 매도주문!!!' % (tic))
            data = pd.read_sql("SELECT * FROM '%s'" % (tic), ConToLogicDB)
            WebSocketLogic.OneStockGraph_DirectFeed(data)
            telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').send_document(chat_id=1184586349, document=open(
                "C:/Users/bbs68/PycharmProjects/Bitcoin/DB/fig.html", 'rb'))
            telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').send_photo(chat_id=1184586349, photo=open(
                "C:/Users/bbs68/PycharmProjects/Bitcoin/DB/fig.png", 'rb'))
            while True:
                time.sleep(0.2)
                stay_time += 0.2
                print('채결 대기')
                ordered_ret = upbit.get_order(ret['uuid'])
                if ordered_ret['state'] == 'done':
                    print('채결 확인')
                    break
                elif stay_time == 5 :
                    print('체결오류')
                    telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(
                        chat_id=1184586349, text='%s 체결오류' % (tic))
                    break
            log_data = {'Date': [now], 'Ticker': [tic], 'Action': ['Sell'],
                        'Trade_Price': [ordered_ret['trades'][0]['price']], 'Balance': [ordered_ret['trades'][0]['volume']]}
            log_data = pd.DataFrame(log_data).copy()
            log_data.to_sql('Trade_Log', ConToTemp, if_exists='append')

            for i in range(0, len(dic)):
                if dic[i]['ticker'] == tic:
                    find_dic_num = i
            if How_Much != None :
                balance = upbit.get_balances()
                for t in range(0, len(balance)):
                    if balance[t]['currency'] == tic[4:]:
                        find_tic_num = t
                if find_tic_num != None and find_dic_num != None:
                    dic[find_dic_num]['balance'] = float(balance[find_tic_num]['balance'])
                    dic[find_dic_num]['avg_buy_price'] = float(balance[find_tic_num]['avg_buy_price'])
            elif How_Much == None :
                dic[find_dic_num]['balance'] = 0
                dic[find_dic_num]['avg_buy_price'] = 0
            dic[find_dic_num]['last_order_UUID'] = ret['uuid']
            print(dic[find_dic_num])
    except:
        telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349, text='Trading_Master Stopped!')
        telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
                                                                                   text=traceback.format_exc())
        logging.error(traceback.format_exc())

    return dic

if __name__ == '__main__':
    # tic = 'KRW-ETH'
    #
    # df_law = pd.read_sql("SELECT * FROM '%s'" % (tic), ConToLogicDB)
    # print(SellLgic_Close_Touch_MA20(df_law))

    dic = [{'ticker': 'KRW-ADA', 'Initial_Sig': 1, 'Acc_Volume': 80199745.31964406, 'Temp_Volume': 80194071.67072512, 'low': 683.0, 'high': 686.0, 'open': 686.0, 'Sell_Price': 0, 'Buy_Price': 0, 'trade_price': 684.0, 'trade_stop': 0, 'last_order_UUID': '', 'Mean_Vol': 0, 'Target_1min': 0, 'Trade_Mode': '5min', 'avg_buy_price': 661.0, 'balance': 30.25718608, 'Temp_min': 23}, {'ticker': 'KRW-ALGO', 'Initial_Sig': 1, 'Acc_Volume': 118125686.21759044, 'Temp_Volume': 117955590.61311463, 'low': 566.0, 'high': 568.0, 'open': 567.0, 'Sell_Price': 0, 'Buy_Price': 0, 'trade_price': 568.0, 'trade_stop': 0, 'last_order_UUID': '', 'Mean_Vol': 0, 'Target_1min': 0, 'Trade_Mode': '5min', 'avg_buy_price': 522.8776, 'balance': 76.49973487, 'Temp_min': 23}, {'ticker': 'KRW-ATOM', 'Initial_Sig': 1, 'Acc_Volume': 770237.24190733, 'Temp_Volume': 770237.24190733, 'low': 20980.0, 'high': 21020.0, 'open': 21020.0, 'Sell_Price': 0, 'Buy_Price': 0, 'trade_price': 21020.0, 'trade_stop': 0, 'last_order_UUID': '', 'Mean_Vol': 0, 'Target_1min': 0, 'Trade_Mode': '5min', 'avg_buy_price': 20850.0, 'balance': 0.95923261, 'Temp_min': 23}, {'ticker': 'KRW-EOS', 'Initial_Sig': 0, 'Acc_Volume': 0, 'Temp_Volume': 0, 'low': 0, 'high': 0, 'open': 0, 'Sell_Price': 0, 'Buy_Price': 0, 'trade_price': 0, 'trade_stop': 0, 'last_order_UUID': '', 'Mean_Vol': 0, 'Target_1min': 0, 'Trade_Mode': '5min', 'avg_buy_price': 1745.0, 'balance': 11.46131805, 'Temp_min': 0}, {'ticker': 'KRW-ETC', 'Initial_Sig': 1, 'Acc_Volume': 1656903.10671196, 'Temp_Volume': 1654543.33371016, 'low': 41460.0, 'high': 41520.0, 'open': 41510.0, 'Sell_Price': 0, 'Buy_Price': 0, 'trade_price': 41460.0, 'trade_stop': 0, 'last_order_UUID': '', 'Mean_Vol': 0, 'Target_1min': 0, 'Trade_Mode': '5min', 'avg_buy_price': 41925.7991, 'balance': 2.3851662, 'Temp_min': 23}, {'ticker': 'KRW-ETH', 'Initial_Sig': 1, 'Acc_Volume': 19617.56563667, 'Temp_Volume': 19617.08045997, 'low': 1924000.0, 'high': 1926500.0, 'open': 1926500.0, 'Sell_Price': 0, 'Buy_Price': 0, 'trade_price': 1926000.0, 'trade_stop': 0, 'last_order_UUID': '', 'Mean_Vol': 0, 'Target_1min': 0, 'Trade_Mode': '5min', 'avg_buy_price': 0, 'balance': 0, 'Temp_min': 23}, {'ticker': 'KRW-GRS', 'Initial_Sig': 1, 'Acc_Volume': 13112332.51430315, 'Temp_Volume': 13112332.51430315, 'low': 486.0, 'high': 487.0, 'open': 486.0, 'Sell_Price': 0, 'Buy_Price': 0, 'trade_price': 486.0, 'trade_stop': 0, 'last_order_UUID': '', 'Mean_Vol': 0, 'Target_1min': 0, 'Trade_Mode': '5min', 'avg_buy_price': 493.4932, 'balance': 141.84592505, 'Temp_min': 23}, {'ticker': 'KRW-LOOM', 'Initial_Sig': 1, 'Acc_Volume': 130707782.86877447, 'Temp_Volume': 130707782.86877447, 'low': 80.0, 'high': 80.1, 'open': 80.1, 'Sell_Price': 0, 'Buy_Price': 0, 'trade_price': 80.0, 'trade_stop': 0, 'last_order_UUID': '', 'Mean_Vol': 0, 'Target_1min': 0, 'Trade_Mode': '5min', 'avg_buy_price': 80.0728, 'balance': 624.43153166, 'Temp_min': 23}, {'ticker': 'KRW-XRP', 'Initial_Sig': 1, 'Acc_Volume': 757245160.6592335, 'Temp_Volume': 756638326.4150095, 'low': 779.0, 'high': 782.0, 'open': 781.0, 'Sell_Price': 0, 'Buy_Price': 0, 'trade_price': 781.0, 'trade_stop': 0, 'last_order_UUID': '', 'Mean_Vol': 0, 'Target_1min': 0, 'Trade_Mode': '5min', 'avg_buy_price': 698.0, 'balance': 42.97994269, 'Temp_min': 23}]
    print(Master(dic, 'Up'))

