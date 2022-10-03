# # -*- coding: utf-8 -*-
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, time, date, timedelta
import telegram
import traceback
from itertools import combinations

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

def newlogic(dic):

    ############# DownCount Setting ############
    Upper_Limit = 0.3
    Lower_Limit = 0.5
    Ignore_Limit = 0.15
    ############# DownCount Setting ############

    Trend_Change_Row_after = 0
    Trend_Change_Row_mid = 0
    Trend_Change_Row_before = 0

    for k in range(0, len(dic)):

        first = 0
        found = 0
        LIST5 = []
        LIST20 = []
        LIST60 = []

        BTM60List = []

        # now = datetime.datetime.now()
        #
        # try:
        #     min1 = pd.read_sql("SELECT * FROM '%s'" % (dic[k]['ticker']), ConToTemp)
        #     df = min1.tail(5).copy()
        #     df['index'] = pd.to_datetime(df['index'], format='%Y-%m-%d %H:%M:00')
        #     df = df.set_index('index')
        #     df1 = df['open'].resample('5T').first()
        #     df2 = df['high'].resample('5T').max()
        #     df3 = df['low'].resample('5T').min()
        #     df4 = df['close'].resample('5T').last()
        #     df5 = df['volume'].resample('5T').sum()
        #     df = pd.concat([df1, df2, df3, df4, df5], axis=1)
        #     df['value'] = df5[0] * df4[0]
        #     df.to_sql(dic[k]['ticker'], ConToDB, if_exists='append')
        #
        # except:
        #     telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
        #                                                                                text=traceback.format_exc())


        try:
            min5 = pd.read_sql("SELECT * FROM '%s'" % (dic[k]['ticker']), ConToDB)
            last_date_str = min5.tail(1)['index'].values[0]
            last_date = datetime.strptime(last_date_str, '%Y-%m-%d %H:%M:%S')
            next_date_fr = last_date + timedelta(minutes=5)
            next_date_to = next_date_fr + timedelta(minutes=5)
            min1 = pd.read_sql("SELECT * FROM '%s'" % (dic[k]['ticker']), ConToTemp)
            sapre_data = min1.loc[min1['index'] >= next_date_fr.strftime('%Y-%m-%d %H:%M:%S')]
            rounds = len(sapre_data) // 5
            for r in range(0, rounds):
                limit_data = min1.loc[min1['index'] >= next_date_fr.strftime('%Y-%m-%d %H:%M:%S')].loc[
                    min1['index'] < next_date_to.strftime('%Y-%m-%d %H:%M:%S')]
                if len(limit_data) == 5:
                    df = limit_data
                    df['index'] = pd.to_datetime(df['index'], format='%Y-%m-%d %H:%M:00')
                    df = df.set_index('index')
                    df1 = df['open'].resample('5T').first()
                    df2 = df['high'].resample('5T').max()
                    df3 = df['low'].resample('5T').min()
                    df4 = df['close'].resample('5T').last()
                    df5 = df['volume'].resample('5T').sum()
                    df = pd.concat([df1, df2, df3, df4, df5], axis=1)
                    df['value'] = df5[0] * df4[0]
                    df.to_sql(dic[k]['ticker'], ConToDB, if_exists='append')
                last_date += timedelta(minutes=5)
                next_date_fr += timedelta(minutes=5)
                next_date_to += timedelta(minutes=5)
        except:
            print('1분봉 차트 아직 없음')

        # dic[k]['trade_stop'] = 0
        try:
            df = pd.read_sql("SELECT * FROM '%s'" % (dic[k]['ticker']), ConToDB)
            # WebSocketLogic.OneStockGraph_DirectFeed(df)
        except:
            telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
                                                                                       text=traceback.format_exc())
        try:
            df_logic = pd.read_sql("SELECT * FROM '%s'" % (dic[k]['ticker']), ConToLogicDB)
            df_logic = df_logic.drop(['level_0'], axis=1)
            lt = df_logic.tail(1).iloc[0]['index']
        except:
            lt = ''
            df_logic = pd.DataFrame()
        df_cutted = df.loc[df['index'] > lt]
        # df_logic = df_logic.append(df_cutted).copy().reset_index(drop=True)
        df_logic = pd.concat((df_logic, df_cutted)).reset_index(drop=True)
        df = df_logic

        if len(df) > 6:
            dic[k]['Mean_Vol'] = sum(df.tail(6)['volume']) / 30
        else:
            dic[k]['Mean_Vol'] = sum(df['volume']) / len(df) * 5

        for r in range(len(df_logic)-len(df_cutted), len(df_logic)):

            # print(df.iloc[r]['index'])
            ############################# 이동평균선 ############################
            try:
                # for n in range(0, 5):
                #     LIST5.append(df.iloc[r - n]['close'])
                LIST5 = df.iloc[r - 5:r]['close'].values.tolist()
                MA5 = sum(LIST5) / 5
            except:
                MA5 = None

            try:
                LIST10 = df.iloc[r - 10:r]['close'].values.tolist()
                MA10 = sum(LIST10) / 10
            except:
                MA10 = None

            try:
                # for n in range(0, 20):
                #     LIST20.append(df.iloc[r - n]['close'])
                LIST20 = df.iloc[r - 20:r]['close'].values.tolist()
                LOW20 = df.iloc[r - 20:r]['low']
                HIGH20 = df.iloc[r - 20:r]['high']
                MA20 = sum(LIST20) / 20
            except:
                MA20 = None
            try:
                # for n in range(0, 60):
                #     LIST60.append(df.iloc[r - n]['close'])
                LIST60 = df.iloc[r- 60:r]['close'].values.tolist()
                LIST60_ = df.iloc[r-10 - 60:r-10]['close'].values.tolist()
                MA60 = sum(LIST60) / 60
            except:
                MA60 = None


            # try:
            #     if r > 120:
            #         LISTMA60 = df.iloc[r - 60:r]['MA60'].values.tolist()
            #         MAMA60 = sum(LISTMA60) / 60
            #         df.loc[r, 'MAMA60'] = round(MAMA60, 4)
            # except:
            #     df.loc[r, 'MAMA60'] = None
            #
            # try:
            #     if r > 40:
            #         LISTMA20 = df.iloc[r - 20:r]['MA20'].values.tolist()
            #         MAMA20 = sum(LISTMA20) / 20
            #         df.loc[r, 'MAMA20'] = round(MAMA20, 4)
            # except:
            #     df.loc[r, 'MAMA20'] = None

            df.loc[r, 'MA5'] = round(MA5, 4)
            df.loc[r, 'MA20'] = round(MA20, 4)
            df.loc[r, 'MA60'] = round(MA60, 4)
            df.loc[r, 'MA10'] = round(MA10, 4)

            try:
                list_ma = [df.loc[r, 'MA20'], df.loc[r - 1, 'MA20'], df.loc[r - 2, 'MA20']]
                if min(list_ma) == df.loc[r - 1, 'MA20'] and list_ma.count(df.loc[r - 1, 'MA20']) == 1:
                    df.loc[r, 'PreviBTM20'] = min(LIST20)
                elif df.loc[r - 1, 'PreviBTM20'] > df.loc[r, 'close']:
                    df.loc[r, 'PreviBTM20'] = 0
                else:
                    df.loc[r, 'PreviBTM20'] = df.loc[r - 1, 'PreviBTM20']
            except:
                df.loc[r, 'PreviBTM20'] = None

            try:
                list_ma = [df.loc[r, 'MA20'], df.loc[r - 1, 'MA20'], df.loc[r - 2, 'MA20']]
                if max(list_ma) == df.loc[r - 1, 'MA20'] and list_ma.count(df.loc[r - 1, 'MA20']) == 1:
                    df.loc[r, 'PreviTOP20'] = max(LIST20)
                elif df.loc[r - 1, 'PreviTOP20'] < df.loc[r, 'close']:
                    df.loc[r, 'PreviTOP20'] = 0
                else:
                    df.loc[r, 'PreviTOP20'] = df.loc[r - 1, 'PreviTOP20']
            except:
                df.loc[r, 'PreviTOP20'] = None


            if r> 80:
                box_num = 20
                box = np.ones(box_num)/box_num
                df.loc[r-10, 'MA60Smooth'] = sum(df.iloc[r-20:r]['MA60']*box)

            try:
                list_ma = [df.loc[r-10, 'MA60'], df.loc[r-10 - 1, 'MA60'], df.loc[r-10 - 2, 'MA60']]
                if min(list_ma) == df.loc[r-10 - 1, 'MA60'] and list_ma.count(df.loc[r-10 - 1, 'MA60']) == 1:
                    df.loc[r-10, 'PreviBTM60'] = min(LIST60_)
                elif df.loc[r-10 - 1, 'PreviBTM60'] > df.loc[r-10, 'close']:
                    df.loc[r-10, 'PreviBTM60'] = 0
                else:
                    df.loc[r-10, 'PreviBTM60'] = df.loc[r-10 - 1, 'PreviBTM60']
            except:
                df.loc[r, 'PreviBTM60'] = None

            # try:
            #     list_ma = [df.loc[r, 'MA60'], df.loc[r - 1, 'MA60'], df.loc[r - 2, 'MA60']]
            #     if min(list_ma) == df.loc[r - 1, 'MA60'] and list_ma.count(df.loc[r - 1, 'MA60']) == 1:
            #         df.loc[r, 'PreviBTM60'] = min(LIST60)
            #     elif df.loc[r - 1, 'PreviBTM60'] > df.loc[r, 'close']:
            #         df.loc[r, 'PreviBTM60'] = 0
            #     else:
            #         df.loc[r, 'PreviBTM60'] = df.loc[r - 1, 'PreviBTM60']
            # except:
            #     df.loc[r, 'PreviBTM60'] = None

            try:
                list_ma = [df.loc[r, 'MA60'], df.loc[r - 1, 'MA60'], df.loc[r - 2, 'MA60']]
                if max(list_ma) == df.loc[r - 1, 'MA60'] and list_ma.count(df.loc[r - 1, 'MA60']) == 1:
                    df.loc[r, 'PreviTOP60'] = max(LIST60)
                elif df.loc[r - 1, 'PreviTOP60'] < df.loc[r, 'close']:
                    df.loc[r, 'PreviTOP60'] = 0
                else:
                    df.loc[r, 'PreviTOP60'] = df.loc[r - 1, 'PreviTOP60']
            except:
                df.loc[r, 'PreviTOP60'] = None

            try:
                # for n in range(0, 5):
                #     LIST5.append(df.iloc[r - n]['close'])
                LIST20vol = df.iloc[r - 20:r]['volume'].values.tolist()
                MA20vol = sum(LIST20vol) / 20
            except:
                MA20vol = None
            df.loc[r, 'MA20_vol'] = round(MA20vol, 4)

            try:
                if df.loc[r - 1, 'MA5'] < df.loc[r - 1, 'MA20'] and df.loc[r, 'MA5'] > df.loc[r, 'MA20']:
                    if df.iloc[r - 20]['low'] != min(LOW20) or df.iloc[r]['low'] != min(LOW20):
                        df.loc[LOW20.idxmin(), 'Sig_BTM20'] = 1
            except:
                pass

            try:
                if df.loc[r - 1, 'MA5'] > df.loc[r - 1, 'MA20'] and df.loc[r, 'MA5'] < df.loc[r, 'MA20']:
                    df.loc[HIGH20.idxmax(), 'Sig_TOP20'] = 1
            except:
                pass

            if r > 1:
                if df.loc[r, 'PreviBTM60'] != df.loc[r - 1, 'PreviBTM60']:
                    try:
                        Trend_Change_Row_before = Trend_Change_Row_mid
                    except:
                        pass
                    try:
                        Trend_Change_Row_mid = Trend_Change_Row_after
                    except:
                        pass
                    Trend_Change_Row_after = r

            ############################# 이동평균선 ############################

            ############################ MACD #################################
            if r >= 26 :
                df.loc[r, 'MACD'] = sum(df.iloc[r - 12:r]['close'].values.tolist()) / 12 - sum(df.iloc[r - 26:r]['close'].values.tolist()) / 26
            if r >= 35 :
                df.loc[r, 'MACD_Signal'] = sum(df.iloc[r - 9:r]['MACD'].values.tolist()) / 9


            ############################# 밑꼬리 or 촛머리 ############################
            # print(df.iloc[r, OpenCol], df.iloc[r, CloseCol])
            if df.loc[r, 'open'] < df.loc[r, 'close']:
                df.loc[r, 'Tail'] = df.loc[r, 'open'] - df.loc[r, 'low']
                df.loc[r, 'Head'] = df.loc[r, 'high'] - df.loc[r, 'close']
            else:
                df.loc[r, 'Tail'] = df.loc[r, 'close'] - df.loc[r, 'low']
                df.loc[r, 'Head'] = df.loc[r, 'high'] - df.loc[r, 'open']
            ############################# 밑꼬리 or 촛머리 ############################

            ############################# 변동률 ############################
            try:
                df.loc[r, 'Per'] = round((df.loc[r, 'close'] - df.loc[r - 1, 'close']) / df.loc[r - 1, 'close'] * 100,
                                         4)
            except:
                df.loc[r, 'Per'] = None
            # df.loc[r,'Per'] = 0
            ############################# 변동률 ############################

            ############################# Down Cownt Logic ############################
            if first == 0:
                try:
                    DownCount20 = df.iloc[r - 20:r]['DownCount'].values.tolist()
                    DownCount20.reverse()
                    LIST20_ = LIST20.copy()
                    LIST20_.reverse()
                    try:
                        index_1 = DownCount20.index(1)
                    except:
                        index_1 = 100
                    try:
                        index__1 = DownCount20.index(-1)
                    except:
                        index__1 = 100
                    if index_1 == 100 and index__1 == 100:
                        found = 0
                    elif index_1 < index__1:
                        found = index_1
                    elif index_1 >= index__1:
                        found = index__1
                    refer = LIST20_[found]
                except:
                    refer = df.loc[r, 'close']
                first += 1

            if (df.loc[r, 'close'] - refer) / refer * 100 > Upper_Limit:
                df.loc[r, 'DownCount'] = 1
                refer = df.loc[r, 'close']
                # dic[k]['trade_stop'] = 0
            elif (df.loc[r, 'close'] - refer) / refer * 100 < -Lower_Limit:
                df.loc[r, 'DownCount'] = -1
                refer = df.loc[r, 'close']
            elif abs((df.loc[r, 'close'] - refer) / refer * 100) <= Ignore_Limit:
                df.loc[r, 'DownCount'] = 0
            else:
                df.loc[r, 'DownCount'] = None



        #     # print(found, refer, (df.loc[r,'close']-refer)/refer*100 , dic[k]['ticker'])
        #     ############################# Down Cownt Logic ############################
        #
        #     ############################# Rapid Hike ############################
        #     # if df.loc[r,'MA20_vol']*3 > df.loc[r,'volume'] and (df.loc[r, 'MA5']>df.loc[r, 'MA20'] and df.loc[r, 'MA5'] >df.loc[r, 'MA60']):
        #     #     pass
        #     ############################# Rapid Hike ############################
        #
        #     ############################# Trade ############################
        #     BuyPrice = 0
        #     SellPrice = 0
        #
        #     fillna = df.tail(30)['DownCount']
        #     DC20 = fillna.fillna(0).values.tolist()
        #     DC20.reverse()
        #     dddf = df.loc[r - 3:r]
        #     LOW5 = dddf['low'].values.tolist()
        #
        #     if position != 'Stop':
        #         try:
        #             if sum(DC20[0:DC20.index(1)]) <= -3 and fillna.tail(1).values[0] != -1:
        #                 df.loc[r, 'Trade_Up'] = 1
        #                 BuyPrice = df.tail(1)['close'].values[0]
        #             # else:
        #             #     df.loc[r, 'Trade_Up'] = -1
        #             #     SellPrice = df.tail(1)['close'].values[0]
        #         except:
        #             df.loc[r, 'Trade_Up'] = 0
        #
        #         try:
        #             if sum(DC20[0:DC20.index(1)]) <= -3 and fillna.tail(1).values[0] != -1:
        #                 df.loc[r, 'Trade_Down'] = 1
        #                 BuyPrice = df.tail(1)['close'].values[0]
        #             # else:
        #             #     df.loc[r, 'Trade_Down'] = -1
        #             #     SellPrice = df.tail(1)['close'].values[0]
        #         except:
        #             df.loc[r, 'Trade_Down'] = 0
        #
        #         try:
        #             if position == 'Trade_Side':
        #                 if min(LOW5) <= df.loc[r, 'PreviBTM60'] and df.loc[r, 'MA5'] <= df.loc[r, 'close']:
        #                     df.loc[r, 'Trade_Side'] = 1
        #                     BuyPrice = df.tail(1)['close'].values[0]
        #
        #                 elif df.loc[r, 'high'] > df.loc[r, 'PreviTOP20']:
        #                     df.loc[r, 'Trade_Side'] = -1
        #                 SellPrice = df.tail(1)['PreviTOP20'].values[0]
        #         except:
        #             df.loc[r, 'Trade_Side'] = 0
        #
        #         try:
        #             if sum(DC20[0:DC20.index(1)]) <= -3 and fillna.tail(1).values[0] != -1:
        #                 df.loc[r, 'Trade_Side'] = 1
        #                 BuyPrice = df.tail(1)['close'].values[0]
        #             else:
        #                 df.loc[r, 'Trade_Side'] = -1
        #                 SellPrice = df.tail(1)['close'].values[0]
        #         except:
        #             df.loc[r, 'Trade_Side'] = 0
        #
        # if position != 'Stop':
        #     ############################# Trade ############################
        #     if df.tail(1)['Trade_%s' % (position)].values[0] == 1:
        #         dic[k]['Buy_Price'] = BuyPrice
        #     if df.tail(1)['Trade_%s' % (position)].values[0] == -1:
        #         dic[k]['Sell_Price'] = SellPrice

        if len(df) - Trend_Change_Row_after < 120:
            Trend_Change_Row = Trend_Change_Row_mid
        elif len(df) - Trend_Change_Row_mid < 120:
            Trend_Change_Row = Trend_Change_Row_before
        else:
            Trend_Change_Row = Trend_Change_Row_after

        df['lines_Foot'] = 0
        slopes = []
        search = df[Trend_Change_Row - 120:]
        search = search[search['Sig_BTM20'] == 1]
        if len(search) != 0:
            search = search.loc[:, ['low']].copy()

            if len(search.loc[search.idxmin().values[0]:]) != 0 :
                search = search.loc[search.idxmin().values[0]:]
                if len(search) >= 2:
                    for i in combinations(list(range(len(search))), 2):
                        if i[0] == 0:
                            slope = (search.iloc[i[0]].values[0] - search.iloc[i[1]].values[0]) / (
                                        search.iloc[i[0]].name - search.iloc[i[1]].name)
                            slopes.append(round(slope, 4))
                    for r in range(min(search.index.values), len(df)):
                        df.loc[r, 'lines_Foot'] = min(slopes) * (r - search.iloc[0].name) + search.iloc[0].values[0]
            elif search.loc[search.idxmax().values[0]:] != 0 :
                search = search.loc[search.idxmax().values[0]:]
                if len(search) >= 2:
                    for i in combinations(list(range(len(search))), 2):
                        if i[0] == 0:
                            slope = (search.iloc[i[0]].values[0] - search.iloc[i[1]].values[0]) / (
                                        search.iloc[i[0]].name - search.iloc[i[1]].name)
                            slopes.append(round(slope, 4))
                    for r in range(min(search.index.values), len(df)):
                        df.loc[r, 'lines_Foot'] = min(slopes) * (r - search.iloc[0].name) + search.iloc[0].values[0]
            else:
                pass
            df['lines_Cap'] = 0
            slopes = []
            search = df[Trend_Change_Row - 120:]
            search = search[search['Sig_TOP20'] == 1]
            if len(search) != 0:
                search = search.loc[:, ['high']].copy()
                if len(search.loc[search.idxmin().values[0]:]) != 0 :
                    search = search.loc[search.idxmin().values[0]:]
                    if len(search) >= 2:
                        for i in combinations(list(range(len(search))), 2):
                            if i[0] == 0:
                                slope = (search.iloc[i[0]].values[0] - search.iloc[i[1]].values[0]) / (
                                            search.iloc[i[0]].name - search.iloc[i[1]].name)
                                slopes.append(round(slope, 4))
                        for r in range(min(search.index.values), len(df)):
                            df.loc[r, 'lines_Cap'] = min(slopes) * (r - search.iloc[0].name) + search.iloc[0].values[0]
                elif search.loc[search.idxmin().values[0]:] != 0 :
                    search = search.loc[search.idxmin().values[0]:]
                    if len(search) >= 2:
                        for i in combinations(list(range(len(search))), 2):
                            if i[0] == 0:
                                slope = (search.iloc[i[0]].values[0] - search.iloc[i[1]].values[0]) / (
                                            search.iloc[i[0]].name - search.iloc[i[1]].name)
                                slopes.append(round(slope, 4))
                        for r in range(max(search.index.values), len(df)):
                            df.loc[r, 'lines_Cap'] = max(slopes) * (r - search.iloc[0].name) + search.iloc[0].values[0]
                else:
                    pass
        df.to_sql(dic[k]['ticker'], ConToLogicDB, if_exists='replace')
        # print(dic[k]['ticker'], ":", 'Every 5min Calculate Complete', dic[k]['Buy_Price'], dic[k]['Sell_Price'], len(search))

        # WebSocketLogic.OneStockGraph_DirectFeed(df)
    print( datetime.now(), 'Every 5min Calculate Complete', dic)
    return dic

# def newlogic(dic, position):
#
#     ############# DownCount Setting ############
#     Upper_Limit = 0.3
#     Lower_Limit = 0.5
#     Ignore_Limit = 0.15
#     ############# DownCount Setting ############
#     Trend_Change_Row_after = 0
#     Trend_Change_Row_mid = 0
#     Trend_Change_Row_before = 0
#
#     print('start')
#     for k in range(0, len(dic)):
#         first = 0
#         found = 0
#         LIST5 = []
#         LIST20 = []
#         LIST60 = []
#
#         # now = datetime.datetime.now()
#
#         try:
#             min1 = pd.read_sql("SELECT * FROM '%s'" % (dic[k]['ticker']), ConToTemp)
#         except:
#             telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
#                                                                                        text=traceback.format_exc())
#             continue
#
#
#
#         df = min1.tail(5).copy()
#         df['index'] = pd.to_datetime(df['index'], format='%Y-%m-%d %H:%M:00')
#         df = df.set_index('index')
#         df1 = df['open'].resample('5T').first()
#         df2 = df['high'].resample('5T').max()
#         df3 = df['low'].resample('5T').min()
#         df4 = df['close'].resample('5T').last()
#         df5 = df['volume'].resample('5T').sum()
#         df = pd.concat([df1, df2, df3, df4, df5], axis=1)
#         df['value'] = df5[0] * df4[0]
#         df.to_sql(dic[k]['ticker'], ConToDB, if_exists='append')
#
#         # dic[k]['trade_stop'] = 0
#         try:
#             df = pd.read_sql("SELECT * FROM '%s'" % (dic[k]['ticker']), ConToDB)
#         except:
#             telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
#                                                                                        text=traceback.format_exc())
#             continue
#
#         try:
#             df_logic = pd.read_sql("SELECT * FROM '%s'" % (dic[k]['ticker']), ConToLogicDB)
#             df_logic = df_logic.drop(['level_0'], axis=1)
#             lt = df_logic.tail(1).iloc[0]['index']
#         except:
#             lt = ''
#             df_logic = pd.DataFrame()
#         df_cutted = df.loc[df['index']>lt]
#         df_logic = df_logic.append(df_cutted).copy().reset_index(drop=True)
#         df = df_logic
#
#         if len(df) > 6 :
#             dic[k]['Mean_Vol'] = sum(df.tail(6)['volume']) / 30
#         else:
#             dic[k]['Mean_Vol'] = sum(df['volume']) / len(df)*5
#
#         # print(df)
#         BuyPrice = 0
#         SellPrice = 0
#         df['Trade_Up'] = 0
#         df['Trade_Down'] = 0
#         df['Trade_Side'] = 0
#
#         for r in range(len(df_logic)-len(df_cutted), len(df_logic)):
#
#
#             # print(df.iloc[r]['index'])
#             ############################# 이동평균선 ############################
#             try:
#                 # for n in range(0, 5):
#                 #     LIST5.append(df.iloc[r - n]['close'])
#                 LIST5 = df.iloc[r - 5:r]['close'].values.tolist()
#                 MA5 = sum(LIST5) / 5
#             except:
#                 MA5 = None
#             try:
#                 # for n in range(0, 20):
#                 #     LIST20.append(df.iloc[r - n]['close'])
#                 LIST20 = df.iloc[r - 20:r]['close'].values.tolist()
#                 LOW20 = df.iloc[r - 20:r]['low']
#                 HIGH20 = df.iloc[r - 20:r]['high']
#                 MA20 = sum(LIST20) / 20
#             except:
#                 MA20 = None
#             try:
#                 # for n in range(0, 60):
#                 #     LIST60.append(df.iloc[r - n]['close'])
#                 LIST60 = df.iloc[r - 60:r]['close'].values.tolist()
#                 MA60 = sum(LIST60) / 60
#             except:
#                 MA60 = None
#             df.loc[r,'MA5'] = round(MA5,4)
#             df.loc[r,'MA20'] = round(MA20,4)
#             df.loc[r,'MA60'] = round(MA60,4)
#
#             try:
#                 list_ma = [df.loc[r,'MA20'], df.loc[r-1,'MA20'], df.loc[r-2,'MA20']]
#                 if min(list_ma) == df.loc[r-1,'MA20'] and list_ma.count(df.loc[r-1,'MA20']) == 1:
#                     df.loc[r, 'PreviBTM20'] = min(LIST20)
#                 elif df.loc[r-1, 'PreviBTM20'] > df.loc[r, 'close']:
#                     df.loc[r, 'PreviBTM20'] = df.loc[r, 'close']
#                 else:df.loc[r, 'PreviBTM20'] = df.loc[r-1, 'PreviBTM20']
#             except: df.loc[r, 'PreviBTM20'] = None
#
#             try:
#                 list_ma = [df.loc[r, 'MA20'], df.loc[r - 1, 'MA20'], df.loc[r - 2, 'MA20']]
#                 if max(list_ma) == df.loc[r - 1, 'MA20'] and list_ma.count(df.loc[r - 1, 'MA20']) == 1:
#                     df.loc[r, 'PreviTOP20'] = max(LIST20)
#                 elif df.loc[r - 1, 'PreviTOP20'] < df.loc[r, 'close']:
#                     df.loc[r, 'PreviTOP20'] = df.loc[r, 'close']
#                 else:
#                     df.loc[r, 'PreviTOP20'] = df.loc[r - 1, 'PreviTOP20']
#             except: df.loc[r, 'PreviTOP20'] = None
#
#             try:
#                 list_ma = [df.loc[r,'MA60'], df.loc[r-1,'MA60'], df.loc[r-2,'MA60']]
#                 if min(list_ma) == df.loc[r-1,'MA60'] and list_ma.count(df.loc[r-1,'MA60']) == 1:
#                     df.loc[r, 'PreviBTM60'] = min(LIST60)
#                 elif df.loc[r-1, 'PreviBTM60'] > df.loc[r, 'close']:
#                     df.loc[r, 'PreviBTM60'] = df.loc[r, 'close']
#                 else:df.loc[r, 'PreviBTM60'] = df.loc[r-1, 'PreviBTM60']
#             except: df.loc[r, 'PreviBTM60'] = None
#
#             try:
#                 list_ma = [df.loc[r, 'MA60'], df.loc[r - 1, 'MA60'], df.loc[r - 2, 'MA60']]
#                 if max(list_ma) == df.loc[r - 1, 'MA60'] and list_ma.count(df.loc[r - 1, 'MA60']) == 1:
#                     df.loc[r, 'PreviTOP60'] = max(LIST60)
#                 elif df.loc[r - 1, 'PreviTOP60'] < df.loc[r, 'close']:
#                     df.loc[r, 'PreviTOP60'] = df.loc[r, 'close']
#                 else:
#                     df.loc[r, 'PreviTOP60'] = df.loc[r - 1, 'PreviTOP60']
#             except: df.loc[r, 'PreviTOP60'] = None
#
#             try:
#                 # for n in range(0, 5):
#                 #     LIST5.append(df.iloc[r - n]['close'])
#                 LIST20vol = df.iloc[r - 20:r]['volume'].values.tolist()
#                 MA20vol = sum(LIST20vol) / 20
#             except:
#                 MA20vol = None
#             df.loc[r,'MA20_vol'] = round(MA20vol,4)
#
#             try:
#                 if df.loc[r - 1, 'MA5'] < df.loc[r - 1, 'MA20'] and df.loc[r, 'MA5'] > df.loc[r, 'MA20']:
#                     if df.iloc[r - 20]['low'] != min(LOW20) or df.iloc[r]['low'] != min(LOW20):
#                         df.loc[LOW20.idxmin(), 'Sig_BTM20'] = 1
#             except:
#                 pass
#
#             try:
#                 if df.loc[r - 1, 'MA5'] > df.loc[r - 1, 'MA20'] and df.loc[r, 'MA5'] < df.loc[r, 'MA20']:
#                     df.loc[HIGH20.idxmax(), 'Sig_TOP20'] = 1
#             except:
#                 pass
#
#             ############################# 이동평균선 ############################
#
#             ############################# 밑꼬리 or 촛머리 ############################
#             # print(df.iloc[r, OpenCol], df.iloc[r, CloseCol])
#             if df.loc[r,'open'] < df.loc[r,'close']:
#                 df.loc[r,'Tail'] = df.loc[r,'open'] - df.loc[r,'low']
#                 df.loc[r,'Head'] = df.loc[r,'high'] - df.loc[r,'close']
#             else:
#                 df.loc[r,'Tail'] = df.loc[r,'close'] - df.loc[r,'low']
#                 df.loc[r,'Head'] = df.loc[r,'high'] - df.loc[r,'open']
#             ############################# 밑꼬리 or 촛머리 ############################
#
#             ############################# 변동률 ############################
#             try:df.loc[r,'Per'] = round((df.loc[r, 'close']- df.loc[r-1,'close'])/df.loc[r-1,'close']*100,4)
#             except: df.loc[r,'Per'] = None
#             # df.loc[r,'Per'] = 0
#             ############################# 변동률 ############################
#
#             ############################# Down Cownt Logic ############################
#             if first == 0 :
#                 try:
#                     DownCount20 = df.iloc[r - 20:r]['DownCount'].values.tolist()
#                     DownCount20.reverse()
#                     LIST20_ = LIST20.copy()
#                     LIST20_.reverse()
#                     try:index_1 = DownCount20.index(1)
#                     except: index_1 = 100
#                     try:index__1 = DownCount20.index(-1)
#                     except: index__1 = 100
#                     if index_1 == 100 and index__1 == 100:
#                         found = 0
#                     elif index_1 < index__1:
#                         found = index_1
#                     elif index_1 >= index__1 :
#                         found = index__1
#                     refer = LIST20_[found]
#                 except:refer=df.loc[r,'close']
#                 first += 1
#
#
#             if (df.loc[r,'close']-refer)/refer*100 > Upper_Limit :
#                 df.loc[r,'DownCount'] = 1
#                 refer = df.loc[r, 'close']
#                 # dic[k]['trade_stop'] = 0
#             elif (df.loc[r,'close']-refer)/refer*100 < -Lower_Limit :
#                 df.loc[r,'DownCount'] = -1
#                 refer = df.loc[r, 'close']
#             elif abs((df.loc[r,'close']-refer)/refer*100) <= Ignore_Limit:
#                 df.loc[r, 'DownCount'] = 0
#             else:
#                 df.loc[r,'DownCount'] = None
#
#             # print(found, refer, (df.loc[r,'close']-refer)/refer*100 , dic[k]['ticker'])
#             ############################# Down Cownt Logic ############################
#
#             ############################# Rapid Hike ############################
#             # if df.loc[r,'MA20_vol']*3 > df.loc[r,'volume'] and (df.loc[r, 'MA5']>df.loc[r, 'MA20'] and df.loc[r, 'MA5'] >df.loc[r, 'MA60']):
#             #     pass
#             ############################# Rapid Hike ############################
#
#
#             ############################# Trade ############################
#             BuyPrice = 0
#             SellPrice = 0
#
#             fillna = df.tail(30)['DownCount']
#             DC20 = fillna.fillna(0).values.tolist()
#             DC20.reverse()
#             dddf = df.loc[r - 3:r]
#             LOW5 = dddf['low'].values.tolist()
#
#             if position != 'Stop':
#                 try:
#                     if sum(DC20[0:DC20.index(1)]) <= -3 and fillna.tail(1).values[0] != -1:
#                         df.loc[r, 'Trade_Up'] = 1
#                         BuyPrice = df.tail(1)['close'].values[0]
#                     # else:
#                     #     df.loc[r, 'Trade_Up'] = -1
#                     #     SellPrice = df.tail(1)['close'].values[0]
#                 except:
#                     df.loc[r, 'Trade_Up'] = 0
#
#                 try:
#                     if sum(DC20[0:DC20.index(1)]) <= -3 and fillna.tail(1).values[0] != -1:
#                         df.loc[r, 'Trade_Down'] = 1
#                         BuyPrice = df.tail(1)['close'].values[0]
#                     # else:
#                     #     df.loc[r, 'Trade_Down'] = -1
#                     #     SellPrice = df.tail(1)['close'].values[0]
#                 except:
#                     df.loc[r, 'Trade_Down'] = 0
#
#                 try:
#                     if position == 'Trade_Side' :
#                         if min(LOW5) <= df.loc[r, 'PreviBTM60'] and df.loc[r, 'MA5'] <= df.loc[r, 'close']:
#                             df.loc[r, 'Trade_Side'] = 1
#                             BuyPrice = df.tail(1)['close'].values[0]
#
#                         elif df.loc[r, 'high'] > df.loc[r, 'PreviTOP20']:
#                             df.loc[r, 'Trade_Side'] = -1
#                         SellPrice = df.tail(1)['PreviTOP20'].values[0]
#                 except:
#                     df.loc[r, 'Trade_Side'] = 0
#
#                 # try:
#                 #     if sum(DC20[0:DC20.index(1)]) <= -3 and fillna.tail(1).values[0] != -1:
#                 #         df.loc[r, 'Trade_Side'] = 1
#                 #         BuyPrice = df.tail(1)['close'].values[0]
#                 #     else:
#                 #         df.loc[r, 'Trade_Side'] = -1
#                 #         SellPrice = df.tail(1)['close'].values[0]
#                 # except:
#                 #     df.loc[r, 'Trade_Side'] = 0
#
#             if r > 1:
#                 if df.loc[r, 'PreviBTM60'] != df.loc[r - 1, 'PreviBTM60']:
#                     try:
#                         Trend_Change_Row_before = Trend_Change_Row_mid
#                     except:
#                         pass
#                     try:
#                         Trend_Change_Row_mid = Trend_Change_Row_after
#                     except:
#                         pass
#                     Trend_Change_Row_after = r
#
#
#             ############################# Trade ############################
#         if df.tail(1)['Trade_%s'%(position)].values[0] == 1:
#             dic[k]['Buy_Price'] = BuyPrice
#         if df.tail(1)['Trade_%s'%(position)].values[0] == -1:
#             dic[k]['Sell_Price'] = SellPrice
#
#
#         if len(df) - Trend_Change_Row_after < 120:
#             Trend_Change_Row = Trend_Change_Row_mid
#         elif len(df) - Trend_Change_Row_mid < 120:
#             Trend_Change_Row = Trend_Change_Row_before
#         else:
#             Trend_Change_Row = Trend_Change_Row_after
#
#         df['lines'] = 0
#         slopes = []
#         search = df[Trend_Change_Row - 90:][df['Sig_BTM20'] == 1]
#         search = search.loc[:, ['low']].copy()
#         search = search.loc[search.idxmin().values[0]:]
#
#         if len(search) >= 2:
#             for i in combinations(list(range(len(search))), 2):
#                 if i[0] == 0:
#                     slope = (search.iloc[i[0]].values[0] - search.iloc[i[1]].values[0]) / (
#                                 search.iloc[i[0]].name - search.iloc[i[1]].name)
#                     slopes.append(round(slope, 4))
#             print(slopes)
#
#             for r in range(min(search.index.values), len(df)):
#                 df.loc[r, 'lines'] = min(slopes) * (r - search.iloc[0].name) + search.iloc[0].values[0]
#
#
#         df.to_sql(dic[k]['ticker'], ConToLogicDB, if_exists='replace')
#         print(dic[k]['ticker'], ":", 'Every 5min Calculate Complete', dic[k]['Buy_Price'], dic[k]['Sell_Price'])
#
#     return dic

def OneMinLogic(Dict):
    for dic in Dict :

        tic = dic['ticker']


        now = datetime.now()
        TimeForDB = now - timedelta(minutes=1) + timedelta(seconds=2)
        date = datetime(TimeForDB.year, TimeForDB.month, TimeForDB.day, TimeForDB.hour, TimeForDB.minute, 0)

        Trade_Volume = dic['Acc_Volume'] - dic['Temp_Volume']
        if dic['Last_Volume'] != 0 :
            dic['Acc_Volume'] = 0
            dic['Temp_Volume'] = 0
        else:
            dic['Temp_Volume'] = dic['Acc_Volume']
            dic['Last_Volume'] = 0

        if Trade_Volume < 0 :
            telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
                                                                                       text='이상한 수치 감지 %s : %s = %s-%s %s' % (now, Trade_Volume,
                                                                                       dic['Acc_Volume'],
                                                                                       dic['Temp_Volume'],
                                                                                       tic))
        dic['Temp_Volume'] = dic['Acc_Volume']
        dic['Temp_Close'] = dic['trade_price']

        dd = {'index': [date], 'open': [dic['open']], 'close': [dic['trade_price']], 'low': [dic['low']], 'high': [dic['high']],
              'volume': Trade_Volume, 'value': Trade_Volume * dic['trade_price']}
        Final_Data2 = pd.DataFrame(dd)
        DataToAppend = Final_Data2.set_index('index')
        if dic['Initial_Sig'] != 0:
            DataToAppend.to_sql(tic, ConToTemp, if_exists='append')
            # print('logic setting 1min!!!')
        # dic[i]['Min_Volume'] = 0

        dic['low'] = dic['trade_price']
        dic['high'] = dic['trade_price']
        dic['open'] = dic['trade_price']

        try:
            df = pd.read_sql("SELECT * FROM '%s'" % (tic), ConToTemp)
        except:
            dic['Target_1min'] = 0
            print(datetime.now(), '1min DB is Empty!! Logic Precess Passed!!', Dict)
            return Dict

        try:
            df_logic = pd.read_sql("SELECT * FROM '%s'" % (tic), ConToLogicDB_1Min)
            df_logic = df_logic.drop(['level_0'], axis=1)
            lt = df_logic.tail(1).iloc[0]['index']
        except:
            lt = ''
            df_logic = pd.DataFrame()
        df_cutted = df.loc[df['index']>lt]
        df_logic = df_logic.append(df_cutted).copy().reset_index(drop=True)
        df = df_logic

        for r in range(len(df_logic)-len(df_cutted), len(df_logic)):
            # df = pd.read_sql("SELECT * FROM '%s'" % (tic), ConToTemp)
            try:
                LIST5 = df.iloc[r - 5:r]['close'].values.tolist()
                MA5 = sum(LIST5) / 5
            except:
                MA5 = None
            try:
                LIST20 = df.iloc[r - 20:r]['close'].values.tolist()
                MA20 = sum(LIST20) / 20
            except:
                MA20 = None
            try:
                LIST60 = df.iloc[r - 60:r]['close'].values.tolist()
                MA60 = sum(LIST60) / 60
            except:
                MA60 = None
            df.loc[r, 'MA5'] = round(MA5, 4)
            df.loc[r, 'MA20'] = round(MA20, 4)
            df.loc[r, 'MA60'] = round(MA60, 4)

            if r > 20:
                target5 = (sum(df.iloc[r - 19:r]['close'].values.tolist())-sum(df.iloc[r - 4:r]['close'].values.tolist())*4)/3
                df.loc[r, 'target5'] = round(target5, 4)

        df.to_sql(tic, ConToLogicDB_1Min, if_exists='replace')
        dic['Target_1min'] = df.tail(1)['target5'].values[0]

    print(datetime.now(), '1min Logic Complete!!!', Dict)
    return Dict

if __name__ == '__main__':

    dic = [{'ticker': 'KRW-WEMIX', 'Initial_Sig': 0, 'Min_Volume': 0, 'Temp_Volume': 0, 'low': 0, 'high': 0, 'open': 0,
            'Temp_min': 0, 'Sell_Price': 0, 'Buy_Price': 0, 'trade_price': 0, 'trade_stop': 0, 'CountPer': 0,
            'ResetPer': 0,
            'SettlePer': 0, 'last_order_UUID': '', 'Mean_Vol': 0, '1min_trade_stop': 0, 'Target_1min': 0,
            'Trade_Mode': '5min', 'avg_buy_price': 0, 'balance':0}]

    print(newlogic(dic))