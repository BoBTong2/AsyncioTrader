# # -*- coding: utf-8 -*-
# ch08/08_04.py
import pyupbit
import pandas as pd
import sqlite3
import warnings
import traceback
import telegram
from datetime import datetime, time, date, timedelta
# row 생략 없이 출력
pd.set_option('display.max_rows', None)
# col 생략 없이 출력
pd.set_option('display.max_columns', None)
warnings.filterwarnings('ignore')
ConToDB = sqlite3.connect("C:/Users/bbs68/PycharmProjects/Bitcoin/DB/UpbitDB.db", check_same_thread=False)

def DB_Manager(Ticker_List, Min_Unit, Term_Days = 0):
    print('DB Download Start!!!')
    for tic in Ticker_List:
        try:
            if Term_Days == 0:
                from_date = date(2022, 7, 1)
            else:
                from_date = datetime.now() - timedelta(days=Term_Days)
            round_time = datetime.now() + timedelta(minutes=200 * Min_Unit)

            try:
                df_read = pd.read_sql("SELECT * FROM '%s'" % (tic), ConToDB)
                last_time = df_read.tail(1).iloc[0]['index']
                last_time = datetime.strptime(last_time, '%Y-%m-%d %H:%M:%S')
                if from_date < last_time :
                    from_date = last_time
            except:
                print(tic, 'No DB Table')
                pass

            df = pd.DataFrame()
            while True:
                DB_Min5 = pd.DataFrame(pyupbit.get_ohlcv(tic, "minute%s"%(Min_Unit), count=200, to=round_time))
                DB_Min5 = DB_Min5[:-1].copy()
                df = pd.concat([df, DB_Min5])
                round_time = DB_Min5.iloc[0].name + timedelta(minutes=Min_Unit)
                if round_time < from_date :
                    df = df.sort_index(ascending=True)
                    df = df[df.index > from_date]
                    df.to_sql(tic, ConToDB, if_exists='append')
                    if len(df) != 0:
                        print(tic, '|| from :', from_date, '~ to :', str(df.tail(1).index.values[0])[:10],str(df.tail(1).index.values[0])[11:-10])
                    break
        except:
            telegram.Bot('5486150673:AAEBu5dvSsmNdtd5RRcKxR-yQDM0SwgpFEk').sendMessage(chat_id=1184586349,
                                                                                       text=traceback.format_exc())
            print(tic, ': DB DownLoad Failed')

    # print('DB DownLoad Complete!! :')
    # if Term_Days == 0:
    #     from_date = date(2022, 7, 1)
    # else:
    #     from_date = datetime.now() - timedelta(days=Term_Days)
    # to_date = datetime.now()
    # # Ticker_List = {'KRW-ETH'}
    # for tic in Ticker_List:
    #     Cut = datetime.combine(date(2000, 1, 1), time(0, 0, 0))
    #     if Term_Days == 0:
    #         round_time = datetime.combine(from_date, time(0, 0, 0))
    #     else:round_time = from_date
    #     # print(tic)
    #     try:
    #         df = pd.read_sql("SELECT * FROM '%s'" % (tic), ConToDB)
    #         # last_time = str(df.tail(1).index)[8:27]
    #         last_time = df.tail(1).iloc[0]['index']
    #         str_to_date = datetime.strptime(last_time, '%Y-%m-%d %H:%M:%S')
    #         round_time = str_to_date+timedelta(minutes=Min_Unit)
    #     except:
    #         print('It was No Record In DB : %s'%(tic))
    #
    #     # try:
    #     while True:
    #         round_time += timedelta(hours=200*Min_Unit//60, minutes=200*Min_Unit%60)
    #         if to_date < round_time:
    #             DT =  to_date - (round_time - timedelta(hours=200*Min_Unit//60, minutes=200*Min_Unit%60))
    #             dy = int(DT.days)
    #             hr = int(DT.seconds // 3600)
    #             min = int((DT - timedelta(hours=hr)).seconds // 60)
    #             count = int((dy*24*60 + hr * 60 + min) // Min_Unit)+1
    #             round_time = to_date
    #             DB_Min5 = pd.DataFrame(pyupbit.get_ohlcv(tic, "minute%s"%(Min_Unit), count=count, to=round_time))
    #             DB_Min5 = DB_Min5[:-1].copy()
    #             DB_Min5.to_sql(tic, ConToDB, if_exists='append')
    #             break
    #         else:
    #             print(round_time)
    #             DB_Min5 = pd.DataFrame(pyupbit.get_ohlcv(tic, "minute%s"%(Min_Unit), count=200, to=round_time))
    #             DB_Min5 = DB_Min5.reset_index().copy()
    #             if Cut != 0:
    #                 DB_Min5 = DB_Min5.loc[DB_Min5['index'] > Cut].copy()
    #             # print(str(DB_Min5.tail(1)['index'])[6:25])
    #             # Cut = datetime.strptime(DB_Min5.tail(1).iloc[0]['index'], '%Y-%m-%d %H:%M:%S')
    #             Cut = DB_Min5.tail(1).iloc[0]['index']
    #             DB_Min5.set_index('index', inplace=True)
    #             DB_Min5.to_sql(tic, ConToDB, if_exists='append')
    #     # except:
    #     #     print('DB DownLoad Failed')
    #     sleep(0.1)
    # # print('DB DownLoad Complete!! :')

if __name__ == '__main__':
    DB_Manager(['KRW-BTC'], 30, Term_Days=5)
    # print(pd.DataFrame(pyupbit.get_ohlcv('KRW-BTC', "minute%s"%(5), count=200, to=datetime.combine(date(2022, 10, 1), time(4, 30, 0)))))
    # round_time = datetime.combine(date(2022, 1, 1), time(0, 0, 0))
    # round_time += timedelta(hours=25 , minutes = 61)
    # print(200*30%60)