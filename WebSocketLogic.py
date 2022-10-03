# # -*- coding: utf-8 -*-
import sqlite3
import pandas as pd
import plotly.graph_objects as go
import plotly.subplots as ms

def OneStockGraph_DirectFeed(df) :
    # df = df.tail(24*24)
    df.reset_index(drop=True)
    df.index = df['index']
    df = df.drop(['index'], axis= 1).copy()
    data_header = list(df)
    default_header = ['index', 'open', 'high', 'low', 'close', 'volume', 'value', 'Tail', 'Head', 'Per', 'level_0',
                      'MA20_vol']
    logic_header = list(set(data_header) - set(default_header))

    df_low = df['low'].to_list()
    df_high = df['high'].to_list()
    r = 0
    m = 0
    candle = go.Candlestick(
        x=df.index,
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
        increasing_line_color='red',  # 상승봉
        decreasing_line_color='blue'  # 하락봉
    )

    """Marker 스타일링 : https://plotly.com/python/marker-style/"""

    fig = ms.make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.02, row_heights=[3, 1, 1])
    fig.add_trace(candle, row=1, col=1)

    marker_list = ['circle', 'square', 'diamond', 'star', 'triangle-up', 'cross', 'x', 'pentagon', 'hexagram', 'hourglass', 'asterisk', 'hash',
                   'circle-open', 'square-open', 'diamond-open', 'star-open', 'triangle-up-open', 'cross-open', 'x-open', 'pentagon-open',
                   'hexagram-open', 'hourglass-open', 'asterisk-open', 'hash-open']

    for i in logic_header:
        signal = [-1, 0, 1]
        dfdf = df[[i]]
        # dfdf = dfdf[i].dropna()
        dfdf = dfdf.dropna(axis=0)
        dfdf = dfdf.drop_duplicates([i])
        df_list = dfdf[i].to_list()
        if len(df_list) == 0:
            continue
        try:
            is_it_signal = list(set(df_list) - set(signal))
            if len(is_it_signal) == 0:
                fig.add_scatter(x=df[df[i] == 1].index, y=df[df[i] == 1]['high']*(1+r), mode='markers', name='%s : 1'%i, marker_size = 10, marker_symbol=marker_list[m])
                m += 1
                fig.add_scatter(x=df[df[i] == -1].index, y=df[df[i] == -1]['high']*(1+r), mode='markers', name='%s : -1'%i,
                                marker_size=10, marker_symbol=marker_list[m])
                m += 1
                r += 0.0005
                continue

            if len(df_list) < len(df) / 4:
                fig.add_scatter(x=df[df[i] > 0].index, y=df[df[i] > 0][i], mode='markers', name=i, marker_symbol='square',
                                marker_size=2)
                continue

            if max(df_list) < min(df_low) * 0.8:
                fig.add_trace(go.Scatter(x=df.index, y=df[i], mode='lines', name=i), row=2, col=1)
                continue
            if min(df_list) > max(df_high) * 1.02:
                try:
                    fig.add_trace(go.Scatter(x=df.index, y=df[i], mode='lines', name=i),
                                  row=2, col=1, secondary_y=True)
                except:
                    fig.add_trace(go.Scatter(x=df.index, y=df[i], mode='lines', name=i),
                                  row=2, col=1)
                continue

            fig.add_scatter(x=df[df[i] > 0].index, y=df[df[i] > 0][i], mode='lines', name=i)
        except:
            print('결함 Column : ', i)

    try:
        fig.add_scatter(x=df[df['lines_Foot'] > 0].index, y=df[df['lines_Foot'] > 0]['lines_Foot'], mode='lines', name='lines_Foot')
        fig.add_scatter(x=df[df['lines_Cap'] > 0].index, y=df[df['lines_Cap'] > 0]['lines_Cap'], mode='lines',
                        name='lines_Cap')
    except:
        pass
    try:
        volume_bar = go.Bar(x=df.index, y=df['volume'], name='volume')
        fig.add_trace(volume_bar, row=3, col=1)
        fig.add_trace(
            go.Scatter(x=df[df['MA20_vol'] > 0].index, y=df[df['MA20_vol'] > 0]['MA20_vol'], mode='lines', name='MA20_vol'), row=3, col=1)
    except:
        pass

    fig.update_layout(
        # title='Title',
        # yaxis1_title='Stock Price',
        # yaxis2_title='Volume',
        # xaxis2_title='periods',
        xaxis1_rangeslider_visible=False,
        xaxis2_rangeslider_visible=False,
        xaxis3_rangeslider_visible=False,
    )
    fig.write_image("C:/Users/bbs68/PycharmProjects/Bitcoin/DB/fig.png", format="jpeg", scale=None, width=1500,
                    height=1500)
    fig.write_html("C:/Users/bbs68/PycharmProjects/Bitcoin/DB/fig.html")
    fig.show()

    # # df = data.tail(48 * 24)
    # # df = data
    # # print(df[df['Down_Count'] == 3])
    # # print(df)
    # candle = go.Candlestick(
    #     x=df.index,
    #     open=df['open'],
    #     high=df['high'],
    #     low=df['low'],
    #     close=df['close'],
    #     increasing_line_color='red',  # 상승봉
    #     decreasing_line_color='blue'  # 하락봉
    # )
    #
    # """Marker 스타일링 : https://plotly.com/python/marker-style/"""
    #
    # # Trade_Point = go.Scatter(x=df[df['Trade'] == 1].index, y=df[df['Trade'] == 1]['low']*0.996,  mode='markers',
    # #                          marker_symbol='star', marker_size=25, marker_color="Red")
    # # DownCownt_Point = go.Scatter(x=df[df['Down_Count'] == 1].index, y=df[df['Down_Count'] == 1]['low']*0.998,  mode='markers',
    # #                          marker_symbol='triangle-down-open', marker_size=15, marker_color="Blue")
    # # DownCownt2_Point = go.Scatter(x=df[df['Down_Count'] == 2].index, y=df[df['Down_Count'] == 2]['low']*0.998,  mode='markers',
    # #                          marker_symbol='triangle-down-open-dot', marker_size=15, marker_color="Blue")
    # # DownCownt3_Point = go.Scatter(x=df[df['Down_Count'] == 3].index, y=df[df['Down_Count'] == 3]['low']*0.998,  mode='markers',
    # #                          marker_symbol='triangle-down', marker_size=15, marker_color="Blue")
    # # SettleCount_Point = go.Scatter(x=df[df['Settle_Count'] >= 3].index, y=df[df['Settle_Count']>= 3]['low']*0.998,  mode='markers',
    # #                          marker_symbol='diamond-wide', marker_size=15, marker_color="Yellow")
    # #
    # # # fig = go.Figure(data=[candle, Trade_Point, DownCownt_Point, DownCownt2_Point,DownCownt3_Point, SettleCount_Point])
    #
    # volume_bar = go.Bar(x=df.index, y=df['volume'])
    # fig = ms.make_subplots(rows=4, cols=1, shared_xaxes=True, vertical_spacing=0.02, row_heights = [4,0,0,1])
    # fig.add_trace(candle, row=1, col=1)
    # # fig.add_scatter(x=df[df['Trade'] == 1].index, y=df[df['Trade'] == 1]['low']*0.994,  mode='markers',
    # #                          marker_symbol='triangle-up', marker_size=25, marker_color="Red", row=1, col=1, name = 'Buy')
    # # fig.add_scatter(x=df[df['Trade'] == -1].index, y=df[df['Trade'] == -1]['high']*1.006,  mode='markers',
    # #                          marker_symbol='triangle-down', marker_size=25, marker_color="Blue", row=1, col=1, name = 'Sell')
    #
    # fig.add_scatter(x=df[df['DownCount'] == 1].index, y=df[df['DownCount'] == 1]['low']*0.998,  mode='markers',
    #                          marker_symbol='triangle-down-open', marker_size=10, marker_color="Blue",name ='DownCount')
    # # fig.add_scatter(x=df[df['DownCount'] == 2].index, y=df[df['DownCount'] == 2]['low']*0.998,  mode='markers',
    # #                          marker_symbol='triangle-down-open-dot', marker_size=10, marker_color="Blue",name ='DownCount(2)')
    # # fig.add_scatter(x=df[df['DownCount'] == 3].index, y=df[df['DownCount'] == 3]['low']*0.998,  mode='markers',
    # #                          marker_symbol='triangle-down', marker_size=15, marker_color="Blue",name ='DownCount(3)')
    # # fig.add_scatter(x=df[df['Settle_Count'] >= 2].index, y=df[df['Settle_Count']>= 2]['low']*0.997,  mode='markers',
    # #                          marker_symbol='diamond-wide', marker_size=15, marker_color="Yellow",name ='Settle')
    # # fig.add_scatter(x=df[df['Settle_Count'] == 1].index, y=df[df['Settle_Count']== 1]['low']*0.997,  mode='markers',
    # #                          marker_symbol='diamond-wide-open', marker_size=15, marker_color="Yellow",name ='Settle')
    # # fig.add_scatter(x=df[df['Tail_Count'] == 1].index, y=df[df['Tail_Count']== 1]['high']*1.003,  mode='markers',
    # #                          marker_symbol='hourglass-open', marker_size=15, marker_color="Green",name ='Tail')
    # # fig.add_scatter(x=df[df['Tail_Count'] >= 2].index, y=df[df['Tail_Count']>= 2]['high']*1.003,  mode='markers',
    # #                          marker_symbol='hourglass', marker_size=15, marker_color="Green",name ='Tail')
    #
    # # fig.add_scatter(x=df[df['MA20_TOP'] == 1].index, y=df[df['MA20_TOP']== 1]['high']*1.003,  mode='markers',
    # #                          marker_symbol='star', marker_size=15, marker_color="Blue",name ='MA20_TOP')
    # # fig.add_scatter(x=df[df['MA20_TOP'] == -1].index, y=df[df['MA20_TOP']== -1]['low']*0.997,  mode='markers',
    # #                          marker_symbol='star', marker_size=15, marker_color="Red",name ='MA20_BTM')
    #
    # # fig.add_trace(go.Scatter(x=df[df['lines'] > 0].index, y=df[df['lines'] > 0]['lines'],mode='lines',name='line'))
    # fig.add_scatter(x=df[df['Sig_BTM20'] == 1].index, y=df[df['Sig_BTM20']== 1]['low'],  mode='markers',
    #                          marker_symbol='star', marker_size=15, marker_color="orange",name ='Sig_BTM20')
    # fig.add_scatter(x=df[df['Sig_TOP20'] == 1].index, y=df[df['Sig_TOP20']== 1]['high'],  mode='markers',
    #                          marker_symbol='star', marker_size=15, marker_color="green",name ='Sig_TOP20')
    #
    # fig.add_scatter(x=df[df['PreviBTM20'] > 0].index, y=df[df['PreviBTM20'] > 0]['PreviBTM20'],  mode='markers',
    #                          marker_symbol='square', marker_size=4, marker_color="yellow",name ='20BTM')
    # fig.add_scatter(x=df[df['PreviBTM60'] > 0].index, y=df[df['PreviBTM60'] > 0]['PreviBTM60'],  mode='markers',
    #                          marker_symbol='square', marker_size=4, marker_color="orange",name ='60BTM')
    # fig.add_scatter(x=df[df['PreviTOP20'] > 0].index, y=df[df['PreviTOP20'] > 0]['PreviTOP20'],  mode='markers',
    #                          marker_symbol='square', marker_size=4, marker_color="lightGreen",name ='20TOP')
    # fig.add_scatter(x=df[df['PreviTOP60'] > 0].index, y=df[df['PreviTOP60'] > 0]['PreviTOP60'],  mode='markers',
    #                          marker_symbol='square', marker_size=4, marker_color="Green",name ='60TOP')
    #
    # # fig.add_scatter(x=df[df['Sig_BTM20'] == 1].index, y=df[df['Sig_BTM20']== 1]['low']*0.997,  mode='markers',
    # #                          marker_symbol='star', marker_size=15, marker_color="Red",name ='MA20_BTM')
    # fig.add_trace(go.Scatter(x=df[df['lines_Foot'] > 0].index, y=df[df['lines_Foot'] > 0]['lines_Foot'], mode='lines', name='lines_Foot', marker_color="Blue"))
    # fig.add_trace(go.Scatter(x=df[df['lines_Cap'] > 0].index, y=df[df['lines_Cap'] > 0]['lines_Cap'], mode='lines', name='lines_Cap', marker_color="Red"))
    #
    #
    # fig.add_scatter(x=df[df['MA5'] > 0].index, y=df[df['MA5'] > 0]['MA5'], line=dict(color='black', width=0.8), name='ma5')
    # fig.add_scatter(x=df[df['MA20'] > 0].index, y=df[df['MA20'] > 0]['MA20'], line=dict(color='red', width=0.9), name='ma20')
    # fig.add_scatter(x=df[df['MA60'] > 0].index, y=df[df['MA60'] > 0]['MA60'], line=dict(color='green', width=1), name='ma60')
    # fig.add_scatter(x=df[df['MAMA60'] > 0].index, y=df[df['MAMA60'] > 0]['MAMA60'], line=dict(color='white', width=1),
    #                 name='ma60')
    # fig.add_scatter(x=df[df['MAMA20'] > 0].index, y=df[df['MAMA20'] > 0]['MAMA20'], line=dict(color='Yellow', width=1),
    #                 name='ma60')
    # fig.add_trace(volume_bar, row=4, col=1)
    #
    # fig.update_layout(
    #     title='Samsung stock price',
    #     yaxis1_title='Stock Price',
    #     yaxis2_title='Volume',
    #     xaxis2_title='periods',
    #     xaxis1_rangeslider_visible=False,
    #     xaxis2_rangeslider_visible=True,
    # )
    # fig.write_image("C:/Users/bbs68/PycharmProjects/Bitcoin/DB/fig.png", format="jpeg",scale=None, width=1500, height=1000)
    # fig.show()

if __name__ == '__main__':
    ConToDB = sqlite3.connect("C:/Users/bbs68/PycharmProjects/Bitcoin/DB/UpbitDB.db", check_same_thread=False)
    ConToLogicTest = sqlite3.connect("C:/Users/bbs68/PycharmProjects/Bitcoin/DB/LogicValueTest.db",
                                     check_same_thread=False)
    ConToTemp = sqlite3.connect("C:/Users/bbs68/PycharmProjects/Bitcoin/DB/WebSocketTrading.db",
                                check_same_thread=False)
    ConToLogicDB = sqlite3.connect("C:/Users/bbs68/PycharmProjects/Bitcoin/DB/LogicDB.db")

    Symbol = 'KRW-XRP'

    df = pd.read_sql("SELECT * FROM '%s'" % (Symbol), ConToDB)

    OneStockGraph_DirectFeed(df)

