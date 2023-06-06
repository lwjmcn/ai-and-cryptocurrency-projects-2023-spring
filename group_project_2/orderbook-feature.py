# python 3.9.9
# pandas == 2.0.2
import pandas as pd
from datetime import datetime


def cal_mid_price(gr_bid_level, gr_ask_level, mid_type=None):
    level = 5

    if len(gr_bid_level) > 0 and len(gr_ask_level) > 0:
        bid_top_price = gr_bid_level.iloc[0].price
        bid_top_level_qty = gr_bid_level.iloc[0].quantity
        ask_top_price = gr_ask_level.iloc[0].price
        ask_top_level_qty = gr_ask_level.iloc[0].quantity
        mid_price = (bid_top_price + ask_top_price) * 0.5

        if mid_type == 'wt':
            mid_price = ((gr_bid_level.head(level))['price'].mean() + (gr_ask_level.head(level))['price'].mean()) * 0.5
        elif mid_type == 'mkt':
            mid_price = ((bid_top_price * ask_top_level_qty) + (ask_top_price * bid_top_level_qty)) / (
                        bid_top_level_qty + ask_top_level_qty)

        return mid_price, bid_top_price, ask_top_price, bid_top_level_qty, ask_top_level_qty

    else:
        print("Error: serious cal_mid_price")
        return -1, -1, -2, -1, -1


def cal_book_imbalance(param, gr_bid_level, gr_ask_level, mid_price):

    ratio = param[0]
    level = param[1]
    interval = param[2]

    quant_v_bid = gr_bid_level.quantity ** ratio
    price_v_bid = gr_bid_level.price * quant_v_bid

    quant_v_ask = gr_ask_level.quantity ** ratio
    price_v_ask = gr_ask_level.price * quant_v_ask

    askQty = quant_v_ask.values.sum()
    bidPx = price_v_bid.values.sum()
    bidQty = quant_v_bid.values.sum()
    askPx = price_v_ask.values.sum()

    book_price = 0  # because of warning, divisible by 0
    if bidQty > 0 and askQty > 0:
        book_price = (((askQty * bidPx) / bidQty) + ((bidQty * askPx) / askQty)) / (bidQty + askQty)

    book_imbalance = (book_price - mid_price) / interval

    return book_imbalance


def get_sim_df(fn):
    print('get_sim_df loading... %s' % fn)
    df = pd.read_csv(fn).apply(pd.to_numeric, errors='ignore')

    group = df.groupby(['timestamp'])
    return group


def main(start_time_param, end_time_param):
    filename = ["2023-05-07-bithumb-btc-orderbook.csv", "2023-05-08-bithumb-btc-orderbook.csv"]

    level = 5
    book_imbalance_params = (0.2, level, 1)

    for i in range(len(filename)):
        feature_filename = str(filename[i][:-13]) + "feature.csv"

        data = get_sim_df(filename[i])

        _dict_indicators = {}
        _timestamp = []
        _mid_price = []
        _mid_price_wt = []
        _mid_price_mkt = []
        _book_imbalance = []

        start_time = datetime.strptime(str(filename[i][:10]) + " " + start_time_param, '%Y-%m-%d %H:%M:%S')
        end_time = datetime.strptime(str(filename[i][:10]) + " " + end_time_param, '%Y-%m-%d %H:%M:%S')

        print('featuring... %s' % feature_filename)

        for gr_o in data:
            timestamp = (gr_o[1].iloc[0])['timestamp']

            convert_timestamp = datetime.strptime(timestamp[:-7], '%Y-%m-%d %H:%M:%S')
            if convert_timestamp < start_time:
                continue
            elif convert_timestamp > end_time:
                break

            gr_o = gr_o[1]
            gr_bid_level = gr_o[(gr_o.type == 0)]
            gr_ask_level = gr_o[(gr_o.type == 1)]

            mid_price, bid, ask, bid_qty, ask_qty = cal_mid_price(gr_bid_level, gr_ask_level)
            mid_price_wt, _, _, _, _ = cal_mid_price(gr_bid_level, gr_ask_level, mid_type='wt')
            mid_price_mkt, _, _, _, _ = cal_mid_price(gr_bid_level, gr_ask_level, mid_type='mkt')
            book_imbalance = cal_book_imbalance(book_imbalance_params, gr_bid_level, gr_ask_level, mid_price)

            _timestamp.append(timestamp)
            _mid_price.append(mid_price)
            _mid_price_wt.append(mid_price_wt)
            _mid_price_mkt.append(mid_price_mkt)
            _book_imbalance.append(book_imbalance)

        book_imbalance_col_name = '%s-%s-%s-%s' % ('book-imbalance',
                                                   book_imbalance_params[0],
                                                   book_imbalance_params[1],
                                                   book_imbalance_params[2])
        _dict_indicators[book_imbalance_col_name] = _book_imbalance
        _dict_indicators['mid_price'] = _mid_price
        _dict_indicators['mid_price_wt'] = _mid_price_wt
        _dict_indicators['mid_price_mkt'] = _mid_price_mkt
        _dict_indicators['timestamp'] = _timestamp

        dict_to_dataframe = pd.DataFrame.from_dict(data=_dict_indicators, orient='columns')

        dict_to_dataframe.to_csv(feature_filename, index=False)


if __name__ == '__main__':
    main("00:00:00", "23:59:59")
