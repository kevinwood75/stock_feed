from queue import Queue
from threading import Thread
import pandas_datareader.data as web
from datetime import date, timedelta
from influxdb import InfluxDBClient
import time

# now = date.today()
now = date.today() - timedelta(2)
start = now.strftime("%Y-%m-%d")
end = now.strftime("%Y-%m-%d")

host = "192.168.2.148"
port = "8086"
user = "influxdblab"
password = "jandrew28"
dbname = "stock_market_test"

client = InfluxDBClient(host, port, user, password, dbname)



def get_tickers():
    with open('ticker-symbols') as f:
       tickers = f.read().splitlines()
    return tickers

def do_work(item):
    try:
       index = item.split(",")[0]
       fname = item.split(",")[1]
       ticker = item.split(",")[2].replace('.','')
       f = web.DataReader(ticker, 'iex', start, end)
       input_json = [
                       {
                          "measurement": "stock_prices",
                          "tags": {
                                "t_tdate": start,
                                "t_ticker": ticker,
                                "t_index": index,
                                "t_open": f.loc[start].get('open', "NA"),
                                "t_high": f.loc[start].get('high', "NA"),
                                "t_low": f.loc[start].get('low', "NA"),
                                "t_close": f.loc[start].get('close', "NA"),
                                "t_volume": f.loc[start].get('volume', "NA")
                          },
                          "fields": {
                                "comapany": fname,
                                "ticker": ticker,
                                "index": index,
                                "open": f.loc[start].get('open', "NA"),
                                "high": f.loc[start].get('high', "NA"),
                                "low": f.loc[start].get('low', "NA"),
                                "close": f.loc[start].get('close', "NA"),
                                "volume": f.loc[start].get('volume', "NA")
                          }
                       }
                    ]
       client.write_points(input_json)       
#       print("{0},{1},{2},{3},{4},{5},{6},{7},{8}".format(start,ticker,fname,index,f.loc[start].get('open', "NA"),f.loc[start].get('high', "NA"),f.loc[start].get('low', "NA"),f.loc[start].get('close', "NA"),f.loc[start].get('volume', "NA")))
    except:
       pass

def worker():
    while True:
        item = q.get()
        do_work(item)
        q.task_done()

q = Queue()
for i in range(12):
     t = Thread(target=worker)
     t.daemon = True
     t.start()

for info in get_tickers():
    q.put(info)

q.join()
