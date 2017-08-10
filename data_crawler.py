# -*- coding: utf-8 -*-
from __future__ import division

import os,sys,datetime
import requests, json
from bs4 import BeautifulSoup
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import pandas_datareader.data as web

from data_model import *
from data_handler import *
from data_writer import *
from services import *

class DataCrawler:
    def __init__(self, wait_sec=5):
        self.wait_sec = wait_sec
        self.dbwriter = services.get('dbwriter')
        self.dbhandler = services.get('dbhandler')

    def downloadCode(self, market_type):
        url = 'http://datamall.koscom.co.kr/servlet/infoService/SearchIssue'
        html = requests.post(url, data={'flag': 'SEARCH', 'marketDisabled': 'null', 'marketBit': market_type})
        print(html.content)
        return html.content

    def parseCodeHTML(self, html, market_type):
        soup = BeautifulSoup(html, 'html.parser')
        options = soup.findAll('option')

        codes = StockCode()

        for a_option in options:
            # print a_tr
            if len(a_option) == 0:
                continue

            code = a_option.text[1:7]
            company = a_option.text[8:]
            full_code = a_option.get('value')

            codes.add(market_type, code, full_code, company)

        return codes

    def updateAllCodes(self):
        for market_type in ['kospiVal','kosdaqVal']:
            html = self.downloadCode(market_type)
            codes = self.parseCodeHTML(html,market_type)
            self.dbwriter.updateCodeToDB(codes)


    def downloadStockData(self,market_type,code,year1,month1,date1,year2,month2,date2):
        def makeCode(market_type,code):
            if market_type==1:
                return "%s" % (code)

            return "%s" % (code)

        start = datetime(year1, month1, date1)
        end = datetime(year2, month2, date2)
        try:
            df = web.DataReader(makeCode(market_type,code), "google", start, end)
            return df
        except:
            print("!!! Fatal Error Occurred")
            return None


    def downloadStockData2(self,year1,month1,date1,year2,month2,date2):
        start = datetime(year1, month1, date1)
        end = datetime(year2, month2, date2)
        try:
            df = web.DataReader("KRX:KOSPI", "google", start, end)
            return df
        except:
            print("!!! Fatal Error Occurred")
            return None


    def getDataCount(self,code):
        sql = "select code from prices where code='%s'" % (code)
        rows = self.dbhandler.openSql(sql).fetchall()
        return len(rows)


    def updateAllStockData(self,market_type,year1,month1,date1,year2,month2,date2,start_index=1):
        print ("Start Downloading Stock Data : %s , %s%s%s ~ %s%s%s" % (market_type,year1,month1,date1,year2,month2,date2))

        sql = "select * from codes"
        sql += " where market_type=%s" % (market_type)
        if start_index>1:
            sql += " and id>%s" % (start_index)

        rows = self.dbhandler.openSql(sql).fetchall()

        self.dbhandler.beginTrans()

        index = 1
        for a_row in rows:
            #print a_row
            code = a_row[2]
            company = a_row[5]

            data_count = self.getDataCount(code)
            if data_count == 0:

                print ("... %s of %s : Downloading %s data " % (index,len(rows),company))

                #df_data = self.downloadStockData(market_type,code,year1,month1,date1,year2,month2,date2)
                df_data = self.downloadStockData2(year1,month1,date1,year2,month2,date2)
                if df_data is not None:
                    df_data_indexed = df_data.reset_index()
                    self.dbwriter.updatePriceToDB(code,df_data_indexed)

            index += 1
            #return

        self.dbhandler.endTrans()

        print ("Done!!!")


if __name__ == "__main__":
    services.register('dbhandler', DataHandler())
    services.register('dbwriter', DataWriter())

    crawler = DataCrawler()
    html_codes = crawler.downloadCode('2')
    codes = crawler.parseCodeHTML(html_codes, '2')
    #print(codes)


    crawler.updateAllCodes()
    crawler.updateAllStockData(1,2017,6,1,2017,6,30,start_index=1)


    # finder = StockFinder()
    # finder.setTimePeriod('20150101','20151130')
    # print finder.doStationarityTest('price_close')


