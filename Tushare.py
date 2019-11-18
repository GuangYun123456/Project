import tushare as ts
import numpy as np
import math
import talib
import time
import threading
import csv
pro = ts.pro_api('c573a914d7c4945b08c8617c335f9da1d8d550924b1a25e1857cd8eb')
# data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name')
# df=ts.get_k_data('600600',start='2019-01-09',end='2019-01-10')000029.SZ

def cutnan(ma):
    for x in range(len(ma)):
        if np.isnan(ma[x]) != True:
            ma=ma[x:len(ma)]
            break
    return ma

def get_stock_list():
    data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name')
    return data

def ma_number(number,closed):
    ma=talib.MA(closed,timeperiod=number)
    ans_ma=0
    for i in ma:
        if math.isnan(i) != True:
            ans_ma=round(i,2)
            break
    return ans_ma

def ma_number_list(number,closed):
    ma=talib.MA(closed,timeperiod=number)
    return ma

def line_same(list1,list2,line_number):
    count=0
    for x in range(len(list1)):
        if list1[x]==list2[x]==1:
           count+=1
    if count>=line_number:
        return True
    else:
        return False

def Anycompare(anynum,cruxnum,*vartuple,gl='g'):
    #print(anynum)
    #print(vartuple)
    line_list=[]
    ans=0
    if gl=='g':
        for var in vartuple:
            if cruxnum>var:
                ans += 1
                line_list.append(1)
            else:
                line_list.append(0)
    elif gl=='l':
        for var in vartuple:
            if cruxnum<var:
                ans += 1
                line_list.append(1)
            else:
                line_list.append(0) 
    else:
        print('error')
    if ans>=anynum:
        return line_list
    else:
        return False

def greatAvg():
    data=get_stock_list()
    for x in range(len(data)):
        if x%200==0 and x != 0:
            break
        df = pro.daily(ts_code=data.iloc[x].loc['ts_code'], start_date='20181001', end_date='20190110')
        if len(df)==0:
            continue
        close=df['close'].values
        todayclose=df.iloc[0].loc['close']
        ma60=sum(close)/60
        ma602=talib.SMA(close,timeperiod=60)
        print(ma60,ma602[len(ma602)-1])
        print(ma602)
        break
        if todayclose>ma60:
            print(data.iloc[x].loc['name'])
        del todayclose,ma60,df

def greatAvg2(startdate,enddate,startindex=0):
    ans_list=[]
    data=get_stock_list()
    for x in range(startindex,len(data)):
        if x%190==0 and x != 0:
            break
        df = pro.daily(ts_code=data.iloc[x].loc['ts_code'], start_date=startdate, end_date=enddate)
        if len(df)==0:
            continue
        close=df['close'].values
        #today=df.iloc[0].loc['trade_date']
        todayclose=df.iloc[0].loc['close']
        todayopen=df.iloc[0].loc['open']
        todayvol=df.iloc[0].loc['vol']
        before5=df[1:6]['vol'].values
        stock_than=todayvol/240/(sum(before5)/1200)
        judge1=Anycompare(3,todayclose,ma_number(5,close),ma_number(10,close),ma_number(20,close),ma_number(30,close),ma_number(60,close),ma_number(180,close),gl='g')
        judge2=Anycompare(3,todayopen,ma_number(5,close),ma_number(10,close),ma_number(20,close),ma_number(30,close),ma_number(60,close),ma_number(180,close),gl='l')
        if todayclose>todayopen and judge1 and judge2 and line_same(judge1,judge2,3) and stock_than>2:
            ans_list.append(data.iloc[x].loc['ts_code'])
        del todayclose,judge1,judge2,df
    return ans_list

def greatAvg3(startdate,enddate,startindex=0):
    #output=sys.stdout
    #outputfile=open("data.txt","w")
    #sys.stdout=outputfile
    datacsv=open('datacsv.csv','w',newline='')
    headers = ['id','ts_code','name','trade_date','todayclose','todayopen','ma60','ma120','ma250','>5%','volume_ratio','turnover_rate','category']
    writer = csv.DictWriter(datacsv, headers)
    writer.writeheader()
    ans_list=[]
    data=get_stock_list()
    conditions_1=0
    conditions_2=0
    for x in range(startindex,len(data)):
        print(x)
        if x%200==0 and x != 0:
            break
            #time.sleep(60)
        df = pro.daily_basic(ts_code=data.iloc[x].loc['ts_code'], start_date=startdate, end_date=enddate)
        df2 = pro.daily(ts_code=data.iloc[x].loc['ts_code'], start_date=startdate, end_date=enddate)
        if len(df)<120 or len(df2)<120:
            continue
        close=df['close'].values
        #today=df.iloc[0].loc['trade_date']
        todayclose=df.iloc[0].loc['close']
        yestodayclose=df.iloc[1].loc['close']
        todayopen=df2.iloc[0].loc['open']
        todayvol=df2.iloc[0].loc['vol']
        before5=df2[1:6]['vol'].values
        stock_than=todayvol/240/(sum(before5)/1200)
        volume_ratio=df.iloc[0].loc['volume_ratio']
        turnover_rate=df.iloc[0].loc['turnover_rate']
        ma250=ma_number(250,close)
        ma120=ma_number(120,close)
        ma60=ma_number(60,close)
        judge1=Anycompare(1,todayclose,ma250,gl='g')
        judge2=Anycompare(1,todayopen,ma250,gl='l')
        judge3=Anycompare(1,ma250,ma60,ma120,gl='g')
        judge4=Anycompare(2,todayclose,ma60,ma120,gl='g')
        if (todayclose-yestodayclose)/yestodayclose>0.05 and ((judge1 and judge2 and judge3) or judge4) and (stock_than>2 or turnover_rate>=5):
            if judge4 !=False and stock_than>2 and turnover_rate>=5:
                ts_code=data.iloc[x].loc['ts_code']
                name=data.iloc[x].loc['name']
                trade_date=df2.iloc[0].loc['trade_date']
                thisdict={'id':x,'ts_code':ts_code,'name':name,'trade_date':trade_date,'todayclose':todayclose,'todayopen':todayopen,'ma60':ma60,'ma120':ma120,'ma250':ma250,'>5%':(todayclose-todayopen)/todayopen,'volume_ratio':volume_ratio,'turnover_rate':turnover_rate,'category':1}
                writer.writerow(thisdict)
                ans_list.append(data.iloc[x].loc['ts_code'])
                conditions_1 += 1
                print('日期',df2.iloc[0].loc['trade_date'])
                print('第',x,'个','换手率:',turnover_rate)
                print(name)
                print('收盘价:',todayclose,'250均线:',ma250,'状态:',judge1)
                print('开盘价:',todayopen,'250均线:',ma250,'状态:',judge2)
                print('250均线:',ma250,'60均线:',ma60,'120均线',ma120,'状态',judge3)
                print('收盘价:',todayclose,'60均线:',ma60,'120均线',ma120,'状态:',judge4)
                print('大于5%:',(todayclose-todayopen)/todayopen)
                print('量比(两个):',stock_than,volume_ratio)
                print('######################','\n')
            elif judge4 ==False:
                ts_code=data.iloc[x].loc['ts_code']
                name=data.iloc[x].loc['name']
                trade_date=df2.iloc[0].loc['trade_date']
                thisdict={'id':x,'ts_code':ts_code,'name':name,'trade_date':trade_date,'todayclose':todayclose,'todayopen':todayopen,'ma60':ma60,'ma120':ma120,'ma250':ma250,'>5%':(todayclose-todayopen)/todayopen,'volume_ratio':volume_ratio,'turnover_rate':turnover_rate,'category':1}
                writer.writerow(thisdict)
                ans_list.append(data.iloc[x].loc['ts_code'])
                conditions_2 += 1
                print('日期',df2.iloc[0].loc['trade_date'])
                print('第',x,'个','换手率:',turnover_rate)
                print(name)
                print('收盘价:',todayclose,'250均线:',ma250,'状态:',judge1)
                print('开盘价:',todayopen,'250均线:',ma250,'状态:',judge2)
                print('250均线:',ma250,'60均线:',ma60,'120均线',ma120,'状态',judge3)
                print('收盘价:',todayclose,'60均线:',ma60,'120均线',ma120,'状态:',judge4)
                print('大于5%:',(todayclose-todayopen)/todayopen)
                print('量比(两个):',stock_than,volume_ratio)
                print('######################','\n')
        del close,judge1,judge2,judge3,df,df2
    #outputfile.close()
    #sys.stdout=output
    datacsv.close()
    return ans_list,conditions_1,conditions_2

print(greatAvg3('20161110','20190115'))
threadLock=threading.Lock()
threads=[]



