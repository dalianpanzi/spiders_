import urllib.request
import bs4
from bs4 import BeautifulSoup
import datetime
import sqlite3
import pandas as pd
import time
from datetime import datetime

# conn=sqlite3.connect("demo.db")  创建数据库，与project在一个根目录下
#c=conn.cursor()
#c.execute('''create table tidal
             (date_time date not null,
              tide int);''') 参数是datetime 和tide  后面是数据格式

def spider(url):
    html = urllib.request.urlopen(url).read()
    #content=html.decode("utf-8")  变为可读的
    # print(content)
    soup = BeautifulSoup(html, 'html.parser', from_encoding='utf-8')
    res_data = soup.findAll('script')
    tidal_data = str(res_data[7])
    index_start = tidal_data.find("{")
    if index_start<0:
        return False
    else:
        index_end = tidal_data.find("}")
        check = tidal_data[index_start:index_end+1]
        tidal = eval(check)  # 时间是时间戳 后3位不要
        tidal_date = str(res_data[3]).split("/")[5]
        #insert_list = []
        save_in_db(tidal,tidal_date)
        return True
format="%Y-%m-%d %H:%M"
def save_in_db(tidal,tidal_date):
    insert_list=[]
    for key,value in tidal.items():
        if key!="24:00":
            tidal_item = {}
            ti=tidal_date[:4]+"-"+tidal_date[4:6]+"-"+tidal_date[6:]+" "+key
            ti=datetime.strptime(ti,format)
            tidal_item['time']=ti
            tidal_item['level']=value
            insert_list.append(tidal_item)

    conn=sqlite3.connect("E:\***\demo.db")
    c=conn.cursor()
    for item in insert_list:
        c.execute("insert into tidal (date_time,tide) \
            values(?,?)",
                  (item['time'],item['level']))
    conn.commit()
    conn.close()

def build_url(start,end):
    url_list=[]
    base_prefix = "http://www.cxb4.com/gjcxbshow172/"
    df=pd.DataFrame()
    df['t']=pd.date_range(start,end)
    df['year']=df['t'].dt.year
    df['month'] = df['t'].dt.month
    df['day'] = df['t'].dt.day
    for i in range(len(df)):
        y=str(df['year'][i])
        if len(str(df['month'][i]))<2:
            m='0'+str(df['month'][i])
        else:
            m=str(df['month'][i])
        if len(str(df['day'][i]))<2:
            d='0'+str(df['day'][i])
        else:
            d=str(df['day'][i])
        time=y+m+d
        url=base_prefix+time+"/"
        url_list.append(url)
    return url_list

def batch_download(url_list):
    for i in url_list:
        flag = False
        flag=spider(i)
        if flag==False:
            continue


if __name__=="__main__":
    url_list=build_url('20240102','20240221')
    batch_download(url_list)
