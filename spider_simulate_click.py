from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
import requests
import json
from collections import OrderedDict
import pandas as pd
from datetime import datetime, timedelta
import itertools

def simulate(url,text1,text2,text3,date): #date:2024-08-12 url="https://mds.nmdis.org.cn/pages/tidalCurrent.html"
    driver = webdriver.Edge()
    driver.get(url)
    wait=WebDriverWait(driver,10)
    #站点输入
    station_ele = wait.until(EC.visibility_of_element_located((By.CLASS_NAME, 'el-input__suffix-inner')))
    station_ele.click()
    xpath = "//span[contains(text(), '{}')]".format(text1)
    span = wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
    span.click()

    xpath_2 = "//span[contains(text(), '{}')]".format(text2)
    span_2 = wait.until(EC.presence_of_element_located((By.XPATH, xpath_2)))
    span_2.click()

    xpath_3 = "//span[contains(text(), '{}')]".format(text3)
    span_3 = wait.until(EC.presence_of_element_located((By.XPATH, xpath_3)))
    span_3.click()

    #输入时间
    input_element = wait.until(
        EC.visibility_of_element_located((By.CSS_SELECTOR, '.el-input__inner[placeholder="选择日期"]')))
    input_element.clear()
    input_element.send_keys(date)
    input_element.send_keys(Keys.RETURN)
    button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '.bottomPart > button')))
    button.click()
#url='https://mds.nmdis.org.cn/pages/tidalCurrent.html'

    #鼠标移至图标中心位置
    '''
    chart=wait.until(EC.visibility_of_element_located((By.CLASS_NAME,'tidalCharts')))
    location=chart.location
    size=chart.size
    actions = ActionChains(driver)
    actions.move_to_element(chart).perform()
    x = 10
    y = 200
    driver.execute_script(f"window.scrollTo({x}, {y});")
    '''

    #鼠标移动至屏幕指定位置
    '''
    import pyautogui
    pyautogui.move(-100, 0, duration=0.25)
    '''

#请求
def httpRequest(date,sitecode):

    headers = {
        'Accept': 'application/json,text/plain,*/*',
        'Content-Type': 'application/json;charset=utf-8',
        'User-Agent': 'User-Agent:Mozilla/5.0'
    }
    url="https://mds.nmdis.org.cn/service/rdata/front/knowledge/chaoxidata/list"

    data = {
        "serchdate": date,
        "sitecode": sitecode
    }
#sitecode编号：澉浦-T073, 乍浦-T072，嵊山-T071，定海-T080， 大戢山-T070，滩浒-T074
#中断请求确认
    data_json=json.dumps(data)
    response = requests.post(url, data=data_json, headers=headers)
    response=response.text
    return response

#see=httpRequest('2024-08-04','T073')

def get_data(response):
    result={}
    data=json.loads(response)
    for item in data.get('data',[]):
        coor=item['coordinate']
        coor=coor.replace(" ","")
        if 'filedata' in item:
            for key,value in item['filedata'].items():
                if key.startswith('a') and key[1:].isdigit():
                    result[key]=value
    sorted_results = sorted(result.items(), key=lambda item: int(item[0][1:]))
    tidall = [tup[1] for tup in sorted_results]
    return coor,tidall

#coor,sorted_results=get_data(a)

def batch_tidal(dict):#dict{sitecode:[starttime--datetime(2024,1,4)-endtime]}
    tidal_dict={}
    for key,value in dict.items():
        df=pd.DataFrame()
        df['t']=pd.date_range(value[0],value[1],freq='h')
        df.drop(df.index[-1],inplace=True)
        request_date_list=[]
        current_date=value[0]
        end_date=value[1]
        tidal=[]
        while current_date<end_date:
            request_date_list.append(current_date.strftime('%Y-%m-%d'))
            current_date+=timedelta(days=1)
        for d in request_date_list:
            response=httpRequest(d,key)
            coor, tidall=get_data(response)
            tidal.append(tidall)
            #sit_coor =json.dumps({key: coor})
        get_tidal = list(itertools.chain.from_iterable(tidal))
        df.insert(loc=1,column='tidal', value =get_tidal)
        tidal_dict[key]=df
    return tidal_dict

if __name__=="__main__":
  input_dict={'T073':[datetime(2024,7,1),datetime(2024,8,5)]}
  tidal_dict=batch_tidal(input_dict)
