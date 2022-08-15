#imports 
import requests
from selenium import webdriver      
from bs4 import BeautifulSoup

from pprint import pprint

import datetime
import time

import pymongo

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor as APThread
from apscheduler.jobstores.base import JobLookupError



def insert_data_in_db_state(data):
    client = pymongo.MongoClient("mongodb+srv://uff:uff@cluster0.hfnz9bq.mongodb.net/?retryWrites=true&w=majority")
    db = client['COD_BOD']
    cl = db["Database"]
    ins = cl.insert_one(data)
    return ins

def get_last_data(query,check_date):
    client = pymongo.MongoClient("mongodb+srv://uff:uff@cluster0.hfnz9bq.mongodb.net/?retryWrites=true&w=majority")
    db = client['COD_BOD']
    cl = db["Database"]
    z = list(cl.find(query).sort("_id",-1))
    if len(z) == 0:
        return True
    else:
        if z[0]['Date And Time'] != check_date:
            return True
        else:
            return False

def Water_Quality_Parameters(value):
    Data_object = {}
    Parameters = value.find_all("td")

    Temp = []
    for value in Parameters:
        if str(value).find('class="hHeading"') == -1:
            try:
                S = float(value.text)
            except:
                S = value.text
            if isinstance(S, float) == False and S != 'NA':
                Temp.append(S)
            else:
                try:
                    if isinstance(S, float) == False:
                        Data_object[Temp.pop(0)] = None
                    else:
                        Data_object[Temp.pop(0)] = S
                except:
                    pass
    return Data_object

def Main_Function():
    search_url = "http://125.19.52.218/realtime/"
    Search_page = requests.get(search_url)
    soup = BeautifulSoup(Search_page.text, 'html.parser')
    Main_Data = soup.find_all("div",{"class": "row"})
    Main_Data.pop(0)
    
    for value in Main_Data:
        Temp = []
        Dict = {}

        Heading_Data = value.find_all("div",{"class": "hHeading"})
        List = Heading_Data[0].find("h2").text
        List = List.split('-')
        Dict['Station Code'] = List[0]
        Dict['Station Name'] = List[1]
        Dict['Station Type'] = Heading_Data[2].find("h3").text
        Dict['Date And Time'] = Heading_Data[3].find("h3").text
        try:
            Dict['Station Working'] = (Heading_Data[4].find("h4").text).replace(u'\xa0', u'')
        except:
            Dict['Station Working'] = "Working Fine"
        #print(Station_Code,Station_Name,Station_Type,Date)
        #print(Dict['Station Code'])
        if get_last_data({'Station Code': Dict['Station Code']},Dict['Date And Time']) == True:
            Data_object = Water_Quality_Parameters(value)
            Dict['Data Object'] = Data_object
            Dict['creatde_at'] = datetime.datetime.today().replace(microsecond=0)
            print(insert_data_in_db_state(Dict))
            #pprint(Dict)
        else:
            print("No Need for Update")

if __name__ == '__main__':
    executors = {
        "default": APThread(max_workers=20)
    }    
    print("Started")
    scheduler = BackgroundScheduler(executors=executors, daemon=True)
    #scheduler.add_job(func=stater_function, trigger='cron', minute='*', second='1', args=[brokers])
    scheduler.add_job(Main_Function, 'interval',  minutes=10)
    scheduler.start()
    

    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        # Not strictly necessary if daemonic mode is enabled but should be done if possible
        scheduler.shutdown()
