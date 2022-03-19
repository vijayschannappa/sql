import pandas as pd
import pymysql
import os
import sys
from ftplib import FTP
from datetime import datetime, timedelta
from pathlib import Path


creds = {
    "host": "54.69.58.20",
    "db": "RRF_TABLES_TEST",
    "user": "wfsuser",
    "passwd": "REConnect4321!?",
}

end_date = datetime.now().date()
start_date = end_date - timedelta(days=1)

temp = """INSERT INTO DATA_ACTUAL_SSI_RAW_2020 
          (INVERTER_ID,TIMESTAMP,SOURCE_TAG,ATTRIBUTE_1,ATTRIBUTE_2,ATTRIBUTE_3,ATTRIBUTE_4) 
           VALUES ('{}','{}','{}',{},{},{},{});"""

conn = FTP(host='104.211.208.200', user='Devdurga', passwd='DeVduRg@81304321')


def pull_push_data():
    file_list = get_file_list()
    change_working_directory()
    download_files(file_list)


def change_working_directory(self):
    parent_dir = Path(__file__).resolve().parent
    os.chdir(parent_dir)
    sys.path.insert(0, parent_dir)
    output_path = parent_dir / 'data_store' / end_date
    output_path.mkdir(exist_ok=True, parents=True)
    os.chdir(output_path)
    print(output_path)


def get_file_list():
    raw_files = conn.nlst()
    end_date_str = end_date.strftime(format='%Y-%m-%d')
    start_date_str = start_date.strftime(format='%Y-%m-%d')
    actual_files = [
        i for i in raw_files if end_date_str in i or start_date in i]
    return actual_files


def download_
