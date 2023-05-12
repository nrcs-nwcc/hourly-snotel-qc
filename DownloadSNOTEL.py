# -*- coding: utf-8 -*-
"""
Created on Mon Nov  9 21:05:18 2020

@author: Anne Heggli and Lauren Bolotin
"""
import csv
import re
from os import makedirs, path

import pandas as pd
import requests

THIS_DIR = path.dirname(path.abspath(__file__))
DATA_DIR = path.join(THIS_DIR, "downloads")
makedirs(DATA_DIR, exist_ok=True)

url = "https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/customMultiTimeSeriesGroupByStationReport,metric/hourly/start_of_period/428:CA:SNTL%7Cid=%22%22%7Cname/2016-10-01,2017-09-30/TOBS::value,TOBS::qcFlag,PREC::value,PREC::qcFlag,SNWD::value,SNWD::qcFlag,WTEQ::value,WTEQ::qcFlag,SMS:-2:value,SMS:-2:qcFlag,SMS:-8:value,SMS:-8:qcFlag,SMS:-20:value,SMS:-20:qcFlag,STO:-2:value,STO:-2:qcFlag,STO:-8:value,STO:-8:qcFlag,STO:-20:value,STO:-20:qcFlag?fitToScreen=false"

with requests.Session() as s:
    download = s.get(url)

    decoded_content = download.content.decode("utf-8")

    cr = csv.reader(decoded_content.splitlines(), delimiter=",")
    my_list = list(cr)
    header = my_list[76]
    snotel_number = re.sub("[^0-9]", "", header[1])
    filepath = path.join(DATA_DIR, f"sh_{snotel_number}_WYtest.csv")

    df = pd.DataFrame(data=my_list[77:], columns=header)
    df.to_csv(filepath, index=False)

base = "https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/customMultiTimeSeriesGroupByStationReport,metric/hourly/start_of_period/"


station = ["428:CA:SNTL"]

other = "%7Cid=%22%22%7Cname/"
date = "2000-01-01,2001-01-01/"
variable = "TOBS::value,TOBS::qcFlag,PREC::value,PREC::qcFlag,SNWD::value,SNWD::qcFlag,WTEQ::value,WTEQ::qcFlag,SMS:-2:value,SMS:-2:qcFlag,SMS:-8:value,SMS:-8:qcFlag,SMS:-20:value,SMS:-20:qcFlag,STO:-2:value,STO:-2:qcFlag,STO:-8:value,STO:-8:qcFlag,STO:-20:value,STO:-20:qcFlag"
end = "?fitToScreen=false"
station_count = 0

for i in range(2022, 2023):
    year = str(i)
    year2 = str(i + 1)
    for j in range(len(station)):
        station_id = station[j]
        date = year + "-10-01," + year2 + "-09-30/"
        url = base + station_id + other + date + variable + end

        with requests.Session() as s:
            download = s.get(url)
            decoded_content = download.content.decode("utf-8")
            cr = csv.reader(decoded_content.splitlines(), delimiter=",")
            my_list = list(cr)
            header_line = 76
            header = my_list[header_line]

            snotel_number = re.sub("[^0-9]", "", header[1])
            filepath = path.join(DATA_DIR, f"sh_{snotel_number}_WY{year2}.csv")
            df = pd.DataFrame(data=my_list[header_line + 1 :], columns=header)
            df.to_csv(filepath, index=False)


url = "https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/customMultiTimeSeriesGroupByStationReport,metric/daily/start_of_period/428:CA:SNTL%7Cid=%22%22%7Cname/2019-04-01,2019-04-30/TAVG::value,TAVG::qcFlag,TMAX::value,TMAX::qcFlag,TMIN::value,TMIN::qcFlag,TOBS::value,TOBS::qcFlag,PREC::value,PREC::qcFlag,PRCP::value,PRCP::qcFlag,PRCPSA::value,PRCPSA::qcFlag,SNWD::value,SNWD::qcFlag,WTEQ::value,WTEQ::qcFlag,SMS:-2:value,SMS:-2:qcFlag,SMS:-8:value,SMS:-8:qcFlag,SMS:-20:value,SMS:-20:qcFlag,SMV:-2:value,SMV:-2:qcFlag,SMV:-8:value,SMV:-8:qcFlag,SMV:-20:value,SMV:-20:qcFlag,SMX:-2:value,SMX:-2:qcFlag,SMX:-8:value,SMX:-8:qcFlag,SMX:-20:value,SMX:-20:qcFlag,SMN:-2:value,SMN:-2:qcFlag,SMN:-8:value,SMN:-8:qcFlag,SMN:-20:value,SMN:-20:qcFlag,STV:-2:value,STV:-2:qcFlag,STV:-8:value,STV:-8:qcFlag,STV:-20:value,STV:-20:qcFlag,STX:-2:value,STX:-2:qcFlag,STX:-8:value,STX:-8:qcFlag,STX:-20:value,STX:-20:qcFlag,STN:-2:value,STN:-2:qcFlag,STN:-8:value,STN:-8:qcFlag,STN:-20:value,STN:-20:qcFlag,STO:-2:value,STO:-2:qcFlag,STO:-8:value,STO:-8:qcFlag,STO:-20:value,STO:-20:qcFlag,SNDN::value,SNDN::qcFlag,SNRR::value,SNRR::qcFlag?fitToScreen=false"
with requests.Session() as s:
    download = s.get(url)
    decoded_content = download.content.decode("utf-8")
    cr = csv.reader(decoded_content.splitlines(), delimiter=",")

    my_list = list(cr)
    header = my_list[127]
    snotel_number = re.sub("[^0-9]", "", header[1])
    filepath = path.join(DATA_DIR, f"sh_{snotel_number}_test.csv")
    df = pd.DataFrame(data=my_list[128:], columns=header)
    df.to_csv(filepath, index=False)

base = "https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/customMultiTimeSeriesGroupByStationReport,metric/daily/start_of_period/"

station = ["428:CA:SNTL"]

other = "%7Cid=%22%22%7Cname/"
date = "2000-01-01,2001-01-01/"
variable = "TAVG::value,TAVG::qcFlag,TMAX::value,TMAX::qcFlag,TMIN::value,TMIN::qcFlag,TOBS::value,TOBS::qcFlag,PREC::value,PREC::qcFlag,PRCP::value,PRCP::qcFlag,PRCPSA::value,PRCPSA::qcFlag,SNWD::value,SNWD::qcFlag,WTEQ::value,WTEQ::qcFlag,SMS:-2:value,SMS:-2:qcFlag,SMS:-8:value,SMS:-8:qcFlag,SMS:-20:value,SMS:-20:qcFlag,SMV:-2:value,SMV:-2:qcFlag,SMV:-8:value,SMV:-8:qcFlag,SMV:-20:value,SMV:-20:qcFlag,SMX:-2:value,SMX:-2:qcFlag,SMX:-8:value,SMX:-8:qcFlag,SMX:-20:value,SMX:-20:qcFlag,SMN:-2:value,SMN:-2:qcFlag,SMN:-8:value,SMN:-8:qcFlag,SMN:-20:value,SMN:-20:qcFlag,STV:-2:value,STV:-2:qcFlag,STV:-8:value,STV:-8:qcFlag,STV:-20:value,STV:-20:qcFlag,STX:-2:value,STX:-2:qcFlag,STX:-8:value,STX:-8:qcFlag,STX:-20:value,STX:-20:qcFlag,STN:-2:value,STN:-2:qcFlag,STN:-8:value,STN:-8:qcFlag,STN:-20:value,STN:-20:qcFlag,STO:-2:value,STO:-2:qcFlag,STO:-8:value,STO:-8:qcFlag,STO:-20:value,STO:-20:qcFlag,SNDN::value,SNDN::qcFlag,SNRR::value,SNRR::qcFlag"
end = "?fitToScreen=false"
station_count = 0

for i in range(2005, 2023):
    year = str(i)
    year2 = str(i + 1)
    for j in range(len(station)):
        station_id = station[j]
        date = year + "-10-01," + year2 + "-09-30/"
        url = base + station_id + other + date + variable + end

        with requests.Session() as s:
            download = s.get(url)
            decoded_content = download.content.decode("utf-8")
            cr = csv.reader(decoded_content.splitlines(), delimiter=",")
            my_list = list(cr)
            header_line = 127
            header = my_list[header_line]

            snotel_number = re.sub("[^0-9]", "", header[1])
            filepath = path.join(DATA_DIR, f"sd_{snotel_number}_WY{year2}.csv")
            df = pd.DataFrame(data=my_list[header_line + 1 :], columns=header)
            df.to_csv(filepath, index=False)
