# -*- coding: utf-8 -*-
"""
Created on Mon Nov  9 21:05:18 2020

@author: Anne Heggli and Lauren Bolotin
"""
import csv
import requests
import pandas as pd
import re

####HOURLY######
#%% Test URL downlaod
#url (needs to be automated)
##Updated URL with all data and QC flags 
url='https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/customMultiTimeSeriesGroupByStationReport,metric/hourly/start_of_period/428:CA:SNTL%7Cid=%22%22%7Cname/2016-10-01,2017-09-30/TOBS::value,TOBS::qcFlag,PREC::value,PREC::qcFlag,SNWD::value,SNWD::qcFlag,WTEQ::value,WTEQ::qcFlag,SMS:-2:value,SMS:-2:qcFlag,SMS:-8:value,SMS:-8:qcFlag,SMS:-20:value,SMS:-20:qcFlag,STO:-2:value,STO:-2:qcFlag,STO:-8:value,STO:-8:qcFlag,STO:-20:value,STO:-20:qcFlag?fitToScreen=false'
#Origional URL on attempt 1
# url ='https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/customMultiTimeSeriesGroupByStationReport,metric/hourly/start_of_period/428:CA:SNTL%7Cid=%22%22%7Cname/2017-10-01,2018-09-30/TOBS::value,PRCP::value,SNWD::value,WTEQ::value,SMS:-2:value,SMS:-8:value,SMS:-20:value?fitToScreen=false'
with requests.Session() as s:
    download = s.get(url) #downloads

    decoded_content = download.content.decode('utf-8') #decodes
    
    cr = csv.reader(decoded_content.splitlines(), delimiter=',') #splits lines
    my_list = list(cr) #saves to a list
    header=my_list[76] #This is the header line. All other lines before are NRCS metadata. May need to change this!!
    snotel_number=re.sub('[^0-9]','', header[1]) #this strips non numeric characters to find the snotel number
    #LIMITATION: SNOTELS that have a number in the name(e.g. SNTL 848 is Ward Creek #3) outputs as snotel_3848 instead of snotel_848). Manually edit these unles there is a better way to address this in the code. 
    path='/Users/anne/OneDrive/Data/SNOTEL_H/sh_'+snotel_number+'WYtest.csv' #this saves using that snotel number
    
    df=pd.DataFrame(data=my_list[77:],columns=header) #save list to a dataframe
    df.to_csv(path,index=False) #save dataframe to a csv

#%% Automate Hourly Data Download
base='https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/customMultiTimeSeriesGroupByStationReport,metric/hourly/start_of_period/'
            
###LIST DESIRED STATIONS
station = ['428:CA:SNTL']

other ='%7Cid=%22%22%7Cname/'
date ='2000-01-01,2001-01-01/' #we will automate this below
#other variables for easy copy and paste:'WTEQ::value,TAVG::value,TMAX::value,TMIN::value,TOBS::value,PREC::value,PRCP::value,PRCPSA::value,SNWD::value,WTEQ::value,WTEQX::value,SMS:-2:value,SMV:-2:value,STO:-2:value,SNDN::value,SNRR::value' #this could be automated to loop through your list or changed manually
variable ='TOBS::value,TOBS::qcFlag,PREC::value,PREC::qcFlag,SNWD::value,SNWD::qcFlag,WTEQ::value,WTEQ::qcFlag,SMS:-2:value,SMS:-2:qcFlag,SMS:-8:value,SMS:-8:qcFlag,SMS:-20:value,SMS:-20:qcFlag,STO:-2:value,STO:-2:qcFlag,STO:-8:value,STO:-8:qcFlag,STO:-20:value,STO:-20:qcFlag'
end ='?fitToScreen=false'
station_count = 0 #which station to pull

for i in range(2022,2023): #this loops i through from WYstart (not included) to WYend (included) in 1 year chunks
    year=str(i) #beginning of period
    year2=str(i+1) #end of period
    for j in range(len(station)): #this loops j through the stations listed above
        station_id=station[j]
        date=year+'-10-01,'+year2+'-09-30/' #make timestamp
        url=base+station_id+other+date+variable+end #string together url
 
    #now we download using code from above!!!
        with requests.Session() as s:
            download = s.get(url) #downloads
            decoded_content = download.content.decode('utf-8') #decodes
            cr = csv.reader(decoded_content.splitlines(), delimiter=',') #splits lines
            my_list = list(cr) #saves to a list
            header_line=76
            header=my_list[header_line] #this is the 52nd line, all other lines before are NRCS metadata. May need to change this!!

            snotel_number=re.sub('[^0-9]','', header[1]) #this strips non numeric characters to find the snotel number
            path='/Users/anne/OneDrive/Data/SNOTEL_H/sh_'+snotel_number+'_WY'+year2+'.csv' #this saves using that snotel number

            df=pd.DataFrame(data=my_list[header_line+1:],columns=header) #save list to a dataframe
            df.to_csv(path, index=False) #save dataframe to a csv


#%%  DAILY
#######Test URL downlaod DAILY#####
url='https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/customMultiTimeSeriesGroupByStationReport,metric/daily/start_of_period/428:CA:SNTL%7Cid=%22%22%7Cname/2019-04-01,2019-04-30/TAVG::value,TAVG::qcFlag,TMAX::value,TMAX::qcFlag,TMIN::value,TMIN::qcFlag,TOBS::value,TOBS::qcFlag,PREC::value,PREC::qcFlag,PRCP::value,PRCP::qcFlag,PRCPSA::value,PRCPSA::qcFlag,SNWD::value,SNWD::qcFlag,WTEQ::value,WTEQ::qcFlag,SMS:-2:value,SMS:-2:qcFlag,SMS:-8:value,SMS:-8:qcFlag,SMS:-20:value,SMS:-20:qcFlag,SMV:-2:value,SMV:-2:qcFlag,SMV:-8:value,SMV:-8:qcFlag,SMV:-20:value,SMV:-20:qcFlag,SMX:-2:value,SMX:-2:qcFlag,SMX:-8:value,SMX:-8:qcFlag,SMX:-20:value,SMX:-20:qcFlag,SMN:-2:value,SMN:-2:qcFlag,SMN:-8:value,SMN:-8:qcFlag,SMN:-20:value,SMN:-20:qcFlag,STV:-2:value,STV:-2:qcFlag,STV:-8:value,STV:-8:qcFlag,STV:-20:value,STV:-20:qcFlag,STX:-2:value,STX:-2:qcFlag,STX:-8:value,STX:-8:qcFlag,STX:-20:value,STX:-20:qcFlag,STN:-2:value,STN:-2:qcFlag,STN:-8:value,STN:-8:qcFlag,STN:-20:value,STN:-20:qcFlag,STO:-2:value,STO:-2:qcFlag,STO:-8:value,STO:-8:qcFlag,STO:-20:value,STO:-20:qcFlag,SNDN::value,SNDN::qcFlag,SNRR::value,SNRR::qcFlag?fitToScreen=false'
with requests.Session() as s:
    download = s.get(url) #downloads
    decoded_content = download.content.decode('utf-8') #decodes
    cr = csv.reader(decoded_content.splitlines(), delimiter=',') #splits lines
    
    my_list = list(cr) #saves to a list
    header=my_list[127] #all other lines before are NRCS metadata. May need to change this!!
    # snotel_number='428'
    snotel_number=re.sub('[^0-9]','', header[1]) #this strips non numeric characters to find the snotel number
    #I manually changed snotels whose names had a number in them (e.g. SNTL 848 is Ward Creek #3, so it often showed up as snotel_3848 instead of snotel_848)
    #Perhaps you can find a better way to solve this in Python
    path='/Users/anne/OneDrive/Data/SNOTEL_D/sh_'+snotel_number+'_test.csv' #this saves using that snotel number
    
    df=pd.DataFrame(data=my_list[128:],columns=header) #save list to a dataframe
    df.to_csv(path, index=False) #save dataframe to a csv

#%% Automate Daily Data Download 
base='https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/customMultiTimeSeriesGroupByStationReport,metric/daily/start_of_period/'
            
###LIST DESIRED STATIONS
station = ['428:CA:SNTL']

other ='%7Cid=%22%22%7Cname/'
date ='2000-01-01,2001-01-01/' #we will automate this below
variable ='TAVG::value,TAVG::qcFlag,TMAX::value,TMAX::qcFlag,TMIN::value,TMIN::qcFlag,TOBS::value,TOBS::qcFlag,PREC::value,PREC::qcFlag,PRCP::value,PRCP::qcFlag,PRCPSA::value,PRCPSA::qcFlag,SNWD::value,SNWD::qcFlag,WTEQ::value,WTEQ::qcFlag,SMS:-2:value,SMS:-2:qcFlag,SMS:-8:value,SMS:-8:qcFlag,SMS:-20:value,SMS:-20:qcFlag,SMV:-2:value,SMV:-2:qcFlag,SMV:-8:value,SMV:-8:qcFlag,SMV:-20:value,SMV:-20:qcFlag,SMX:-2:value,SMX:-2:qcFlag,SMX:-8:value,SMX:-8:qcFlag,SMX:-20:value,SMX:-20:qcFlag,SMN:-2:value,SMN:-2:qcFlag,SMN:-8:value,SMN:-8:qcFlag,SMN:-20:value,SMN:-20:qcFlag,STV:-2:value,STV:-2:qcFlag,STV:-8:value,STV:-8:qcFlag,STV:-20:value,STV:-20:qcFlag,STX:-2:value,STX:-2:qcFlag,STX:-8:value,STX:-8:qcFlag,STX:-20:value,STX:-20:qcFlag,STN:-2:value,STN:-2:qcFlag,STN:-8:value,STN:-8:qcFlag,STN:-20:value,STN:-20:qcFlag,STO:-2:value,STO:-2:qcFlag,STO:-8:value,STO:-8:qcFlag,STO:-20:value,STO:-20:qcFlag,SNDN::value,SNDN::qcFlag,SNRR::value,SNRR::qcFlag'
end ='?fitToScreen=false'
station_count = 0 #which station to pull

for i in range(2005,2023): #this loops i through from WYstart (not included) to WYend (included) in 1 year chunks
    year=str(i) #beginning of period
    year2=str(i+1) #end of period
    for j in range(len(station)): #this loops j through the stations listed above
        station_id=station[j]
        date=year+'-10-01,'+year2+'-09-30/' #make timestamp
        url=base+station_id+other+date+variable+end #string together url
 
    #now we download using code from above!!!
        with requests.Session() as s:
            download = s.get(url) #downloads
            decoded_content = download.content.decode('utf-8') #decodes
            cr = csv.reader(decoded_content.splitlines(), delimiter=',') #splits lines
            my_list = list(cr) #saves to a list
            header_line=127
            header=my_list[header_line] #all other lines before are NRCS metadata. May need to change this!!

            snotel_number=re.sub('[^0-9]','', header[1]) #this strips non numeric characters to find the snotel number
            path='/Users/anne/OneDrive/Data/SNOTEL_D/sd_'+snotel_number+'_WY'+year2+'.csv' #this saves using that snotel number

            df=pd.DataFrame(data=my_list[header_line+1:],columns=header) #save list to a dataframe
            df.to_csv(path, index=False) #save dataframe to a csv
