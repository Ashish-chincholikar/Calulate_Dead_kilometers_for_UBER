# -*- coding: utf-8 -*-
"""
Created on Wed Nov 30 12:25:47 2022

@author: user
"""

import pandas as pd
import numpy as np

#importing two files 
df1 = pd.read_csv('c:/Dead_km/Everest_data.csv')
df2 = pd.read_csv('c:/Dead_km/trip_activity_Everest_Master.csv')

#let us check the total columns in df1
df1.columns
#Check the columns in df2
df2.columns
#since we need data of specific daate,for that we need to make Dates column in Date and time format
# import important module
from datetime import datetime
from datetime import date
# call datetime.strptime to
# convert it into datetime datatype

# Convert the date to datetime64
df1['Dates'] = pd.to_datetime(df1['day_b'], format='%Y-%m-%d')
# Filter data between two dates
df1 = df1.loc[(df1['Dates'] < '2022-11-19')]
df1['Dates'] = pd.to_datetime(df1['Dates']).dt.date    
df1.columns
#since Df2 do not have Dates column hence let us create date column assigning values of trip request time
df2['dates']=df2['Trip request time']

df2.columns                 
df2['dates'] = pd.to_datetime(df2['dates'], format='%Y-%m-%d')
  
# Filter data between two dates
df2 = df2.loc[(df2['dates'] < '2022-11-19')]
df2['dates'] = pd.to_datetime(df2['dates']).dt.date 



# Inner joining the dataframe
#In order to join two dataframes ,we need to find column which is common in both,trip uuid has been used

df3 = pd.merge(df1,df2,left_on='uuid',right_on='Trip UUID')
df3.columns

#There are several columns which are not required for further analysis ,hence let us drop them put
#   columns 
df4 = df3.drop(['day_b', 'city_id',  'vehicle_uuid', 'partner_uuid',
       'uuid',  'status', 'is_cash_trip',
        'dropoff_timestamp_local','request_timestamp_local',
        'Trip UUID','Driver first name', 'Driver surname', 'Vehicle UUID', 
       'Service type', 'Trip request time', 'Trip drop-off time','Trip distance',
       'dates'], axis=1)
df4.columns

#let us export the df4 to excel
df4.to_excel('c:/Dead_km_final/final_dataframe.xlsx', index=True)
############################################################################################
# There is very long address fields,we need to select only subburn name,for that we need to apply 
#NLP process


from gensim.models import word2vec
import pandas as pd
import os
#let us import the desired file which has source address,destination address ,latitude and longitude
df=pd.read_excel("c:/Dead_km_final/final_dataframe.xlsx")
df.shape

#There are certain columns which are not useful,let us drop these columns
df=df.drop(['begintrip_lng.1','Dates','Driver UUID'],axis=1)
df.columns       
#There are certain columns having diffrent names,let us rename the columns
df=df.rename(columns={'begintrip_lat':'Lat_s',
                   'begintrip_lng':'Lon_s',
                   'dropoff_lat':'Lat_d',
                   'dropoff_lng':'Lon_d',
                   'Pick-up address':'Source_Address', 
                   'Drop-off address':'Destination_Address',
                   'Trip status':'Trip_status',
                   'Number plate':'Number_plate'
                   
                   })


df=df.drop(['destination_lat','Unnamed: 0'],axis=1)           
#df=df.drop(['begintrip_lng.1'],axis=1)        
           
df.columns
#If you want to discard the rows having name plates "KA","HR" etc,then you need to convert into string
#data types
#In order to apply NLP process we need convert all columns into string form
df.dtypes
df=df.convert_dtypes()
df.dtypes
#In the sheet there is data from Karnataka,Tamilnadu,Hariyana
#let us drop rows having word "KA"
discard=["KA"]

df=df[~df.Number_plate.str.contains('|'.join(discard))]


discard=["TS"]
#df.shape
df=df[~df.Number_plate.str.contains('|'.join(discard))]
discard=["HR"]

df=df[~df.Number_plate.str.contains('|'.join(discard))]


#There are requests which has been cancelled either by riders or drivers
#let us drop rows which are have word canceled and unfulfilled in status column

#similarly we need to work on trip_status having "canceled","unfulfilled" words
discard=["canceled"]

df=df[~df.Trip_status.str.contains('|'.join(discard))]

discard=["unfulfilled"]

df=df[~df.Trip_status.str.contains('|'.join(discard))]

discard=["cancelled"]

df=df[~df.Trip_status.str.contains('|'.join(discard))]

discard=["failed"]

df=df[~df.Trip_status.str.contains('|'.join(discard))]
# Now there are rows which are only have rows with service completed

df.Trip_status
#df.drop_duplicates()
#Let us start processing on source_address
#let us convert souce address column to a list
pick_up_address_list=df['Source_Address'].to_list()
pick_up_address_list[0:10]
#let us split the addresses using seperator ","
sentence_stream=[doc.split(",") for doc in pick_up_address_list]
#There are few combinations which require two word combinations ,inorder to do that following code will process the text
from gensim.models import Phrases
from gensim.models.phrases import Phraser
bigram=Phrases(sentence_stream,min_count=2)
bigram_phraser=Phraser(bigram)
#in order to save two combination words let us declare the empty list
token_list=[]

for sent in sentence_stream:
    tokens=bigram_phraser[sent]

    token_list.append(tokens)
print(token_list[0:5])


#We require only Andheri West Mumbai,Parle East mumbai so filter is applied to the list
for i in range(len(token_list)):
    print(token_list[i][-4:-3])

#final_list=['Navi Mumbai'if Navi Mumbai in token_list else token_list[i][-4:-3] for i in range(len(token_list))]
    
final_list=[token_list[i][-4:-3] for i in range(len(token_list))]
#sample['source_add']=final_list

#let us join the token list to create a source address list
source_add=["".join(final_list[i]) for i in range(len(final_list))]
#let us create column name as source address
df["source_add"]=source_add

#Because of this filtering many rows are becoming empty,we need to copy from given address in Source_Address column to souurce_add

#####################################################
# filtering the rows where job is Govt
for index, row in df.iterrows():
    if 'International' in row['Source_Address']:
      df_new=df.Source_Address.replace('International','International Airport',regex=True)
df['Source_Address']=pd.Series(df_new)
#################################################
# filtering the rows where job is Govt
for index, row in df.iterrows():
    if 'Domastic' in row['Source_Address']:
      df_new=df.Source_Address.replace('Domastic','Domastic Airport',regex=True)
df['Source_Address']=pd.Series(df_new)
###################################################

indexes=[]
to_modify=[]

for index, row in df.iterrows():
    if 'Navi' in row['Source_Address']:
       print(index,row['Source_Address'])
       indexes.append(index)
       to_modify.append(row['Source_Address'])

#df['Source_Address']=pd.Series(df_new)

df_new1=df[df["Source_Address"].str.contains("Navi")]
k1=df_new1['Source_Address'].tolist ()
    if 'Navi' in row['Source_Address']:
        old_value=row['
        df_new1=df.Source_Address.to_list()[-6:-4]

sentence_stream1=[doc.split(",") for doc in k1]
#There are few combinations which require two word combinations ,inorder to do that following code will process the text
from gensim.models import Phrases
from gensim.models.phrases import Phraser
bigram1=Phrases(sentence_stream,min_count=2)
bigram_phraser1=Phraser(bigram1)
#in order to save two combination words let us declare the empty list
token_list1=[]

for sent in sentence_stream1:
    tokens1=bigram_phraser1[sent]

    token_list1.append(tokens1)
print(token_list1[0:5])

#We require only Andheri West Mumbai,Parle East mumbai so filter is applied to the list
for i in range(len(token_list1)):
    print(token_list1[i][-3:-2])

#final_list=['Navi Mumbai'if Navi Mumbai in token_list else token_list[i][-4:-3] for i in range(len(token_list))]
    
replacements=[token_list1[i][-3:-2] for i in range(len(token_list1))]
#source_add=["".join(final_list1[i]) for i in range(len(final_list1))]

#Also when iterating through 2 lists in parallel I like to use the zip function (or izip if you're worried about memory consumption, but I'm not one of those iteration purists). So try this instead.

indexes
replacements
to_modify

len1=len(indexes)
for i in range(len1):
    A=indexes[i]
    B=replacements[i]
    df.loc[A,"source_add"]=B

#applying replace() method and printing the result

df['source_add'] = df['source_add'].replace('', pd.NA).fillna(df['Source_Address'])
##############################################################################

#Similar process has been applied to "Desitination_Address" column
#let us extract the column to list
drop_off_address_list=df['Destination_Address'].to_list()
drop_off_address_list[0:10]
#let us split the text using ","
sentence_stream2=[doc.split(",") for doc in drop_off_address_list]
#let us convert the text to bigram form
bigram2=Phrases(sentence_stream2,min_count=2)
bigram_phraser2=Phraser(bigram2)
#Let us store this text into token_list2
token_list2=[]

for sent in sentence_stream2:
    tokens=bigram_phraser[sent]
    token_list2.append(tokens)
print(token_list2[0:5])

#let us apply filter to get Andheri west Mumbai ,rest all text is ignored
for i in range(len(token_list2)):
          
       print(token_list2[i][-4:-3])

#["nan" if x == '' else x for x in a]

final_list2=['Navi Mumbai'if i=='Navi Mumbai'else token_list2[i][-4:-3] for i in range(len(token_list2))]    
#final_list2=['Navi Mumbai'if i=='Navi Mumbai'else token_list2[i][-4:-3] for i in range(len(token_list2))]
#sample['source_add']=final_list

desti_add=["".join(final_list2[i]) for i in range(len(final_list2))]

df["desti_add"]=desti_add

######################################################################################


#####################################################
# filtering the rows where job is Govt
for index, row in df.iterrows():
    if 'International' in row['Destination_Address']:
      df_new=df.Source_Address.replace('International','International Airport',regex=True)
df['Destination_Address']=pd.Series(df_new)
#################################################
# filtering the rows where job is Govt
for index, row in df.iterrows():
    if 'Domastic' in row['Destination_Address']:
      df_new=df.Source_Address.replace('Domastic','Domastic Airport',regex=True)
df['Destination_Address']=pd.Series(df_new)
###################################################

indexes1=[]
to_modify1=[]

for index, row in df.iterrows():
    if 'Navi' in row['Destination_Address']:
       print(index,row['Destination_Address'])
       indexes1.append(index)
       to_modify1.append(row['Destination_Address'])

#df['Source_Address']=pd.Series(df_new)

df_new2=df[df["Destination_Address"].str.contains("Navi")]
k2=df_new2['Destination_Address'].tolist ()
   

sentence_stream2=[doc.split(",") for doc in k2]
#There are few combinations which require two word combinations ,inorder to do that following code will process the text
from gensim.models import Phrases
from gensim.models.phrases import Phraser
bigram2=Phrases(sentence_stream2,min_count=2)
bigram_phraser2=Phraser(bigram2)
#in order to save two combination words let us declare the empty list
token_list2=[]

for sent in sentence_stream2:
    tokens2=bigram_phraser2[sent]

    token_list2.append(tokens2)
print(token_list2[0:5])

#We require only Andheri West Mumbai,Parle East mumbai so filter is applied to the list
for i in range(len(token_list2)):
    print(token_list2[i][-4:-2])

#final_list=['Navi Mumbai'if Navi Mumbai in token_list else token_list[i][-4:-3] for i in range(len(token_list))]
    
replacements1=[token_list2[i][-4:-2] for i in range(len(token_list2))]
#source_add=["".join(final_list1[i]) for i in range(len(final_list1))]

#Also when iterating through 2 lists in parallel I like to use the zip function (or izip if you're worried about memory consumption, but I'm not one of those iteration purists). So try this instead.

indexes1
replacements1
to_modify1

len2=len(indexes)
for i in range(len2):
    A1=indexes[i]
    B1=replacements[i]
    df.loc[A1,"desti_add"]=B1



df['desti_add'] = df['desti_add'].replace('', pd.NA).fillna(df['Destination_Address'])
df.to_excel('c:/Dead_km_final/processed_data/processed_file.xlsx', index=False)
#######################################################################################
#Let us find out total number drivers in data frame
split_values=df['driver_uuid'].unique()
print(split_values)
split_values.shape
""" for value in split_values:
    df1=df[df['Driver_UUID']==value]
    output_file_name="Driver_file_with_"+str(value) +".xlsx"
    df.to_excel(output_file_name,index=False) """
#let us apply group this driver_uuid 
grouped_df = df.groupby('driver_uuid')

#Let us now sort excel sheets as per drivers and it will store in working directory
for data in grouped_df.driver_uuid:
    grouped_df.get_group(data[0]).to_excel(data[0]+".xlsx",index=False)  
    