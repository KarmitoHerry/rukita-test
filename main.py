import os
import sys
import pandas as pd
import numpy as np
import logging
from flask import jsonify, make_response
from dotenv import load_dotenv
from datetime import date
from google.oauth2 import service_account


load_dotenv()

logger = logging.getLogger(__name__)

today = date.today()

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

GBQ_PROJECT = os.getenv('GBQ_PROJECT')
GBQ_DATASET = os.getenv('GBQ_DATASET')
GBQ_LOCATION = os.getenv('GBQ_LOCATION')


def insertToGbq(df:pd.DataFrame, project, dataset, table, schema, behaviour, location):
    credentials = service_account.Credentials.from_service_account_file('key.json',)
    destination = f"{dataset}.{table}"
    return df.to_gbq(project_id = project, credentials= credentials,destination_table = destination, table_schema = schema, if_exists = behaviour, location = location)

def rename_columns(df):
    columns = {} 
    for col in df.columns:
        columns[col] = col.lower().replace(' ', '_')
    df.rename(columns=columns, inplace=True)
    return df

def case_1():

    df_agreement = pd.read_csv('case_1/dataset/agreement.csv')
    df_buildings = pd.read_csv('case_1/dataset/buildings.csv')
    df_rooms = pd.read_csv('case_1/dataset/rooms.csv')

    df = pd.merge(df_buildings, df_rooms, how = 'left', left_on = 'id', right_on = 'building_id')
    df = pd.merge(df, df_agreement, how = 'left', on = 'building_id')

    df['building_live_date'] = pd.to_datetime(df['building_live_date'])
    df['soft_live_date'] = pd.to_datetime(df['soft_live_date'])

    # Filtering data by building_live_date <= today
    df = df[df['building_live_date'] <= str(today)]

    # Adding new columns for filtering data base on requirements
    occupied_conditions = [
        (df['room_status'] != 10) & (df['live_date_confirm'].dtype == bool)
    ]
    occupied_values = [1]
    df["occupied"] = np.select(occupied_conditions, occupied_values, default= 0)
    
    # calculate occupied rooms
    df_get_occupied_rooms = df.groupby(["building_id","occupied"], as_index=False)["room_id"].count()
    df_get_occupied_rooms = df_get_occupied_rooms.rename(columns = {"room_id" : "occupied_rooms"})
    df = pd.merge(df, df_get_occupied_rooms, how = 'inner', on = ['building_id','occupied'])
    
    # calculate total rooms
    df_get_total_rooms = df.groupby(["building_id"], as_index=False)["room_id"].count()
    df_get_total_rooms = df_get_total_rooms.rename(columns = {"room_id" : "total_rooms_by_building"})
    df = pd.merge(df, df_get_total_rooms, how = 'inner', on = ['building_id'])
    
    
    df = df[df["occupied"] == 1]
    # calculate occupancy
    df["occupancy"] = df["occupied_rooms"] / df["total_rooms_by_building"]
    
    df = df[["property_code","rukita_option","occupancy","building_live_date"]]
    df = df.rename(columns = {'building_live_date' : 'date'})
    df = df.drop_duplicates()
    df = df.astype({"property_code" : "string", "rukita_option" : "boolean", "occupancy" : "float", "date" : "datetime64[ns]"})
    df = df.sort_values(by=["date", "property_code"])
    print(df.info(verbose=True))
    
    # insert to datawarehouse
    schema = [
        {"name" : "property_code", "type": "STRING"},
        {"name" : "rukita_option", "type": "BOOLEAN"},
        {"name" : "occupancy", "type": "FLOAT"},
        {"name" : "date", "type": "DATETIME"}
    ]
    insertToGbq(df, GBQ_PROJECT, GBQ_DATASET, "daily_occupancy", schema, "replace", GBQ_LOCATION)

    return True

def case_2():

    df_leads = pd.read_csv('case_2/dataset/leads_data.csv')
    df_signing_data = pd.read_csv('case_2/dataset/signing_data.csv')
    df_traffic = pd.read_csv('case_2/dataset/traffic.csv')

    # rename columns
    df_leads = rename_columns(df_leads)
    df_signing_data = rename_columns(df_signing_data)
    df_traffic = rename_columns(df_traffic)

    # print(df_traffic['date'].value_counts())
    
    # filtering "Only order with “Full Payment” and “Only Deposit” order status that will be counted as signing"
    df_signing_data["signing"] = np.where((df_signing_data["order_status"] == "Full Payment") | (df_signing_data["order_status"] == "Only Deposit"), 1, 0)
    # filtering "Signed Date shouldn’t be after Check In Date, if this happen Signed Date will use Check In Date"
    df_signing_data["final_date"] = np.where(df_signing_data["signed_date"] >=  df_signing_data["check_in_date"], df_signing_data["check_in_date"], df_signing_data["signed_date"])
    # join-date leads and signing_data
    df = pd.merge(df_leads, df_signing_data, how = "left", left_on = ['first_contact','email','phone'], right_on = ['final_date','email','phone'])
    # convert date traffic
    df_traffic['date'] = pd.to_datetime(df_traffic['date'])
    # generate final data
    f_dataframe = pd.DataFrame()
    total_signing = df.groupby('final_date', as_index=False)["signing"].count()
    total_signing = total_signing.rename(columns = {"signing" : "number_of_singings"})

    total_leads = df.groupby('first_contact', as_index=False)["email"].count()
    total_leads = total_leads.rename(columns = {"email" : "number_of_leads"}) 

    f_dataframe = pd.merge(total_signing, total_leads, how="right", left_on= "final_date", right_on="first_contact")
    # pre-processing
    f_dataframe['first_contact'] = pd.to_datetime(f_dataframe['first_contact'])
    f_dataframe["number_of_singings"] = f_dataframe["number_of_singings"].fillna(0)
    f_dataframe = f_dataframe.rename(columns = {"first_contact" : "date"}) 
    f_dataframe = f_dataframe[['date','number_of_leads','number_of_singings']]
        
    f_dataframe = pd.merge(f_dataframe, df_traffic, how = "left", left_on= "date", right_on="date")
    f_dataframe = f_dataframe.rename(columns= {"views" : "number_of_traffic"})
    f_dataframe = f_dataframe[["number_of_traffic", "number_of_leads", "number_of_singings", "date"]]
    f_dataframe = f_dataframe[f_dataframe['number_of_traffic'].notnull()]
    f_dataframe = f_dataframe.sort_index()
    
    schema = [
        {"name" : "number_of_traffic", "type": "INTEGER"},
        {"name" : "number_of_leafs", "type": "INTEGER"},
        {"name" : "number_", "type": "INTEGER"},
        {"name" : "date", "type": "DATE"}
    ]
    
    insertToGbq(f_dataframe, GBQ_PROJECT, GBQ_DATASET, "conversion_leads", schema, "replace", GBQ_LOCATION)
    
    return True

# case_1()
# case_2()

