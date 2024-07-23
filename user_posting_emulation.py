import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
import yaml


random.seed(100)


class AWSDBConnector:

    def __init__(self):

        self.HOST = self.get_db_credentials()['HOST']
        self.USER = self.get_db_credentials()['USER']
        self.PASSWORD = self.get_db_credentials()['PASSWORD']
        self.DATABASE = self.get_db_credentials()['DATABASE']
        self.PORT = self.get_db_credentials()['PORT']

    def get_db_credentials(self):
        
        with open('db_creds.yaml', "r") as creds:
            cred_dic = yaml.safe_load(creds)

        return cred_dic
    
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine

new_connector = AWSDBConnector()

def send_data_to_s3(data):
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}

    if "user" in data:
       payload = json.dumps({ "records" : [{
           "value" : {
                "ind": data["user"]["ind"],
                "first_name": data["user"]["first_name"],
                "last_name": data["user"]["last_name"],
                "age": data["user"]["age"],
                "date_joined": data["user"]["date_joined"].strftime("%Y-%m-%d %H:%M:%S") 
                }
                }]})
       response = requests.request("POST", f"{new_connector.get_db_credentials()["INVOKE_URL"]}/topics/0e03d1c30c91.user", headers=headers, data=payload)
       print(response.status_code)

    if "geo" in data:
       payload = json.dumps({"records": [{ 
           "value": { 
               "ind": data["geo"]["ind"],
               "timestamp": data["geo"]["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
               "latitude": data["geo"]["latitude"],
               "longitude": data["geo"]["longitude"],
               "country": data["geo"]["country"]
               }
               }]})
       response = requests.request("POST", f"{new_connector.get_db_credentials()["INVOKE_URL"]}/topics/0e03d1c30c91.geo", headers=headers, data=payload)
       print(response.status_code)

    if "pin" in data:
        payload = json.dumps({ "records": [{
            "value": {
                "index": data["pin"]["index"],
                "unique_id": data["pin"]["unique_id"],
                "title": data["pin"]["title"],
                "description": data["pin"]["description"],
                "poster_name": data["pin"]["poster_name"],
                "follower_count": data["pin"]["follower_count"],
                "tag_list": data["pin"]["tag_list"],
                "is_image_or_video": data["pin"]["is_image_or_video"],
                "image_src": data["pin"]["image_src"],
                "downloaded": data["pin"]["downloaded"],
                "save_location": data["pin"]["save_location"],
                "category": data["pin"]["category"]
                }
                }]})
        response = requests.request("POST", f"{new_connector.get_db_credentials()["INVOKE_URL"]}/topics/0e03d1c30c91.pin", headers=headers, data=payload)
        print(response.status_code)

def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
                send_data_to_s3({"pin": pin_result})

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                send_data_to_s3({'geo': geo_result})

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
                send_data_to_s3({'user': user_result})
            
            # print(pin_result)
            # print(geo_result)
            # print(user_result)

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


