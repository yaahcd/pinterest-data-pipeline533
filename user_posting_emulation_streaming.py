import requests
import json

def send_data_to_stream(data, url):
    headers = {'Content-Type': 'application/json'}

    if "user" in data:
       payload = json.dumps({
          "StreamName": "streaming-0e03d1c30c91-user",
           "Data": {
                "ind": data["user"]["ind"],
                "first_name": data["user"]["first_name"],
                "last_name": data["user"]["last_name"],
                "age": data["user"]["age"],
                "date_joined": data["user"]["date_joined"].strftime("%Y-%m-%d %H:%M:%S") 
                },
            "PartitionKey": "user-partition"
            })
       response = requests.request("PUT", f"{url}/streams/streaming-0e03d1c30c91-user/record", headers=headers, data=payload)
       print(f"user: {response.status_code}")

    if "geo" in data:
       payload = json.dumps({
          "StreamName": "streaming-0e03d1c30c91-geo",
           "Data": { 
               "ind": data["geo"]["ind"],
               "timestamp": data["geo"]["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
               "latitude": data["geo"]["latitude"],
               "longitude": data["geo"]["longitude"],
               "country": data["geo"]["country"]
               },
            "PartitionKey": "geo-partition"
            })
       response = requests.request("PUT", f"{url}/streams/streaming-0e03d1c30c91-geo/record", headers=headers, data=payload)
       print(f"geo: {response.status_code}")

    if "pin" in data:
        payload = json.dumps({ 
            "StreamName": "streaming-0e03d1c30c91-pin",
            "Data": {
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
                },
            "PartitionKey": "pin-partition"
            })
        response = requests.request("PUT", f"{url}/streams/streaming-0e03d1c30c91-pin/record", headers=headers, data=payload)
        print(f"pin: {response.status_code}")
  
