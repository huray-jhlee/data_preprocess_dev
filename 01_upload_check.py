import os
import json
import pickle
import pandas as pd

from tqdm import tqdm
from glob import glob
from httplib2 import Http
from dotenv import load_dotenv
from collections import defaultdict
from datetime import datetime, timedelta

load_dotenv()

START_DATE_AI = datetime(2025, 7, 1)
START_DATE = datetime(2025, 7, 7)

RAW_DATA_DIR = "/data3/ppg_data/raw"
CSV_PATH = "/home/ai04/workspace/ppg_process/user_device_table.csv"

AI_DIVISION = ["4c37111_f66d2e64", "cf782c01_10c971c2", "10639090_4212f054", "a31d491b_4a3ec8e8", "5c5cbde6_992e5ecc", "414a7e87_1887ce0f"]

WEBHOOK_KEY = os.getenv("WEBHOOK_KEY")
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN")

def parse_user2device(csv_path):
    df = pd.read_csv(csv_path)
    df = df[df["Device_Id"] != "c5ad2c27_a90f2adb"]
    df = df[df["Note"] != "test기기"]
    # df = df[df["Note"] == "test기기"]
    df = df.drop(columns=["Note"])
    table = dict(zip(df['User_Id'], df['Device_Id']))
    return table

def make_date_list(start_date: datetime, end_date: datetime=None, exclude: set[str]=None):
    
    date_set = set()
    today = datetime.today() if end_date is None else end_date
    current = today
    while current.date() >= start_date.date():
        if current.weekday() < 5:
            date_set.add(current.strftime("%Y-%m-%d"))
        current -= timedelta(days=1)
    
    if exclude is not None:
        date_set -= exclude
    
    return set(sorted(date_set))


def send_to_chat(message):
    
    app_message = {"text": message}
    
    url = f"https://chat.googleapis.com/v1/spaces/AAAAw1daNpw/messages?key={WEBHOOK_KEY}&token={WEBHOOK_TOKEN}"
    message_headers = {"Content-Type": "application/json; charset=UTF-8"}
    http_obj = Http()
    response = http_obj.request(
        uri=url,
        method="POST",
        headers=message_headers,
        body=json.dumps(app_message),
    )
    print(response[0].get("status"))

def catch_missing_data(target_device_ids, exclude_date_set, save=True):
    
    progress = tqdm(target_device_ids)
    
    missing_date_dict = defaultdict(dict)
    
    for target_device_id in progress:
        date_set = make_date_list(
            start_date=START_DATE if target_device_id not in AI_DIVISION else START_DATE_AI,
            exclude=exclude_date_set
        )
        progress.set_description(f"Device-> {target_device_id}")
        target_device_dir = os.path.join(RAW_DATA_DIR, target_device_id)
        
        for spec_dir in ["har_label", "sensor_data", "samsung_health"]:
            print(f"\nspec dir: {spec_dir}")
            target_dir = os.path.join(target_device_dir, spec_dir)
            
            if not os.path.exists(target_dir):
                print(f"{target_dir} is not exists")
                continue
            
            filenames = os.listdir(target_dir)

            if spec_dir == "har_label":
                for filename in filenames:
                    check_path = os.path.join(target_dir, filename)
                    with open(check_path, "r") as f:
                        datas = json.load(f)
                    collected_har_label_date = {inner_dict["timeString"].split(" ")[0] for inner_dict in datas}
                    
                    missing_date_dict[target_device_id][spec_dir] = sorted(date_set - collected_har_label_date)
                    
            elif spec_dir == "sensor_data":
                
                date_hour_dict = defaultdict(list)
                collected_sensor_data_date = set()
                for filename in filenames:
                    filename = filename.split(".")[0]
                    _, _, date, hour = filename.split("_")
                    collected_sensor_data_date.add(date)
                    date_hour_dict[date].append(hour)
                
                missing_date_dict[target_device_id][spec_dir] = sorted(date_set - collected_sensor_data_date)
                missing_date_dict[target_device_id][f"{spec_dir}-hour"] = sorted(date_hour_dict)
                
            else: # samsung_health
                
                collected_samsung_health_date = set(filenames)
                missing_date_dict[target_device_id][f"collected_{spec_dir}_date"] = sorted(collected_samsung_health_date)
    if save:
        with open("upload_check.pkl", "wb") as f:
            pickle.dump(missing_date_dict, f, pickle.HIGHEST_PROTOCOL)
        
    return missing_date_dict
        
def main():
    
    user2device = parse_user2device(CSV_PATH)
    
    exclude_date_set = make_date_list(
        start_date=datetime(2025, 7, 21),
    )
    
    target_device_ids = list(user2device.values())
    
    missing_date_dict = catch_missing_data(target_device_ids, exclude_date_set, save=False)
    
    message = ""
    for device_id, device_dict in missing_date_dict.items():
        message += f"\n{device_id}\n{'-'*40}\n"
        for inner_key, inner_date_list in device_dict.items():
            if inner_date_list:
                message += f"{inner_key}\n{', '.join(inner_date_list)}\n\n"
        message += f"\n{'='*40}\n"
    
    print(message)
    
    send_to_chat(message)
    
    pass


if __name__ == "__main__":
    main()
    # send_to_chat()