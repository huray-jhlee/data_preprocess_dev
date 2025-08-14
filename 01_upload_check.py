import os
import json
import pickle
import pandas as pd

from tqdm import tqdm
from glob import glob
from httplib2 import Http
from dotenv import load_dotenv
from collections import defaultdict, Counter
from datetime import datetime, timedelta

load_dotenv()

RAW_DATA_DIR = os.getenv("RAW_DATA_DIR")
CSV_PATH = "/home/ai04/workspace/ppg_process/user_device_table.csv"

WEBHOOK_KEY = os.getenv("WEBHOOK_KEY")
WEBHOOK_TOKEN = os.getenv("WEBHOOK_TOKEN")

def parse_user2device(csv_path, reverse=False):
    df = pd.read_csv(csv_path)
    df = df[df["Device_Id"] != "c5ad2c27_a90f2adb"]
    df = df[df["Note"] != "test기기"]
    # df = df[df["Note"] == "test기기"] # TODO: for debugging
    df = df.drop(columns=["Note"])
    
    if reverse:
        table = dict(zip(df['Device_Id'], df['User_Id']))
    else:
        table = dict(zip(df['User_Id'], df['Device_Id']))
        
    return table

def make_date_list(start_date: datetime=None, end_date: datetime=None, exclude: set[str]=None):
    
    if start_date is None:
        today = datetime.today()
        start_date = today - timedelta(days=today.weekday())
    
    today = datetime.today() if end_date is None else end_date
    
    current = today if today.hour >= 16 else today-timedelta(days=1)
    
    date_set = set()
    while current.date() >= start_date.date():
        date_set.add(current.strftime("%Y-%m-%d"))
        current -= timedelta(days=1)
    
    if exclude is not None:
        date_set -= exclude
    
    return set(sorted(date_set))

def send_to_chat(message):
    
    app_message = {"text": message}
    url = f"https://chat.googleapis.com/v1/spaces/AAQA9ue2i9I/messages?key={WEBHOOK_KEY}&token={WEBHOOK_TOKEN}"
    message_headers = {"Content-Type": "application/json; charset=UTF-8"}
    http_obj = Http()
    response = http_obj.request(
        uri=url,
        method="POST",
        headers=message_headers,
        body=json.dumps(app_message),
    )
    print(response[0].get("status"))

def catch_missing_data(target_device_ids):
    
    # today = datetime.now()
    # target_date = today if today.hour >= 16 else today-timedelta(days=1)
    
    progress = tqdm(target_device_ids)
    
    missing_date_dict = defaultdict(dict)
    date_set = make_date_list()
    
    date_obj_list = [datetime.strptime(ds, "%Y-%m-%d") for ds in date_set]
    week_start_date = min(date_obj_list)

    for target_device_id in progress:
        target_device_dir = os.path.join(RAW_DATA_DIR, target_device_id)
        
        for spec_dir in ["har_label", "sensor_data", "samsung_health"]:
            progress.set_description(f"Device-> {target_device_id}, spec_dir-> {spec_dir}")
            
            target_dir = os.path.join(target_device_dir, spec_dir)
            
            # 없으면 넘어가진 말고, 뒤에서 메세지 보낼때 처리
            if not os.path.exists(target_dir):
                os.makedirs(target_dir, exist_ok=True)
            
            filenames = os.listdir(target_dir)

            if spec_dir == "har_label":
                date_har_dict = {datetime.strptime(filename.split("_")[0],"%y%m%d"):filename for filename in filenames}
                latest_filename = date_har_dict[max(date_har_dict)]
                
                check_path = os.path.join(target_dir, latest_filename)
                with open(check_path, "r") as f:
                    datas = json.load(f)
                collected_har_label_date = {inner_dict["timeString"].split(" ")[0] for inner_dict in datas}
                
                missing_date_dict[target_device_id][f"{spec_dir}"] = sorted(date_set - collected_har_label_date)
                    
            elif spec_dir == "sensor_data":
                date_hour_dict = defaultdict(set)
                collected_sensor_data_date_list = []
                filenames = sorted(filenames)
                for filename in filenames[::-1]:
                    filename = filename.split(".")[0]
                    _, _, date, hour = filename.split("_")
                    if datetime.strptime(date, "%Y-%m-%d") < week_start_date :
                        break
                    collected_sensor_data_date_list.append(date)
                    date_hour_dict[date].add(int(hour))
                
                valid_collected_date = {date for date, count in dict(Counter(collected_sensor_data_date_list)).items() if count>=6}
                # valid_collected_date = set(collected_har_label_date)  # TODO: for debugging
                
                missing_date_dict[target_device_id][f"{spec_dir}"] = sorted(date_set - valid_collected_date)
                sorted_date_hour_dict = dict(sorted(date_hour_dict.items(), key=lambda x: datetime.strptime(x[0], "%Y-%m-%d")))
                missing_date_dict[target_device_id][f"collected-{spec_dir}-hour"] = sorted_date_hour_dict
                
            else: # samsung_health
                collected_samsung_health_date = set(filenames)
                missing_date_dict[target_device_id][f"{spec_dir}"] = sorted(set(date_set) - set(collected_samsung_health_date))
        
    return missing_date_dict
        
def main(save_pkl=False):
    
    user2device = parse_user2device(CSV_PATH)
    device2user = parse_user2device(CSV_PATH, reverse=True)
    today = datetime.today()
    target_date = today if today.hour >= 16 else today - timedelta(days=1)
    target_date_str = target_date.strftime("%Y-%m-%d")
    
    target_device_ids = list(user2device.values())
    target_device_ids = ["a31d491b_4a3ec8e8"]
    
    missing_date_dict = catch_missing_data(target_device_ids)
    message = f"Missing Data Report: {target_date_str}\n{'='*40}"
    exclude_key = ["samsung_health"]
    
    message_dict = defaultdict(list)
    
    invalid_ids = set()
    
    for device_id, device_dict in missing_date_dict.items():
        for inner_key, inner_date_list in device_dict.items():
            if inner_key in exclude_key:
                continue
            elif inner_key == "collected-sensor_data-hour":
                # TODO
                continue
            if inner_date_list:
                message_dict[inner_key].append(device_id)
                invalid_ids.add(device_id)
    
    missing_date_dict["valid_data"] = {
        "date": target_date_str,
        "device_ids": set(target_device_ids) - invalid_ids,
    }
    
    if save_pkl:
        with open("upload_check.pkl", "wb") as f:
            pickle.dump(missing_date_dict, f, pickle.HIGHEST_PROTOCOL)
            
    for data_type, device_id_list in message_dict.items():
        message += f"\n{data_type}\n\n{', '.join([f'{device2user[device_id]}({device_id})' for device_id in device_id_list])}\n{'-'*40}"
        
    print(message)
    send_to_chat(message)

if __name__ == "__main__":
    main(save_pkl=True)