import os
import json
import pickle
import pandas as pd
from glob import glob
from tqdm import tqdm
from typing import List
from dotenv import load_dotenv

from utils import process_binary, utc2kst, parse_iso_duration
from config import DATA_UNITS, VALUE_KEY, VALUE_STR_KEY, FUNCTION_MAP
    
"""
처리할 데이터
1. sensor_data
2. samsung_health_data
"""
load_dotenv()

RAW_DATA_DIR = os.getenv("RAW_DATA_DIR")
PROCESSED_DATA_DIR = os.getenv("PROCESSED_DATA_DIR")

# 삼성헬스 어플리케이션에서 제공하는 사전 운동 타입
# https://developer.samsung.com/health/android/data/api-reference/EXERCISE_TYPE.html
with open("exercise_type.json", "r") as f:
    EXERCISE_MAP = json.load(f)
    
REMOVE_KEYS = ["uid", "appId", "deviceId", "startTime", "endTime"]


def process_sensor_data(device_id, target_date):
    
    target_device_dir = os.path.join(RAW_DATA_DIR, device_id)

    sensor_data_dir = os.path.join(target_device_dir, "sensor_data")
    target_sensor_data_paths = glob(os.path.join(sensor_data_dir, f"*{target_date}*"))
    target_sensor_data_paths = sorted(target_sensor_data_paths)
    
    collected_df = None
    
    progress = tqdm(target_sensor_data_paths)
    
    for sensor_data_path in progress:
        
        progress.set_description(f" processing-> {os.path.basename(sensor_data_path)}")
        
        sensor_record_list = process_binary(sensor_data_path)
        inner_df = pd.DataFrame(sensor_record_list)

        if collected_df is None:
            collected_df = inner_df
        else :
            collected_df = pd.concat([collected_df, inner_df], ignore_index=True)
    
    return collected_df

def process_samsung_health(datas: List, data_type: str):
    
    processed_list = []
    for data in datas:
        
        
        start_time = utc2kst(data["startTime"])
        end_time = utc2kst(data["endTime"])
        value = data[VALUE_KEY[data_type]] if data_type in VALUE_KEY else None
        processed_value = parse_iso_duration(value)
        
        value_dict = {}
        if data_type in VALUE_STR_KEY:
            for inner_value_str_key in VALUE_STR_KEY[data_type]:
                value_dict[inner_value_str_key] = data[inner_value_str_key]

        if data_type == "Exercise":
            processed_value = EXERCISE_MAP[processed_value]
            value_dict["exercise_str"] = value
        
        inner_dict = {
            "category": data_type,
            "start_time": start_time,
            "end_time": end_time,
        }
        
        if data_type in DATA_UNITS:
            inner_dict["unit"] = DATA_UNITS[data_type]
        
        if data_type in FUNCTION_MAP["activity_summary"]:
            inner_dict["value"] = processed_value
        elif data_type in FUNCTION_MAP["sequential_data"]:
            inner_dict["value"] = processed_value
            inner_dict["value_str"] = json.dumps(value_dict, ensure_ascii=False)
        elif data_type in FUNCTION_MAP["etc"]:
            value_dict = data.copy()
            for rm_key in REMOVE_KEYS:
                value_dict.pop(rm_key)
            
            inner_dict["value_str"] = json.dumps(value_dict, ensure_ascii=False)
        else :
            raise ValueError("Invalid data_type")

        processed_list.append(inner_dict)
        
    return processed_list

def main():
    
    with open("upload_check.pkl", "rb") as f:
        data_dict = pickle.load(f)
    
    target_date = data_dict["valid_data"]["date"]
    valid_device_ids = data_dict["valid_data"]["device_ids"]
    
    for valid_device_id in valid_device_ids:
        
        collected_df = process_sensor_data(valid_device_id, target_date)
        save_path = os.path.join(PROCESSED_DATA_DIR, valid_device_id, "sensor_data", f"{target_date}.parquet")
        save_dir = os.path.dirname(save_path)
        os.makedirs(save_dir, exist_ok=True)
        
        collected_df.to_parquet(save_path, index=False, engine="pyarrow")
        
        ####
        
        # 2. samsung_health
        target_data_dir = os.path.join(RAW_DATA_DIR, valid_device_id, "samsung_health", target_date)
        # target_data_dir = "samsung_health/2025-08-07"  # TODO: for debug
        target_paths = glob(os.path.join(target_data_dir, "*.json"))
        target_paths = sorted(target_paths)
        
        results = []
        
        processing_samsung_health = tqdm(target_paths)
        
        for target_path in processing_samsung_health:
            
            processing_samsung_health.set_description(f" processing-> {os.path.basename(target_path)}")
            
            data_type = os.path.basename(target_path).split("_")[0]
            with open(target_path, "r") as f:
                datas = json.load(f)
            print(1)
            processed_health_list = process_samsung_health(datas, data_type)
            results += processed_health_list
        
        samsung_health_df = pd.DataFrame(results)
        
        save_path = os.path.join(PROCESSED_DATA_DIR, valid_device_id, "samsung_health", f"{target_date}.parquet")
        save_dir = os.path.dirname(save_path)
        os.makedirs(save_dir, exist_ok=True)
        
        samsung_health_df.to_parquet(save_path, index=False, engine="pyarrow")
    
    print("Done")

if __name__ == "__main__":
    main()