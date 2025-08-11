import os
import json
import pickle
import pandas as pd

from glob import glob
from tqdm import tqdm

from utils import process_binary

"""
처리할 데이터
1. sensor_data
2. samsung_health_data
"""

RAW_DATA_DIR = "/data3/ppg_data/raw"
PROCESSED_DATA_DIR = "/data3/ppg_data/processed"

###

SAMSUNG_HEALTH_PROCESS_DICT = {
    "series_data": ["BloodGlucose", "BloodOxygen", "HeartRate", "skintemper"],  # 4
    "activity_summary": ["TotalActive", "calburn", "TotalCaloriesBurned", "TotalDistance", "step", "WaterIntake"],  # 6
    "sessions": ["sleep", "Exercise"],  # 2
    "body_composition": ["BodyComposition"],  # 1
    "nutrition": ["Nutrition"]  # 1
}

REVERSED_SAMSUNG_HEALTH_PROCESS_DICT = {
    value: key
    for key, values in SAMSUNG_HEALTH_PROCESS_DICT.items()
    for value in values
}

DATA_UNITS = {
    "BloodGlucose": "mmol/l",
    "BloodOxygen": "percent",
    "calburn": "kcal",
    "Exercise": "exercise_code",
    "HeartRate": "bpm",
    "skintemper": "celcious",
    "sleep": "seconds",
    "step": "steps",
    "TotalActive": "seconds",
    "TotalCaloriesBurned": "kcal",
    "TotalDistance": "m",
    "WaterIntake": "ml",
}

VALUE_KEY = {
    "BloodGlucose": "glucoseLevel",
    "BloodOxygen": "oxygenSaturation",
    "calburn": "value",
    "Exercise": "exerciseType",
    "HeartRate": "heartRate",
    "skintemper": "skinTemperature",
    "sleep": "duration",
    "step": "value",
    "TotalActive": "value",
    "TotalCaloriesBurned": "value",
    "TotalDistance": "value",
    "WaterIntake": "amount",
}

VALUE_STR_KEY = {
    "BloodGlucose": ["insulinInjected", "mealStatus", "mealTime", "seriesData"],
    "BloodOxygen": ["min", "max", "seriesData"],
    "HeartRate": ["min", "max", "seriesData"],
    "skintemper": ["min", "max", "seriesData"],
    "Exercise": ["sessions"],
    "sleep": ["sessions"],
}

"""
삼성헬스 어플리케이션에서 제공하는 사전 운동 타입
https://developer.samsung.com/health/android/data/api-reference/EXERCISE_TYPE.html
"""
with open("exercise_type.json", "r") as f:
    EXERCISE_MAP = json.load(f)
    

import isodate
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

def utc2kst(time):
    
    if isinstance(time, str):
        time = datetime.fromisoformat(time.replace("Z", "+00:00"))

    kst_dt = time.astimezone(ZoneInfo("Asia/Seoul"))
    kst_str = kst_dt.isoformat()
    
    return kst_str

def parse_iso_duration(input_duration):
    try:
        duration = isodate.parse_duration(input_duration)
        if isinstance(duration, timedelta):
            # print("iso duration format")
            return duration.total_seconds()
        else :
            # print("iso duration format (date-based)")
            return duration
    except :
        # print("not iso duration format")
        return input_duration

###

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

def main():
    
    with open("upload_check.pkl", "rb") as f:
        data_dict = pickle.load(f)
    
    target_date = data_dict["valid_data"]["date"]
    valid_device_ids = data_dict["valid_data"]["device_ids"]
    remove_keys = ["uid", "appId", "deviceId", "startTime", "endTime"]
    
    
    for valid_device_id in valid_device_ids:
        
        collected_df = process_sensor_data(valid_device_id, target_date)
        save_path = os.path.join(PROCESSED_DATA_DIR, valid_device_id, "sensor_data", f"{target_date}.parquet")
        save_dir = os.path.dirname(save_path)
        os.makedirs(save_dir, exist_ok=True)
        
        collected_df.to_parquet(save_path, index=False, engine="pyarrow")
        
        ####
        
        # 2. samsung_health
        # target_data_dir = os.path.join(RAW_DATA_DIR, valid_device_id, "samsung_health", target_date)
        target_data_dir = "samsung_health/2025-08-07"  # TODO: for debug
        target_paths = glob(os.path.join(target_data_dir, "*.json"))
        target_paths = sorted(target_paths)
        
        results = []
        
        for target_path in target_paths:
            data_type = os.path.basename(target_path).split("_")[0]
            print(target_path)
            with open(target_path, "r") as f:
                datas = json.load(f)
            
            target_func = REVERSED_SAMSUNG_HEALTH_PROCESS_DICT[data_type]
            
            #############
            if target_func == "series_data":
                # 구조 내부에 seriesData 형태로 sequential 한 데이터 들어옴
                # seriesData안에는 value, min, max, startTime, endTime 이렇게 어느정도 고정된 데이터로 예상
                # 취급데이터
                # -> BloodGlucose, BloodOxygen, HeartRate, SkipTemp
                for data in datas:
                    start_time = utc2kst(data["startTime"])
                    end_time = utc2kst(data["endTime"])
                    value = data[VALUE_KEY[data_type]]
                    
                    value_dict = {}
                    for inner_value_str_key in VALUE_STR_KEY[data_type]:
                        value_dict[inner_value_str_key] = data[inner_value_str_key]
                    
                    results.append({
                        "category": data_type,
                        "start_time": start_time,
                        "end_time": end_time,
                        "value": value,
                        "unit": DATA_UNITS[data_type],
                        "value_str": json.dumps(value_dict, ensure_ascii=False)  # 데이터 추리기
                    })
                    
            elif target_func == "activity_summary":
                # value, waterintake -> amount
                # 취급데이터
                # -> TotalActivie, calburn, TotalCaloriesBurned, TotalDistance, step, WaterIntake
                for data in datas:
                    
                    start_time = utc2kst(data["startTime"])
                    end_time = utc2kst(data["endTime"])
                    value = data[VALUE_KEY[data_type]]
                    processed_value = parse_iso_duration(value)
                    
                    results.append({
                        "category": data_type,
                        "start_time": start_time,
                        "end_time": end_time,
                        "value": processed_value,
                        "unit": DATA_UNITS[data_type],
                    })
                    
            elif target_func == "sessions":
                # 구조 내부에 sessions로 리스트에 데이터가 담겨서 전달
                # sessions아래에는 데이터별로 key값들이 달라서 따로 수집 분기를 나눠야함
                # 취급데이터
                # -> sleep, Exercise
                
                # sleep -> sessions -> stage
                # Exercise -> sessions -> 수많은 키들 존재 + log 리스트
                for data in datas:
                    start_time = utc2kst(data["startTime"])
                    end_time = utc2kst(data["endTime"])
                    value = data[VALUE_KEY[data_type]]
                    
                    
                    value_dict = {}
                    for inner_value_str_key in VALUE_STR_KEY[data_type]:
                        value_dict[inner_value_str_key] = data[inner_value_str_key]
                    
                    if data_type == "sleep":
                        processed_value = parse_iso_duration(value)
                    else :
                        processed_value = EXERCISE_MAP[value]
                        value_dict["exercise_str"] = value
                    
                    results.append({
                        "category": data_type,
                        "start_time": start_time,
                        "end_time": end_time,
                        "value": processed_value,
                        "unit": DATA_UNITS[data_type],
                        "value_str": json.dumps(value_dict, ensure_ascii=False)  # 데이터 추리기
                    })
                
            elif target_func == "body_composition":
                # 구조가 유일, 따로 처리
                # 취급데이터: BodyComposition
                for data in datas:
                    start_time = utc2kst(data["startTime"])
                    end_time = utc2kst(data["endTime"])
                    
                    value_dict = data.copy()
                    for rm_key in remove_keys:
                        value_dict.pop(rm_key)
                    
                    value_str = json.dumps(value_dict)
                    
                    results.append({
                        "category": data_type,
                        "start_time": start_time,
                        "end_time": end_time,
                        "value_str": value_str,
                    })
                
            elif target_func == "nutrition":
                # 구조가 유일, 따로 처리
                # 취급데이터: Nutrition
                for data in datas:
                    start_time = utc2kst(data["startTime"])
                    end_time = utc2kst(data["endTime"])
                    
                    value_dict = data.copy()
                    for rm_key in remove_keys:
                        value_dict.pop(rm_key)
                        
                    results.append({
                        "category": data_type,
                        "start_time": start_time,
                        "end_time": end_time,
                        "value_str": json.dumps(data),
                    })
        
        ####
        
        samsung_health_df = pd.DataFrame(results)
        
        save_path = os.path.join(PROCESSED_DATA_DIR, valid_device_id, "samsung_health", f"{target_date}.parquet")
        save_dir = os.path.dirname(save_path)
        os.makedirs(save_dir, exist_ok=True)
        
        samsung_health_df.to_parquet(save_path, index=False, engine="pyarrow")
    
    print("Done")
    


if __name__ == "__main__":
    main()