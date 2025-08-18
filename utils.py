import os
import json
import struct
import isodate
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta, timezone

#### For decoding binary files

MAGIC_HEADER = 0x53454E53
REVERSE_SENSOR_TYPE_MAP = {
    1001: "SAMSUNG_PPG",
    1002: "ACCELEROMETER", 
    1003: "GYROSCOPE",
    1004: "SAMSUNG_HEART_RATE",
    1005: "SAMSUNG_TEMP",
    1006: "SAMSUNG_ACCE",
    1007: "SAMSUNG_ECG",
    1008: "LIGHT",
    1009: "TYPE_STEP_COUNTER",
}

#### For decoding binary files

def format_timestamp(timestamp: int) -> str:
    try:
        dt = datetime.fromtimestamp(timestamp / 1000)
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    except (ValueError, OSError):
        return f"Invalid timestamp: {timestamp}"

def parse_file_header(file_header):
    
    if len(file_header) != 16:
        raise ValueError(f"Invalid header size: {len(file_header)}") 
    
    magic_header, format_version, creation_time = struct.unpack("> I I Q", file_header)
    
    if magic_header != MAGIC_HEADER:
        raise ValueError(f"Invalid magic header: {hex(magic_header)}")
    
    return format_version, creation_time

def parse_batch(file):
    
    data_size_check_dict = {
        1001: 3,
        1003: 3,
        1004: 2,
        1005: 2,
        1006: 3,
        # 1002, 1007, 1008, 1009 Sensors are not collected
    }
    
    def _check_valid(sensor_type, collected_ts, acc, data_size):
        if sensor_type not in REVERSE_SENSOR_TYPE_MAP:
            return False, {
                "at": "sensor_type",
                "got": sensor_type,
                "expected": f"{REVERSE_SENSOR_TYPE_MAP.keys()}"
            }
        
        if not(len(str(collected_ts)) == 13 and 1e12 <= collected_ts <= 2e12):
            return False, {
                "at": "collected_ts",
                "got": collected_ts,
                "len": len(str(collected_ts)),
                "expected": "13-digits epoch millis in [1e12, 2e12]"
            }
        
        if acc != 0:
            return False, {
                "at": "accuracy",
                "got": acc,
                "expected": 0
            }

        check_size = data_size_check_dict.get(sensor_type)
        if check_size is None:
            return False, {
                "at": "data_size check",
                "got": sensor_type,
                "expected": f"{data_size_check_dict.keys()}"
            }
        
        else:
            if sensor_type == 1004:
                if data_size < check_size:
                    return False, {
                        "at": "data_size_min",
                        "got": data_size,
                        "expected": "length of HeartRate needs at least 2"
                    }
            elif data_size != check_size:
                return False, {
                    "at": "data_size",
                    "got": data_size,
                    "expected": f"check_size"
                }
        
        return True, None
    
    start_pos = file.tell()
    fixed_part = file.read(20)
    if len(fixed_part) < 20:
        return None, {
            "at": "fixed_part",
            "pos": start_pos,
            "got": len(fixed_part),
            "expected": "length of fixed_part needs at least 20"
        }
    
    sensor_type, collected_ts, accuracy, data_size = struct.unpack(">I Q I I", fixed_part)
    
    valid, fail_log = _check_valid(sensor_type, collected_ts, accuracy, data_size)
    if not valid:
        return False, fail_log
    
    value_bytes = file.read(data_size * 4)
    if len(value_bytes) < data_size * 4:
        # raise EOFError("값 부족")
        return False, {
            "at": "value_bytes",
            "pos": start_pos,
            "got": f"{len(value_bytes)}",
            "expected": f"{data_size * 4}"
        }
    
    values = struct.unpack(f">{data_size}f", value_bytes)
    
    return {
        "sensor_type": sensor_type,
        "collected_ts": collected_ts,
        "values": values
    }, None
    
def process_binary(file_path):
    
    invalid_file_flag = False
    error_info = None
    
    with open(file_path, "rb") as file:
        
        data_sequence = 0
        file_header = file.read(16)
        format_version, creation_time = parse_file_header(file_header)
        
        sensor_record_list = []
        while True:
            if invalid_file_flag:
                break
            # before_tmp_sensor = {}        # TODO: for debugging
            
            batch_header = file.read(12)
            if len(batch_header) < 12:
                break
            
            batch_size, batch_timestamp = struct.unpack(">I Q", batch_header)
            
            if batch_size <= 0 or batch_size > 10000:
                raise ValueError(f"Invalid batch_size: {batch_size}")
            
            for i in range(batch_size):
                rec_pos = file.tell()
                if rec_pos == 7333536: 
                    print(1)
                sensor_data, err = parse_batch(file)
                if not sensor_data:
                    error_info = {
                        "file_path": file_path,
                        "batch_timestamp": batch_timestamp,
                        "record_index": i,
                        "pos": rec_pos,
                        "timestamp": datetime.now(ZoneInfo("Asia/Seoul")).isoformat(timespec="milliseconds")
                    }
                    if err:
                        error_info["details"] = err
                    print("Error detected:", error_info)
                    invalid_file_flag = True
                    break
                
                # before_tmp_sensor[i] = sensor_data   # TODO: for debugging
                sensor_type_str = REVERSE_SENSOR_TYPE_MAP[sensor_data.get("sensor_type")]
                collected_time = format_timestamp(sensor_data.get("collected_ts"))
                record = {
                    "file_path": file_path,
                    "sequence": data_sequence,
                    "sensor_type": sensor_type_str,
                    "data": sensor_data.get("values"),
                    "collected_time": collected_time,
                    "timestamp": sensor_data.get("collected_ts")
                }
                sensor_record_list.append(record)
                
                data_sequence += 1
                
    if error_info is not None:
        today = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%y%m%d")
        error_save_path = os.path.basename(file_path) + ".errors.jsonl"
        error_save_path = os.path.join("test", today, error_save_path)
        os.makedirs(os.path.dirname(error_save_path), exist_ok=True)
        with open(error_save_path, "w", encoding="utf-8") as f:
            f.write(json.dumps(error_info, ensure_ascii=False) + "\n")
                
    return sensor_record_list

#### For processing samsung health

def utc2kst(time):
    
    if isinstance(time, str):
        time = datetime.fromisoformat(time.replace("Z", "+00:00"))
    elif isinstance(time, datetime) and time.tzinfo is None:
        time = time.replace(tzinfo=timezone.utc)

    kst_dt = time.astimezone(ZoneInfo("Asia/Seoul"))
    kst_str = kst_dt.isoformat(timespec="milliseconds")
    
    return kst_str

def parse_iso_duration(input_data):
    try:
        duration = isodate.parse_duration(input_data)
        if isinstance(duration, timedelta):
            return duration.total_seconds()
        else :
            return duration
    except :
        return input_data