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
    fixed_part = file.read(20)
    if len(fixed_part) < 20:
        raise EOFError("fixed part error")
    
    sensor_type, collected_ts, accuracy, data_size = struct.unpack(">I Q I I", fixed_part)
    
    value_bytes = file.read(data_size * 4)
    if len(value_bytes) < data_size * 4:
        raise EOFError("값 부족")
    
    values = struct.unpack(f">{data_size}f", value_bytes)
    
    return {
        "sensor_type": sensor_type,
        "collected_ts": collected_ts,
        "values": values
    }
    
def process_binary(file_path):
    
    with open(file_path, "rb") as file:
        
        data_sequence = 0
        
        file_header = file.read(16)
        format_version, creation_time = parse_file_header(file_header)
        
        sensor_record_list = []
        
        while True:
            batch_header = file.read(12)
            if len(batch_header) < 12:
                break
            
            batch_size, batch_timestamp = struct.unpack(">I Q", batch_header)
            
            if batch_size <= 0 or batch_size > 10000:
                raise ValueError(f"Invalid batch_size: {batch_size}")

            for _ in range(batch_size):
                sensor_data = parse_batch(file)
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