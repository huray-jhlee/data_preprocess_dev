# Data Preprocessing for PPG sensor data, Samsung Health Data

## 01_upload_check.py
- 일단위 데이터 업로드 체크
- 체크하는 데이터 : `har_label`, `sensor_data` / `samsung_health`(Optional)
- 서버에 업로드한 `har_label`과 `sensor_data`를 체크하여 둘 다 제대로 업로드 되었을때, `valid_data`를 추려서 pkl로 저장
- Output : `upload_check.pkl`
    ```
    {
        "<device_id>": {
            "har_label": list,
            "sensor_data": list,
            "collected-sensor_data-hour": {
                "<YYYY-MM-DD>": {hour, hour, ...}
            },
            "samsung_health": list
        },
        "valid_data": {
            "date": "<YYYY-MM-DD>",
            "device_ids": { "<device_id>", ... }
        }
    }
    ```
### TODO
- collected-sensor_data-hour : 중간에 빠진 데이터에 대해서 체크하는 부분 추가?


## 02_process_data.py
- `upload_check.pkl`을 기반으로 무결한 device_id에 대해서 raw 데이터를 전처리
- 데이터는 일 단위의 `parquet`형태로 저장
- 처리 과정
    1. 워치에서 수집된 binary 형태의 센서데이터 파싱
    2. 폰에서 업로드한 14개의 Samsung Health 데이터 파싱 및 하나의 `parquet`으로 병합
