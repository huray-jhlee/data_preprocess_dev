FUNCTION_MAP = {
    "activity_summary": ["TotalActive", "calburn", "TotalCaloriesBurned", "TotalDistance", "step", "WaterIntake"],
    "sequential_data": ["BloodGlucose", "BloodOxygen", "HeartRate", "skintemper", "sleep", "Exercise"],
    "etc": ["BodyComposition", "Nutrition"]
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