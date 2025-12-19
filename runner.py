from ext_fuel_data import run_terminus_fetch
from datetime import datetime, timedelta

# 🔹 yesterday (ตามเวลาเครื่อง)
yesterday = datetime.now() - timedelta(days=2)

start = yesterday
end = yesterday

current = start
while current <= end:
    date_str = current.strftime("%Y-%m-%d")
    date_start = f"{date_str} 00:00"
    date_end = f"{date_str} 23:59"
    run_terminus_fetch(date_start, date_end)
    current += timedelta(days=1)
