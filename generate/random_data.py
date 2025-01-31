import uuid
import random
from datetime import datetime, timedelta
import json
import requests

# 生成100条数据
data = []
end_date = datetime.now()
start_date = end_date - timedelta(days=30)

for _ in range(100):
    # 生成随机日期
    random_date = start_date + timedelta(
        seconds=random.randint(0, int((end_date - start_date).total_seconds()))
    )
    date_str = random_date.strftime("%d/%b/%Y:%H:%M:%S +0000")
    
    # 生成随机IP
    ip = f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}"
    
    entry = {
        "id": str(uuid.uuid4()),
        "datetime": date_str,
        "host": ip,
        "value": random.randint(1, 100)
    }
    data = entry

    # 发送到Parseable
    url = "http://localhost:8000/api/v1/ingest"
    headers = {
        "X-P-Stream": "dp",
        "Authorization": "Basic YWRtaW46YWRtaW4=",
        "Content-Type": "application/json"
    }

    response = requests.post(url, headers=headers, json=data)
    print(f"Status Code: {response.status_code}")