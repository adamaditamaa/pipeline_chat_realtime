import requests
import time
import random
from datetime import datetime

URL = "http://localhost:8000/send"

users = ["adam", "budi", "sarah", "rina"]
messages = [
    "Halo, ada yang bisa bantu?",
    "Koneksi saya putus tadi",
    "Sudah coba restart modem?",
    "Sudah, tapi masih belum bisa",
    "Baik, saya cek dulu ya",
    "Oke ditunggu",
    "Sudah normal sekarang?",
    "Iya sudah bisa, terima kasih ",
    "Sama-sama, senang bisa membantu 😊",
    "Kalau ada kendala lagi kabari ya"
]

for i in range(50):
    sender = random.choice(users)
    receiver = random.choice([u for u in users if u != sender])

    payload = {
        "chat_id": "room-1",
        "sender": sender,
        "receiver": receiver,
        "message": random.choice(messages),
        "timestamp": datetime.utcnow().isoformat()
    }

    try:
        response = requests.post(URL, json=payload)
        print(f"[{i+1}] {sender} -> {receiver}: {payload['message']} | {response.status_code}")
    except Exception as e:
        print(f"[{i+1}] Failed:", e)

    time.sleep(0.2)

print("✅ 50 realistic chat messages sent")