import requests
import time
import uuid
from datetime import datetime

URL = "http://localhost:8000/send"

# Skenario Funnel per Room
def run_simulation(room_id, phone, channel):
    # 1. Lead Message (Mengandung Keyword sesuai NocoDB)
    lead_payload = {
        "room_id": room_id,
        "sender_phone": phone,
        "timestamp": datetime.utcnow().isoformat(),
        "message_text": "Halo, saya tertarik dengan Promo Bundling Seven Retail",
        "metadata": {
            "channel": channel,
            "is_opening": True
        }
    }
    send_msg(lead_payload)
    time.sleep(1)

    # 2. Chat Tengah (Tanya Jawab Biasa)
    chat_payload = {
        "room_id": room_id,
        "sender_phone": phone,
        "timestamp": datetime.utcnow().isoformat(),
        "message_text": "Apakah stoknya masih tersedia untuk wilayah Jakarta?",
        "metadata": {"channel": channel}
    }
    send_msg(chat_payload)
    time.sleep(1)

    # 3. Booking Message (Memicu booking_date)
    book_payload = {
        "room_id": room_id,
        "sender_phone": phone,
        "timestamp": datetime.utcnow().isoformat(),
        "message_text": "Oke saya mau booking satu slot ya",
        "metadata": {
            "channel": channel,
            "booking_info": {
                "booking_id": f"BK-{uuid.uuid4().hex[:6]}",
                "status": "confirmed"
            }
        }
    }
    send_msg(book_payload)
    time.sleep(1)

    # 4. Transaction Message (Memicu transaction_value)
    trans_payload = {
        "room_id": room_id,
        "sender_phone": "SYSTEM",
        "timestamp": datetime.utcnow().isoformat(),
        "message_text": "Pembayaran Sukses!",
        "metadata": {
            "channel": channel,
            "payment_info": {
                "transaction_id": f"TRX-{uuid.uuid4().hex[:8]}",
                "amount": 250000,
                "status": "PAID"
            }
        }
    }
    send_msg(trans_payload)

def send_msg(payload):
    try:
        response = requests.post(URL, json=payload)
        print(f"Sent to {payload['room_id']} | Text: {payload['message_text'][:30]}... | Status: {response.status_code}")
    except Exception as e:
        print(f"Failed: {e}")

# Main Loop: Simulasi untuk 5 Customer berbeda
channels = ["Instagram Ads", "Google Ads", "Organic", "Facebook"]
for i in range(5):
    print(f"\n🚀 Simulating Funnel for Customer {i+1}...")
    test_room = f"room-retail-{i+100}"
    test_phone = f"6281299900{i}"
    test_channel = channels[i % len(channels)]
    
    run_simulation(test_room, test_phone, test_channel)

print("\n✅ 5 Complete Lead Funnels sent to Kafka")