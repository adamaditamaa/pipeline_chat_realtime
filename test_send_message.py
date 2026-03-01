import requests
import time
import uuid
import random
from datetime import datetime

url = "http://localhost:8000/send"
duration = 80 

def send_msg(payload):
    try:
        response = requests.post(url, json=payload)
        status = "success" if response.status_code == 200 else f"error {response.status_code}"
        print(f"sent to {payload['room_id']} | text: {payload['message_text'][:30]}... | status: {status}")
    except Exception as e:
        print(f"failed: {e}")

def run_complex_simulation(room_id, phone, channel, scenario):
    send_msg({
        "room_id": room_id,
        "sender_phone": phone,
        "timestamp": datetime.utcnow().isoformat(),
        "message_text": "halo, saya tertarik dengan Promo bundling seven retail",
        "metadata": {"channel": channel, "is_opening": True}
    })
    time.sleep(1)

    if scenario == "lead_only":
        send_msg({
            "room_id": room_id,
            "sender_phone": phone,
            "timestamp": datetime.utcnow().isoformat(),
            "message_text": "cuma tanya-tanya dulu ya kak, makasih.",
            "metadata": {"channel": channel}
        })
        return

    send_msg({
        "room_id": room_id,
        "sender_phone": phone,
        "timestamp": datetime.utcnow().isoformat(),
        "message_text": "saya mau booking slot promonya",
        "metadata": {
            "channel": channel, 
            "booking_info": {
                "booking_id": f"bk-{uuid.uuid4().hex[:4]}", 
                "status": "confirmed"
            }
        }
    })
    time.sleep(1)

    if scenario == "book_only":
        send_msg({
            "room_id": room_id,
            "sender_phone": phone,
            "timestamp": datetime.utcnow().isoformat(),
            "message_text": "nanti saya kabari lagi untuk pembayarannya.",
            "metadata": {"channel": channel}
        })
        return

    send_msg({
        "room_id": room_id,
        "sender_phone": "system",
        "timestamp": datetime.utcnow().isoformat(),
        "message_text": "pembayaran terverifikasi!",
        "metadata": {
            "channel": channel,
            "payment_info": {
                "transaction_id": f"trx-{uuid.uuid4().hex[:6]}",
                "amount": random.choice([150000, 250000, 500000]),
                "status": "PAID"
            }
        }
    })

def run_non_lead(room_id, phone):
    send_msg({
        "room_id": room_id,
        "sender_phone": phone,
        "timestamp": datetime.utcnow().isoformat(),
        "message_text": random.choice(["p", "cek alamat", "info lokasi", "salah sambung"]),
        "metadata": {"channel": "Organic", "is_opening": False}
    })

channels = ["Facebook", "Organic", "Google Ads", "Instagram Ads"]
start_time = time.time()
customer_count = 0

print(f"starting mixed funnel simulation for {duration} seconds")

while (time.time() - start_time) < duration:
    customer_count += 1
    current_elapsed = int(time.time() - start_time)
    test_room = f"room-{uuid.uuid4().hex[:5]}"
    test_phone = f"62812{str(uuid.uuid4().int)[:8]}"
    
    choice = random.random()
    
    if choice < 0.3:
        print(f"\n[non-lead] customer {customer_count} (elapsed: {current_elapsed}s)")
        run_non_lead(test_room, test_phone)
    elif choice < 0.5:
        print(f"\n[lead-only] customer {customer_count} (elapsed: {current_elapsed}s)")
        run_complex_simulation(test_room, test_phone, random.choice(channels), "lead_only")
    elif choice < 0.7:
        print(f"\n[book-only] customer {customer_count} (elapsed: {current_elapsed}s)")
        run_complex_simulation(test_room, test_phone, random.choice(channels), "book_only")
    else:
        print(f"\n[full-conversion] customer {customer_count} (elapsed: {current_elapsed}s)")
        run_complex_simulation(test_room, test_phone, random.choice(channels), "full_conversion")
    
    time.sleep(2)

print(f"\nsimulation finished. total: {customer_count} customers")