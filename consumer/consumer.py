import json
import time
import os
import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
DB_URL = os.getenv("DATABASE_URL", "postgresql://user:password@dwh_postgres:5432/chat_db")

def get_db_connection():
    while True:
        try:
            conn = psycopg2.connect(DB_URL)
            print("Success Connect DB")
            return conn
        except Exception as e:
            print(f"Waiting for DB {e}")
            time.sleep(2)

#  DB connection and cursor
conn = get_db_connection()
cur = conn.cursor()

def init_db():
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_data.chat_logs (
                id SERIAL PRIMARY KEY,
                message_data JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
    except Exception as e:
        print(f"Error initializing DB: {e}")
        conn.rollback()

init_db()

# Konfigurasi Kafka Consumer
consumer = KafkaConsumer(
    "chat_topic",
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id="chat-batch-consumer",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

batch_size = 100
buffer = []
last_commit_time = time.time()


try:
    while True:
        msg_pack = consumer.poll(timeout_ms=1000)

        for tp, messages in msg_pack.items():
            for message in messages:
                buffer.append((json.dumps(message.value),))

        current_time = time.time()
        if buffer and (len(buffer) >= batch_size or (current_time - last_commit_time > 5)):
            
            while True:
                try:
                    execute_values(cur, 
                        "INSERT INTO raw_data.chat_logs (message_data) VALUES %s", 
                        buffer
                    )                    
                    conn.commit()                    
                    consumer.commit() 
                    
                    print(f"Batch Inserted {len(buffer)} messages.")
                    buffer.clear()
                    last_commit_time = current_time
                    break 
                
                except (psycopg2.InterfaceError, psycopg2.OperationalError) as e:
                    print(f"DB connection lost, reconnecting ({e})")
                    conn = get_db_connection()
                    cur = conn.cursor()
                
                except Exception as e:
                    print(f"Error inserting batch: {e}")
                    conn.rollback()
                    buffer.clear()
                    break 
except KeyboardInterrupt:
    print("Stopping consumer")
finally:
    cur.close()
    conn.close()