import json
import random
import time
import uuid
from datetime import datetime,timedelta,timezone

from confluent_kafka import Producer

BOOTSTRAP="localhost:9092"
TOPIC="events"

NUM_USERS=500
NUM_PRODUCTS=50
EVENT_TYPES=["view","add_to_cart","purchase"]
EVENT_WEIGHTS=[0.80,0.15,0.05]

DUPLICATE_RATE=0.05
LATE_RATE=0.05

producer=Producer({"bootstrap.servers":BOOTSTRAP})

def delivery_report(err,msg):
    if err is not None:
        print(f"Delivery failed:{err}")

def make_event():
    event_time=datetime.now(timezone.utc)
    if random.random()<LATE_RATE:
        event_time-=timedelta(minutes=random.randint(10,60))

    return {
        "event_id":str(uuid.uuid4()),
        "user_id":random.randint(1,NUM_USERS),
        "product_id":random.randint(1,NUM_PRODUCTS),
        "event_type":random.choices(EVENT_TYPES,weights=EVENT_WEIGHTS)[0],
        "event_time":event_time.isoformat()
    }

def send(event):
    producer.produce(
        TOPIC,
        key=str(event["user_id"]),
        value=json.dumps(event),
        callback=delivery_report,
    )
    producer.poll(0)


def main(total=2000,events_per_sec=50):
    sent=dups=late=0
    for _ in range(total):
        event=make_event()
        send(event)
        sent+=1

        if random.random()<DUPLICATE_RATE:
            send(event)
            dups+=1

        time.sleep(1/events_per_sec)

        if sent%500==0:
            print(f"sent {sent} events ({dups} duplicates injected)")

    producer.flush()
    print(f"Done. Unique events :{sent}, duplicates injected:{dups}")

if __name__=="__main__":
    main()


