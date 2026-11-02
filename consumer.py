import json
import psycopg2
from psycopg2.extras import execute_values
from confluent_kafka import Consumer

BOOTSTRAP="localhost:9092"
TOPIC="events"
GROUP_ID="clickstream-loader"
BATCH_SIZE=100

DB=dict(host="localhost",port=5432,dbname="clickstream",user="pipeline",password="pipeline")

INSERT_SQL="""
    INSERT INTO events(event_id,user_id,product_id,event_type,event_time) VALUES %s
    ON CONFLICT (event_id) DO NOTHING
    """

consumer=Consumer({
    "bootstrap.servers":BOOTSTRAP,
    "group.id":GROUP_ID,
    "auto.offset.reset":"earliest",
    "enable.auto.commit":False,
})

consumer.subscribe([TOPIC])

conn=psycopg2.connect(**DB)

def parse(msg):
    e=json.loads(msg.value())
    return (e["event_id"],e["user_id"],e["product_id"],e["event_type"],e["event_time"])

def main():
    consumed=inserted=skipped_dups=bad=0
    print("Consumer started.Press Ctrl+C to stop.")
    try:
        while True:
            msgs=consumer.consume(num_messages=BATCH_SIZE,timeout=1.0)
            if not msgs:
                continue

            rows=[]
            for m in msgs:
                if m.error():
                    print(f"kafka error: {m.error()}")
                    continue
                try:
                    rows.append(parse(m))
                except (json.JSONDecodeError,KeyError) as ex:
                    bad+=1
                    print(f"Bad message at offset {m.offset()}:{ex}")

            if rows:
                with conn.cursor() as cur:
                    execute_values(cur,INSERT_SQL,rows,page_size=len(rows))
                    new_rows=cur.rowcount

                conn.commit()
                inserted+=new_rows
                skipped_dups+=len(rows)-new_rows

            consumer.commit(asynchronous=False)
            consumed+=len(msgs)
            print(f"consumed={consumed} inserted={inserted}" 
                  f"duplicates_skipped={skipped_dups} bad={bad}")
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        consumer.close()
        conn.close()

if __name__=="__main__":
    main()
