import json
import time
import argparse
from kafka import KafkaProducer

def gold_extract(timeline_data: dict):
    match_id = timeline_data.get("metadata", {}).get("matchId", "UNKNOWN_MATCH")
    frames = timeline_data.get("info", {}).get("frames",[])

    for frame in frames:
        pf = frame.get("participantFrames", {}) or {}

        if pf and not all(isinstance(k, str) for k in pf.keys()):
            pf = {str(k): v for k, v in pf.items()}

        def tg(i: int) -> int:
            return (pf.get(str(i)) or {}).get("totalGold", 0)

        blue_gold = sum(tg(i) for i in range(1, 6))
        red_gold  = sum(tg(i) for i in range(6, 11))

        ts = frame.get("timestamp", 0)

        yield {
            "matchId": match_id,
            "frameTimestamp": ts,
            "frameMinute": ts // 60000,
            "blueTotalGold": blue_gold,
            "redTotalGold": red_gold,
            "goldDiff": blue_gold - red_gold,
            "eventTs": ts,
        }


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--file", required=True)
    p.add_argument("--topic", default="lol.timeline.frames")
    p.add_argument("--bootstrap-server", default="kafka:19092")
    p.add_argument("--speed", type=int, default=60) 
    args = p.parse_args()

#프로듀서생성
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_server,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        linger_ms=10,
        compression_type="lz4",   
        acks="all",
        retries=3,
        request_timeout_ms=30000,
        max_in_flight_requests_per_connection=1, 
    )

    with open(args.file) as f:
        data = json.load(f)

    prev = 0
    for msg in gold_extract(data):
        producer.send(args.topic, key=msg["matchId"], value=msg)
        print(f"send {msg}")
        dt = (msg["frameTimestamp"] - prev) / 1000.0 / args.speed
        time.sleep(max(0, dt))
        prev = msg["frameTimestamp"]
    producer.flush()
    print("producer fin")
