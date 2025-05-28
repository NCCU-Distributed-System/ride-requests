from kafka import KafkaProducer
import json
import random
import time
import datetime

# NCCU coordinates as a reference
NCCU_LAT, NCCU_LON = 24.9886, 121.5786

# Simulated passenger IDs
passenger_pool = [2001 + i for i in range(12)]

# Basic location-based zoning
def classify_zone(lat, lon):
    if lat > NCCU_LAT and lon < NCCU_LON:
        return "文山區"
    elif lat > NCCU_LAT and lon >= NCCU_LON:
        return "中正區"
    elif lat <= NCCU_LAT and lon < NCCU_LON:
        return "信義區"
    else:
        return "大安區"

# Generate a single ride request event
def generate_ride_request(passenger_id):
    origin_lat = round(random.uniform(24.9820, 25.0020), 7)
    origin_lon = round(random.uniform(121.5700, 121.5900), 7)
    dest_lat = round(random.uniform(24.9850, 25.0050), 7)
    dest_lon = round(random.uniform(121.5750, 121.5950), 7)

    request_time = datetime.datetime.now().isoformat()
    service_type = random.choice(["快車", "共乘", "尊榮"])
    zone = classify_zone(origin_lat, origin_lon)

    return {
        "event_type": "ride_request",
        "passenger_id": passenger_id,
        "timestamp": request_time,
        "data": {
            "pickup": {"lat": origin_lat, "lon": origin_lon},
            "dropoff": {"lat": dest_lat, "lon": dest_lon},
            "service": service_type,
            "zone": zone
        }
    }

# Produce events to Kafka topic
def produce_ride_requests(bootstrap_servers="140.119.164.16:9092", topic="ride-requests", duration=30):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
    )

    print("[Producer] Starting ride request simulation...")
    start = time.time()
    counter = 0

    try:
        while (time.time() - start) < duration:
            for pid in passenger_pool:
                event = generate_ride_request(pid)
                producer.send(topic, event)
                counter += 1
                print(f"[Ride Request] #{counter} sent for passenger {pid} ({event['data']['service']}) in {event['data']['zone']}")
            time.sleep(2)
    finally:
        producer.flush()
        producer.close()
        print(f"[Producer] Finished. Total ride requests sent: {counter}")

if __name__ == "__main__":
    produce_ride_requests()