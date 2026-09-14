# Welcome to your new notebook
# Type here in the cell editor to add code!
import json
import time
import random
from datetime import datetime
from azure.eventhub import EventHubProducerClient, EventData, TransportType

# 1. Paste connection string copied from Eventstream Custom App
CONNECTION_STR = "Endpoint=sb://esehjnbmbsp4kru4c1e324f.servicebus.windows.net/;SharedAccessKeyName=key_7ebf3b76-53f3-4a2f-a227-b5c26a5378cc;SharedAccessKey=0q0uKjpBxdXPGrjuLxrnHMffSIO60ZdG0+AEhPTJ3Vg=;EntityPath=esehjnbmbsp4kru4c1e324f_eh"

# Create producer client
producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STR,
    transport_type=TransportType.AmqpOverWebsocket,
)

airlines = ["AA", "DL", "UA", "WN", "B6"]
airports = ["ATL", "ORD", "DFW", "DEN", "LAX", "JFK"]

print("Starting real-time flight telemetry producer...")

try:
    with producer:
        while True:
            # Generate simulated flight telemetry payload
            payload = {
                "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "flight_number": random.randint(100, 9999),
                "airline": random.choice(airlines),
                "origin": random.choice(airports),
                "destination": random.choice(airports),
                "current_altitude_ft": random.randint(10000, 38000),
                "speed_knots": random.randint(350, 550),
                "departure_delay_mins": random.choices([0, 5, 12, 45, 120], weights=[70, 15, 8, 5, 2])[0],
                "event_type": "TELEMETRY_UPDATE"
            }
            
            # Send batch of 1 event
            event_data_batch = producer.create_batch()
            event_data_batch.add(EventData(json.dumps(payload)))
            producer.send_batch(event_data_batch)
            
            print(f"Sent event: Flight {payload['airline']}{payload['flight_number']} | Delay: {payload['departure_delay_mins']}m")
            
            # Wait 2 seconds between events
            time.sleep(2)

except KeyboardInterrupt:
    print("Producer stopped by user.")