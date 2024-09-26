from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient

from open_sky import retrieve_data

from datetime import datetime
import time
import asyncio
import json
import sys




#EVENTHUB Should be the key of the EVENT RESSOURCE (Connexion with the Event Hub ressource)
EVENT_HUB_CONNECTION_STR = "Endpoint=sb://event-hub-data.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=R5vOwKq1r2TTFx8sVtjvBuUbxXxrVATnP+AEhJRky68="
EVENT_HUB_NAME = "opensky" #name of hub 


# asynchrone function for communication with cloud service
async def send_data(waiting_time):
    #create a producer to send message to the event hub
    #specify a connection string to your event hubs namespace and
    #the event hub name
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR,eventhub_name=EVENT_HUB_NAME
    )
    async with producer:
        start = time.time()
        # Add events to the batch.
        while time.time() <= start + waiting_time * 60:
            #create a batch
            event_data_batch = await producer.create_batch()

            #for element in retrieve_data():
            data = retrieve_data()
            time_request = data[1]
            if len(data[0]) > 0 :
                try:
                    for element in data[0]:
                        element["time"] = time_request
                        #print(element)
                        event_data_batch.add(EventData(json.dumps(element).replace('None', ''))) #replace None by null
                    #send the batch of events to the event hub.
                    await producer.send_batch(event_data_batch)
                    print(f"Batch sent for time {time_request} \n {'*'*40}")
                except:
                    print("Error while sending data")
            await asyncio.sleep(waiting_time * 60 )


def run(duration, frequence):
    start_time = time.time()
    while time.time() < start_time + 60 * duration:
        asyncio.run(send_data(frequence))

run(float(sys.argv[1]), float(sys.argv[2])) # l'envoie toutes les 60secondes


