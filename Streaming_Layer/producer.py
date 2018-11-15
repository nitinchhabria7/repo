from pykafka import KafkaClient
client = KafkaClient(hosts="ip-172-31-20-247.ec2.internal:6667")
topic = client.topics['meetups']
import requests
req=requests.get("https://stream.meetup.com/2/rsvps",stream=True)
producer=topic.get_sync_producer()
for line in req.iter_lines():
	producer.produce(line)
