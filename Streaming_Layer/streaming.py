from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
sc = SparkContext()
ssc = StreamingContext(sc, 600)
broker = "ip-172-31-20-247.ec2.internal:6667"
kafka=KafkaUtils.createDirectStream(ssc,['meetups'],{"metadata.broker.list":broker})
kafka.map(lambda data : data[1]).saveAsTextFiles("/user/chhabrianitin72368/datasets/meetup/inbox/meetups-rsvp")
ssc.start()  
ssc.awaitTermination() 
