Framework used: Spark with python


Files:
1. producer.py -> Python script which executes a kafka producer and fetches data from meetup api.
2. streaming.py -> Pyspark script, streams data from the kafka broker and store it to the hdfs inbox folder.
3. wrapper_populate_meetup.sh -> Unix script, moves files from inbox directory to working directory and runs populate_meetup_table.py
4. populate_meetup_table.py -> Pyspark script, ingests data present in working directory to hive table using dataframes.
5. wrapper_populate_trending_topic.sh -> Unix script, gets user input data and runs populate_trending_topics.py.
6. populate_trending_topics.py -> Pyspark script, populates trending topics based on the input.


Commands:
1. python producer.py
2. spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.2 streaming.py 
3. ./wrapper_populate_meetup.sh
4. ./wrapper_populate_trending_topic.sh



Just to add few more things on the implementation.

1. Producer and Spark streaming applications must run continuously.
2. Meetups populating job can be scheduled to run after every hour.
3. Trending populating job is triggered by the user by specifying the user inputs.
4. User can query for the trending topics based on relative and absolute timings. 
5. User can pass city specific query.


Important Notes:
1. Hours query must start/end with 00 minute.
2. Data for 00 to 59 minute is stored in same hour's partition eg. Data for 04:30,04:45 and 04:50 is stored in 04:00 partition.


Future Scope:
User input validation can be implemented.