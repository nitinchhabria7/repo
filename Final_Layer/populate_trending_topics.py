from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import explode
from pyspark.sql import SparkSession,functions as f
import sys

sqlContext=SparkSession.builder\
	.appName('trending_topics')\
	.enableHiveSupport()\
	.getOrCreate()
conditions=sys.argv[1]
topN=int(sys.argv[2])
rsvps=sqlContext.table('Meetups.meetup')
trending=rsvps.filter(conditions).select(explode(rsvps.group.group_topics).alias('topic'))\
        .select('topic.topic_name')\
        .groupBy('topic_name')\
        .agg(f.count('topic_name').alias('topic_count'))\
        .orderBy('topic_count',ascending=[0])\
        .limit(topN)
trending.rdd.map(lambda rsvp: rsvp[0]).saveAsTextFile("/user/chhabrianitin72368/datasets/meetup/output/trendingTopics")


