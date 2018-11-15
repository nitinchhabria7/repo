from pyspark.sql import SparkSession,functions as f
sqlContext=SparkSession.builder\
	.appName('populating_meetups')\
	.enableHiveSupport()\
	.getOrCreate()
sqlContext.conf.set("hive.exec.dynamic.partition", "true")
sqlContext.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
rsvps=sqlContext.read.json("/user/chhabrianitin72368/datasets/meetup/working/")
rsvps_with_date=rsvps.withColumn('day',f.from_unixtime(rsvps.mtime/1000,'yyyy-MM-dd'))\
	.withColumn('time',f.from_unixtime(rsvps.mtime/1000,'hh-00'))
rsvps_with_date.write.insertInto("Meetups.meetup")
