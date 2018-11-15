list_files=`hdfs dfs -ls -R /user/chhabrianitin72368/datasets/meetup/inbox/ | grep part | awk '{print($8)}'`
dirs=`hdfs dfs -ls -R /user/chhabrianitin72368/datasets/meetup/inbox/ | grep -v '_SUCCESS\|part'| awk '{print($8)}'`
for file in $list_files
do
	timestamp=`echo $file | cut -d'/' -f7 | cut -d'-' -f3`
	filename=`echo $file | cut -d'/' -f8`
hdfs dfs -mv $file /user/chhabrianitin72368/datasets/meetup/working/$filename-$timestamp
done

for dir in $dirs
do
	hdfs dfs -rm -r $dir
done

spark-submit populate_meetup_table.py
hdfs dfs -mv /user/chhabrianitin72368/datasets/meetup/working/* /user/chhabrianitin72368/datasets/meetup/archive/
