printf "Important Notes:\n1.Hours query must start/end with 00 minute\n2.Data for 00 to 59 minute is stored in same hour's partition eg. Data for 04:30,04:45 and 04:50 is stored in 04:00 partition.\n"
printf "Based on how you want to query please provide your input\na: Absolute range\nrh:Relative range in hours\nrd: Relative range in Days\n"
read mode
rh_flag=0
rd_flag=0
a_flag=0
case $mode in
rh)
echo "Enter relative hours(N hours ago)"
read relative_hours
rh_flag=1
;;
rd)
echo "Enter relative days (N days ago)"
read relative_days
rd_flag=1
;;
a)
echo "Enter start date(yyyy-mm-dd)"
read start_date
echo "Enter start time(hh-mm)"
read start_time
echo "Enter end date(yyyy-mm-dd)"
read end_date
echo "Enter end time(hh-mm)"
read end_time
a_flag=1
;;
esac

printf "Do you want city specific trends?(y/n):\t"
read city_choice
if [ $city_choice == "y" ]
then
	printf "Enter city name:"
	read city
else
	city="NA"
fi
printf "Enter # of trending topics you are looking for:\t"
read topN

if [ $rh_flag == 1 ]
then
        start_date=`date +%Y-%m-%d -d "$relative_hours hours ago"`
        start_time=`date +%H-00 -d "$relative_hours hours ago"`
        end_date=`date +%Y-%m-%d`
        end_time=`date +%H-00`
        tmp_date=$start_date
        tmp_time=$start_time
        condition=""
        if [ $start_date == $end_date ]
        then
                condition="day='$start_date' and ( time>='$start_time' or time=<'$end_time')"
        else
                while [ $(date -d $tmp_date +%s) -le $(date -d $end_date +%s) ]
                do
                        if [ $tmp_date == $start_date ]
                        then
                                condition=$condition"(day='$start_date' and time>='$start_time')"
                        elif [ $tmp_date == $end_date ]
                        then
                                condition=$condition" or (day='$end_date' and time<='$end_time')"
                        else
                                condition=$condition"  or (day='$tmp_date' and time>='00-00')"
                        fi
                        tmp_date=$(date +%Y-%m-%d -d "$tmp_date + 1 day")
                done
        fi
elif [ $rd_flag == 1 ]
then
        start_date=`date +%Y-%m-%d -d "$relative_days days ago"`
        condition="day>='$start_date'"
else
        tmp_date=$start_date
        tmp_time=$start_time
        condition=""
        if [ $start_date == $end_date ]
        then
                condition="day='$start_date' and ( time>='$start_time' or time=<'$end_time')"
        else
                while [ $(date -d $tmp_date +%s) -le $(date -d $end_date +%s) ]
                do
                        if [ $tmp_date == $start_date ]
                        then
                                condition=$condition"(day='$start_date' and time>='$start_time')"
                        elif [ $tmp_date == $end_date ]
                        then
                                condition=$condition" or (day='$end_date' and time<='$end_time')"
                        else
                                condition=$condition"  or (day='$tmp_date' and time>='00-00')"
                        fi
                        tmp_date=$(date +%Y-%m-%d -d "$tmp_date + 1 day")
                done
        fi
fi

if [ $city != "NA" ]
then
	condition="($condition) and (group.group_city='$city')"
else
	condition="$condition"
fi
#echo $condition"-->"$topN

hdfs dfs -rm -r /user/chhabrianitin72368/datasets/meetup/output/trendingTopics/
echo "Running job......."
spark-submit populate_trending_topics.py "$condition" $topN
