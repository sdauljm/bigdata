flag=`date "+%Y%m%d"`
hive -e "use default; insert into table user_info select count(distinct user_id) as active_num, dt from user_clicks where dt ='${flag}'"
