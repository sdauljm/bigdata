flag=`date "+%Y%m%d"`
data_path=/user_log/${flag}/
hive -e "use default; load data inpath '${data_path}' into table user_clicks partition(dt='${flag}');" 
