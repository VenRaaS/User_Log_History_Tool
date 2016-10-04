#!/bin/bash     #宣告此script要在 bash 下執行

echo 'start executing daily script';
hive -f daily_script_create_demo_table.sql -hiveconf YEAR=$(date +"%Y" --date="1 days ago") -hiveconf MONTH=$(date +"%m" --date="1 days ago") -hiveconf DATE=$(date +"%d" --date="1 days ago");     #執行-f,並傳進設定好的日期變數

echo 'create daliy demo table sucessfully';

# 易改版:
# hive -f daily_script_create_demo_table.sql -hiveconf YEAR=2016 -hiveconf MONTH=04 -hiveconf DATE=08;

