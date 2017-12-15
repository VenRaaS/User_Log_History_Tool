#!/bin/bash     #宣告此script要在 bash 下執行

echo 'start executing daily script';

#執行-f,並傳進設定好的日期變數
hive -f RGs_data_preparation/gohappy/RGs_table_gen.sql -hiveconf YEAR=$(date +"%Y" --date="1 days ago") -hiveconf MONTH=$(date +"%m" --date="1 days ago") -hiveconf DATE=$(date +"%d" --date="1 days ago");     

echo 'create daliy data table sucessfully';

# 易改版:
# hive -f 使用者瀏覽歷程/gohappy/RGs_table_gen.sql -hiveconf YEAR=2016 -hiveconf MONTH=04 -hiveconf DATE=08;

