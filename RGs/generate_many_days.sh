#!/bin/bash     #宣告此script要在 bash 下執行

echo 'start executing daily script'
echo 'cron job testing' > cron.txt 

for ((d=20170605;d<=20170615;d++))
do
hive -f RGs_data_preparation/gohappy/RGs_table_gen.sql -hiveconf YEAR=${d:0:4} -hiveconf MONTH=${d:4:2} -hiveconf DATE=${d:6:2}
done




							#執行-f,並傳進設定好的日期變數
