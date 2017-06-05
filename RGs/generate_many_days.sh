#!/bin/bash     #宣告此script要在 bash 下執行

echo 'start executing daily script'
echo 'cron job testing' > cron.txt 

hive -f 使用者瀏覽歷程/gohappy/RGs_table_gen.sql -hiveconf YEAR=2017 -hiveconf MONTH=03 -hiveconf DATE=01





							#執行-f,並傳進設定好的日期變數
