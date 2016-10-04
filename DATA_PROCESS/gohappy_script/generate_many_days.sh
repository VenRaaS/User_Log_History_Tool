#!/bin/bash     #宣告此script要在 bash 下執行

echo 'start executing daily script';

hive -f 使用者瀏覽歷程/gohappy/RGs_table_array.sql -hiveconf YEAR=2016 -hiveconf MONTH=07 -hiveconf DATE=18;
hive -f 使用者瀏覽歷程/gohappy/RGs_table_column.sql -hiveconf YEAR=2016 -hiveconf MONTH=07 -hiveconf DATE=18;

							#執行-f,並傳進設定好的日期變數
