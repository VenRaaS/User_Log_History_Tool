#!/bin/bash     #宣告此script要在 bash 下執行

echo 'start executing daily script';

#新板
hive -f 使用者瀏覽歷程/momo/RGs_table.sql -hiveconf YEAR=2016 -hiveconf MONTH=02 -hiveconf DATE=25;
hive -f 使用者瀏覽歷程/momo/RGs_table.sql -hiveconf YEAR=2016 -hiveconf MONTH=02 -hiveconf DATE=26;
hive -f 使用者瀏覽歷程/momo/RGs_table.sql -hiveconf YEAR=2016 -hiveconf MONTH=02 -hiveconf DATE=27;
hive -f 使用者瀏覽歷程/momo/RGs_table.sql -hiveconf YEAR=2016 -hiveconf MONTH=02 -hiveconf DATE=28;

#舊版
#hive -f 使用者瀏覽歷程/momo/RGs_table_old.sql -hiveconf YEAR=2016 -hiveconf MONTH=04 -hiveconf DATE=01;

							#執行-f,並傳進設定好的日期變數
