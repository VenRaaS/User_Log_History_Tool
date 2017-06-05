#!/bin/bash     #宣告此script要在 bash 下執行

#產生整個月的 rfm_label
#hive -f RFM/rfm_relabel.sql -hiveconf YEAR=2017 -hiveconf MONTH=04 -hiveconf DATE=01;

#算每天的 rfm_count
hive -f RFM/RFM_count_script.sql -hiveconf YEAR=2017 -hiveconf MONTH=05 -hiveconf DATE=01;
hive -f RFM/RFM_count_script.sql -hiveconf YEAR=2017 -hiveconf MONTH=05 -hiveconf DATE=02;
hive -f RFM/RFM_count_script.sql -hiveconf YEAR=2017 -hiveconf MONTH=05 -hiveconf DATE=03;
hive -f RFM/RFM_count_script.sql -hiveconf YEAR=2017 -hiveconf MONTH=05 -hiveconf DATE=04;
hive -f RFM/RFM_count_script.sql -hiveconf YEAR=2017 -hiveconf MONTH=05 -hiveconf DATE=05;
hive -f RFM/RFM_count_script.sql -hiveconf YEAR=2017 -hiveconf MONTH=05 -hiveconf DATE=06;
hive -f RFM/RFM_count_script.sql -hiveconf YEAR=2017 -hiveconf MONTH=05 -hiveconf DATE=07;
hive -f RFM/RFM_count_script.sql -hiveconf YEAR=2017 -hiveconf MONTH=05 -hiveconf DATE=08;
hive -f RFM/RFM_count_script.sql -hiveconf YEAR=2017 -hiveconf MONTH=05 -hiveconf DATE=09;
hive -f RFM/RFM_count_script.sql -hiveconf YEAR=2017 -hiveconf MONTH=05 -hiveconf DATE=10;
hive -f RFM/RFM_count_script.sql -hiveconf YEAR=2017 -hiveconf MONTH=05 -hiveconf DATE=11;
hive -f RFM/RFM_count_script.sql -hiveconf YEAR=2017 -hiveconf MONTH=05 -hiveconf DATE=12;
hive -f RFM/RFM_count_script.sql -hiveconf YEAR=2017 -hiveconf MONTH=05 -hiveconf DATE=13;
hive -f RFM/RFM_count_script.sql -hiveconf YEAR=2017 -hiveconf MONTH=05 -hiveconf DATE=14;
hive -f RFM/RFM_count_script.sql -hiveconf YEAR=2017 -hiveconf MONTH=05 -hiveconf DATE=15;

#算每天的 rfm_hr
hive -f RFM/RFM_hr_script.sql -hiveconf DATE=2017-05-01;
hive -f RFM/RFM_hr_script.sql -hiveconf DATE=2017-05-02;
hive -f RFM/RFM_hr_script.sql -hiveconf DATE=2017-05-03;
hive -f RFM/RFM_hr_script.sql -hiveconf DATE=2017-05-04;
hive -f RFM/RFM_hr_script.sql -hiveconf DATE=2017-05-05;
hive -f RFM/RFM_hr_script.sql -hiveconf DATE=2017-05-06;
hive -f RFM/RFM_hr_script.sql -hiveconf DATE=2017-05-07;
hive -f RFM/RFM_hr_script.sql -hiveconf DATE=2017-05-08;
hive -f RFM/RFM_hr_script.sql -hiveconf DATE=2017-05-09;
hive -f RFM/RFM_hr_script.sql -hiveconf DATE=2017-05-10;
hive -f RFM/RFM_hr_script.sql -hiveconf DATE=2017-05-11;
hive -f RFM/RFM_hr_script.sql -hiveconf DATE=2017-05-12;
hive -f RFM/RFM_hr_script.sql -hiveconf DATE=2017-05-13;
hive -f RFM/RFM_hr_script.sql -hiveconf DATE=2017-05-14;
hive -f RFM/RFM_hr_script.sql -hiveconf DATE=2017-05-15;



#執行-f,並傳進設定好的日期變數

