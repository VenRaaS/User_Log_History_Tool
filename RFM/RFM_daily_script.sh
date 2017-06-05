#!/bin/bash     #宣告此script要在 bash 下執行

# 每天要執行的daily script

#算每天的 rfm count
hive -f RFM/RFM_count_script.sql -hiveconf YEAR=$(date +"%Y" --date="1 days ago") -hiveconf MONTH=$(date +"%m" --date="1 days ago") -hiveconf DATE=$(date +"%d" --date="1 days ago");

#算每天的 rfm hr
hive -f RFM/RFM_hr_script.sql -hiveconf DATE=$(date +"%Y-%m-%d" --date="1 days ago");

# end
