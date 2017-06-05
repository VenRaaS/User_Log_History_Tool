#!/bin/bash     #宣告此script要在 bash 下執行

#產生整個月的 rfm_label
hive -f RFM/RFM_relabel.sql -hiveconf YEAR=$(date +"%Y" --date="1 days ago") -hiveconf MONTH=$(date +"%m" --date="1 days ago") -hiveconf DATE=$(date +"%d" --date="1 days ago");
