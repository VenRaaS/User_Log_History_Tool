#!/bin/bash

hive -e "select * from joshua_453041.demo_weblog_$(date +"%Y%m%d" --date="1 days ago")" > weblog_$(date +"%Y%m%d" --date="1 days ago").tsv ;

scp ~/weblog_$(date +"%Y%m%d" --date="1 days ago").tsv u453041@140.96.83.234:~/  ;

rm weblog_$(date +"%Y%m%d" --date="1 days ago").tsv ;

#end