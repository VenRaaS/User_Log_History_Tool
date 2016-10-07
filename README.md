# User_Log_History_Tool

Hackpad Link : https://hackpad.com/RGs-Project-kezutkkkioJ

RGs Project
商品歷程（Rolling-Goods）
Data Proprocessing
General workflow
using the table on hive 147 
require hive table for each firm:
weblog、reclog、unima_goods、unima_category
then use hive-ql script to generate the table which is named "rgs_xxxxx_date" that including all information we plan to show.

Source code Link 

RGs_table.sql : 
step 1. 
join two table : weblog、reclog on now_rec=recomd_id 並且利用 LATERAL VIEW OUTER 指令將推薦清單的array 從一列拆解成多列，結果存成step_1 table。
step 2.
將step_1與unima_goods做 join on recomd_gid=gid,加入被推薦商品的名稱、img_url...等資訊
step 3.
用group by 配合collect_list函數,將原先拆成多列的資料合併回一列
step 4.
再與unima_goods做 join ,加入當前商品頁的商品名稱、img_url...等資訊
step 5.
再與unima_category join 加入分類名稱欄位
step 6.
加上guid的index，用以分別不同guid
Final step.
接收由generate_many_days.sh或是daliy_shell_script.sh所傳來的日期變數，用這個日期create出new table，
叫做"rgs_xxxxx_xxxxxxxx"
[firm]     [date]


daliy_shell_script.sh :
Set the cronjob into the linux terminate using 
"20 30 * * * source home/W100.ITRI/u453041/使用者瀏覽歷程/gohappy/RGs_table.sql"
Then you can run the script at 20:30 every day.
generate_many_days.sh :
Alter the date in this file to you want to generate.
Use the command  " source -f  使用者瀏覽歷程/gohappy/RGs_table.sql "
Then you can get many tables named  "rgs_xxxxx_date" in the Hive 141 joshua_453041 DB once a time.


momo's special case（momo 少 now_rec）
Since momo's weblog didn't have now_rec value, we should use some specific method to reconstruct the now_rec column like following process:
pre_1 : 將reclog與recomder join
pre_2 : 依session和page_type為單位將weblog和reclog的序列依照順序做出編號，成為pre_2_weblog和pre_2_reclog
pre_3 : 依page_type為gop,p,cap,others將pre_2_weblog分成四個部分
pre_4 : 依recomder_pgty為Main,Goods,Goods_CS,Category將pre_2_reclog分成四個部分
pre_5 : 將pre_3和pre_4的結果依相對應的page_type分別做join，最後再將之union起來，就完成了重建now_rec的動作


Automation

1.Dump data from Hive (.147)


2.Load data into PostgreSQL (.234)


Architecture

User Guide

