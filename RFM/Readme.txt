程式碼使用說明:
(請參照 RFM_結構圖 )

1.RFM_relabel.sql: 更新 rfm_label_table

2.RFM_count_script.sql: 以rfm_label_table的分群為基準，計算當天各群的上線人數
                        並存入DB.rfm_123_daily_record的table中。

3.RFM_hr_script.sql:    以rfm_label_table的分群為基準，計算各群當天的點擊率
                        並存入DB.rfm_123_hr_record的table中。

4.RFM_monthly_relabel.sh: 控制RFM_relabel.sql，埋在crontab中，一個月執行一次。

5.RFM_daily_script.sh: 同時控制RFM_count_script.sql + RFM_hr_script.sql
                       埋在crontab中，一天執行一次。

6.RFM_generate_many_days.sh: 批次式的shell執行檔，可自己編輯要計算的年、月、日，
                             同時控制RFM_count_script.sql + RFM_hr_script.sql，
                             通常用於掉資料時需要補資料的時候。

-------------------------------------------------------------------------------------

########### 手動更新 DB.rfm_label_table ###########

step-1.編輯generate_many_days.sh檔，其它都註解掉，只留下:
       hive -f RFM/RFM_relabel.sql -hiveconf YEAR=2017 -hiveconf MONTH=06 -hiveconf DATE=01;
       
step-2.在linux command line 輸入:

	source RFM/generate_many_days.sh

step-3.press "enter"，就可以得到以6/1為基礎算出來的 RFM分群table了。

########### 手動insert資料進table ###########

step-1.修改generate_many_days.sh檔，加入想要insert的 RFM record的日期

step-2.在linux command line 輸入:

	source RFM/generate_many_days.sh

step-3.press "enter"

########### 設置crontab自動化 ###########

step-1.在RFM_daily_script.sh中用DATE=$(date +"%Y%m%d" --date="1 days ago")取出昨天日期
       再透過執行指令的後面加上 -hiveconf 傳入DATE變數
       執行指令的file中用${hiveconf:DATE}接收變數

step-2.用crontab加入排程的方法:
     (1)用crontab -e 編輯排程
     (2)再按i進入vim的編輯模式
     (3)編輯完後按esc
     (4)接著輸入:wq 按enter (表示write & quit)

*透過crontab -l 可以看目前有的排程

step-3.設定排程的格式
	ex:
        	20 15 * * * source home/W100.ITRI/u453041/daily_shell_script.sh 
		 ^  ^	^     ^       ^
	表示:在	分 時  any   執行   script    
        	        ˇ
  			ˇ
  	         (天、週、月)



