
![alt text](https://github.com/VenRaaS/User_Log_History_Tool/blob/master/RGs/RGs_%E7%B5%90%E6%A7%8B%E5%9C%96.jpg)


程式碼使用說明:
(請參照 RGs_結構圖 )

1.RGs_table_gen.sql : 用來生產每天的RGs table (input => weblog、reclog)

2.daily_shell_script.sh : 用crontab的方式置入每日排程中
                          會自動把昨天的年、月、日餵入RGs_table_gen.sql中執行
			  創造出昨天的RGs table

3.generate_many_days.sh : 批次式的shell執行檔，可自己編輯要未入哪幾天的年、月、日
                          通常用於掉資料時需要補資料的時候

-------------------------------------------------------------------------------------

########### 手動化產生table ###########

step-1.修改generate_many_days.sh檔  , 加入想要產生table的日期

step-2.在linux command line 輸入:

	source 使用者瀏覽歷程/gohappy/generate_many_days.sh

	或

	source 使用者瀏覽歷程/momo/generate_many_days.sh


step-3.press "enter"

########### 設置crontab自動化 ###########

step-1.在daily_shell_script.sh中用DATE=$(date +"%Y%m%d" --date="1 days ago")取出昨天日期
       再透過執行指令的後面加上 -hiveconf 傳入DATE變數
       執行指令的file中用${hiveconf:DATE}接收變數

*注意:不要加上無謂的引號'或加號+ 因為linux中字串相加是不需要其他運算元的,放在一起就好

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


1.若要執行daily自動化的動作,直接在linux埋下執行demo_shell_script.sh的程式碼即可

2.若要選擇create任意一天的table,則
    將demo_shell_script.sh的程式碼中的 YEAR,MONTH,DATE 改成想要的年,月,日即可



