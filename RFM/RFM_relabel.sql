--在每個月月初執行

--僅用 "昨天weblog的uid" join "一年內的orderlist" 去標記每個uid的RFM
--並 mapping RFM to {1,2,3}  
--use threshlod : { R:[  30 , 180] ,F:[1000,100] ,M:[1000000,50000]}
--                    [1個月,半年]    [  次數  ]    [     金額    ] 

use joshua_453041;

drop table if exists RFM_label_table;

create table RFM_label_table as

select uid
	,if(Recency<=30,1,if(Recency<=180,2,3)) as R
	,if(Frequency>=1000,1,if(Frequency>=100,2,3)) as F
	,if(Monetary>=1000000,1,if(Monetary>=50000,2,3)) as M
	
from
	(select t1.uid,MAX(t2.order_date) last_consum_date
       ,datediff(date_format('${hiveconf:YEAR}-${hiveconf:MONTH}-${hiveconf:DATE}','yyyy-MM-dd'),to_date(MAX(t2.order_date))) as Recency
       ,count(t2.order_date) as Frequency
       ,sum(t2.final_amt) as Monetary
	from (select * from gohappy_unima.all_weblog where to_date(api_logtime) 
	                                    between date_format(date_sub('${hiveconf:YEAR}-${hiveconf:MONTH}-${hiveconf:DATE}',365),'yyyy-MM-dd') 
										and date_format(date_sub('${hiveconf:YEAR}-${hiveconf:MONTH}-${hiveconf:DATE}',1),'yyyy-MM-dd')
          ) t1 
	left join (select * from gohappy_unima.all_orderlist where to_date(order_date) 
	                                    between date_format(date_sub('${hiveconf:YEAR}-${hiveconf:MONTH}-${hiveconf:DATE}',365),'yyyy-MM-dd') 
										and date_format(date_sub('${hiveconf:YEAR}-${hiveconf:MONTH}-${hiveconf:DATE}',1),'yyyy-MM-dd')
          )t2 
	on t1.uid=t2.uid
	
	group by t1.uid) w1;

--end 