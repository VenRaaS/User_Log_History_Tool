-- Recency  -- Frequency -- Monetary 
use joshua_453041;

drop table if exists RFM_count;
drop table if exists RFM_today;

--construct table to restore r*f*m (each 1~3)  

--create table rfm_r (r int);
--create table rfm_f (f int);
--create table rfm_m (m int);
--insert into table rfm_r values(1),(2),(3);
--insert into table rfm_f values(1),(2),(3);
--insert into table rfm_m values(1),(2),(3);


--去查當天每筆weblog在rfm是屬於哪一群的
create table RFM_today as

select uid,if(w1.label="old_user",w1.r,"new_user") r
          ,if(w1.label="old_user",w1.f,"new_user") f
          ,if(w1.label="old_user",w1.m,"new_user") m
from (

select t1.uid,t2.r,t2.f,t2.m,if(t2.uid is not NULL,"old_user","new_user") label
from gohappy_unima.all_weblog t1 left join joshua_453041.RFM_label_table t2 on t1.uid = t2.uid
where t1.log_mon_i = '${hiveconf:YEAR}-${hiveconf:MONTH}'
  and to_date(t1.api_logtime) = '${hiveconf:YEAR}-${hiveconf:MONTH}-${hiveconf:DATE}' and t1.uid != '' --先暫時把uid空的濾掉

) w1
;

--計算rfm各群的數量 和 new_user的數量
create table RFM_count as
    
    select t1.*,'RFM' as RFM,if(t2.num is NULL,0,t2.num) count 
    from (select * from rfm_r cross join rfm_f cross join rfm_m order by r,f,m) t1 
    left join (select R,F,M,count(uid) num from joshua_453041.RFM_today group by R,F,M order by R,F,M) t2 
    on t1.r=t2.r and t1.f=t2.f and t1.m=t2.m
    
    UNION
    
    select R,F,M,'RFM' as RFM,count(uid) count from joshua_453041.RFM_today where R="new_user" group by R,F,M
;
-- 開一個table RFM_record 來insert每天算好的值
create table if not exists rfm_123_daily_record
(
  date_time string,
  R1_F1_M1 int,R1_F1_M2 int,R1_F1_M3 int,
  R1_F2_M1 int,R1_F2_M2 int,R1_F2_M3 int,
  R1_F3_M1 int,R1_F3_M2 int,R1_F3_M3 int,
  R2_F1_M1 int,R2_F1_M2 int,R2_F1_M3 int,
  R2_F2_M1 int,R2_F2_M2 int,R2_F2_M3 int,
  R2_F3_M1 int,R2_F3_M2 int,R2_F3_M3 int,
  R3_F1_M1 int,R3_F1_M2 int,R3_F1_M3 int,
  R3_F2_M1 int,R3_F2_M2 int,R3_F2_M3 int,
  R3_F3_M1 int,R3_F3_M2 int,R3_F3_M3 int,
  new_user int,
  insert_time string);
  
INSERT INTO joshua_453041.rfm_123_daily_record  --注意,此處結尾不能加分號

-- count each group number
select '${hiveconf:YEAR}-${hiveconf:MONTH}-${hiveconf:DATE}'
    ,collect_list(count)[0],collect_list(count)[1],collect_list(count)[2],collect_list(count)[3],collect_list(count)[4]
    ,collect_list(count)[5],collect_list(count)[6],collect_list(count)[7],collect_list(count)[8],collect_list(count)[9]
    ,collect_list(count)[10],collect_list(count)[11],collect_list(count)[12],collect_list(count)[13],collect_list(count)[14]
	,collect_list(count)[15],collect_list(count)[16],collect_list(count)[17],collect_list(count)[18],collect_list(count)[19]
	,collect_list(count)[20],collect_list(count)[21],collect_list(count)[22],collect_list(count)[23],collect_list(count)[24]
	,collect_list(count)[25],collect_list(count)[26],collect_list(count)[27]
    ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')
from rfm_count
group by rfm
;

--drop table if exists RFM_count;

--end










