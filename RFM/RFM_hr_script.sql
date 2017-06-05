--計算每天不同RFM分群的hr
use joshua_453041;

drop table if exists rfm_uid2hr;
drop table if exists rfm_hr_today;
drop table if exists rfm_hr_tmp;

--建造當天的uid2hr table
--t1:total view count , t2:hit count
create table rfm_uid2hr as

select t1.uid,count(distinct t2.recomd_id)/t1.total_rec HR,count(distinct t2.recomd_id) hit_num,t1.total_rec view_num,t1.rec_pos,t1.rec_code,t1.device,t1.group_key,t1.rec_type from

(select reclog.uid,ven_guid,rec_pos,rec_code,device,group_key,rec_type,count(distinct recomd_id) total_rec
  from
  (select uid,ven_guid,rec_pos,rec_code,device,group_key,rec_type,recomd_id
   from gohappy_unima.all_reclog where log_mon_i = substring('${hiveconf:DATE}',1,7) 
                            and to_date(insert_dt) = '${hiveconf:DATE}'
						    and  recomd_id is not null and recomd_id !='' 
						    and ven_guid !='' ) reclog
 
   join

  (select uid,get_json_object(regexp_replace(now_rec, "\\[|\\]", ""), '$.rec') as now_rec 
   from gohappy_unima.all_weblog where log_mon_i = substring('${hiveconf:DATE}',1,7) 
						    and to_date(api_logtime) = '${hiveconf:DATE}' 
						    and  now_rec is not null and now_rec !='') weblog
 
   on reclog.recomd_id=weblog.now_rec
   group by reclog.uid,ven_guid,rec_pos,rec_code,device,group_key,rec_type) t1
   
   left join
   
   ( select b.uid, 
       b.ven_guid,
	   b.ven_session,
       b.device_viewtype,
       b.from_rec as recomd_id,
       b.gid,
       a.rec_type,
	   b.page_type,
       a.group_key,
       a.rec_code,
       a.rec_pos
       from (select recomd_id,rec_pos,rec_code,ven_guid,rec_type,group_key,device
		     from gohappy_unima.all_reclog
				where log_mon_i = substring('${hiveconf:DATE}',1,7) and to_date(insert_dt) = '${hiveconf:DATE}'
				and recomd_id !='' and ven_guid !=''
            ) a
			join 
			(select uid,ven_guid,gid,ven_session,page_type,from_rec,device_viewtype,api_logtime 
			 from gohappy_unima.all_weblog
			    where log_mon_i = substring('${hiveconf:DATE}',1,7) and to_date(api_logtime) = '${hiveconf:DATE}'
			    and from_rec is not null
			    and from_rec <> ""
			    and action ='pageload'
			    and device_viewtype !=''
			    and uid <> ""
                and uid <> '""'
            ) b
               
   
    on (a.recomd_id = b.from_rec )
    Where a.rec_pos is not null 
    ) t2
     
on t1.ven_guid = t2.ven_guid 

group by t1.uid,t1.total_rec,t1.rec_pos,t1.rec_code,t1.device,t1.group_key,t1.rec_type

;

--用當天的uid2hr 和當月的rfm_label_table 去計算各個RFM分群的HR
create table rfm_hr_today as 

select uid,if(w1.label="old_user",w1.r,"new_user") r
          ,if(w1.label="old_user",w1.f,"new_user") f
          ,if(w1.label="old_user",w1.m,"new_user") m
		  ,hit_num,view_num
from (

select t1.uid,t2.r,t2.f,t2.m,if(t2.uid is not NULL,"old_user","new_user") label,hit_num,view_num
from joshua_453041.rfm_uid2hr t1 left join joshua_453041.RFM_label_table t2 on t1.uid = t2.uid

) w1
;

--計算rfm各群的數量 和 new_user的數量  ,if(t2.num is NULL,0,t2.num) count
create table RFM_hr_tmp as
    	
    select t1.*,'RFM' as RFM,t2.tthit,t2.ttview,t2.hr
		
    from 
	
		(select * from rfm_r cross join rfm_f cross join rfm_m order by r,f,m) t1 
    
	left join 
	
		(select R,F,M,sum(hit_num) tthit,sum(view_num) ttview,sum(hit_num)/sum(view_num) hr 
		from joshua_453041.RFM_hr_today group by R,F,M order by R,F,M) t2 
    
	on t1.r=t2.r and t1.f=t2.f and t1.m=t2.m
	
	UNION
    
    select R,F,M,'RFM' as RFM,sum(hit_num) tthit,sum(view_num) ttview,sum(hit_num)/sum(view_num) hr 
	from joshua_453041.RFM_hr_today where R="new_user" group by R,F,M
	
	order by r,f,m
    
;

--將那天rfm各個分群的HR存入rfm_123_hr table中
-- 開一個table rfm_123_hr 來insert每天算好的值
create table if not exists rfm_123_hr_record
(
  date_time string,
  R1_F1_M1 double,R1_F1_M2 double,R1_F1_M3 double,
  R1_F2_M1 double,R1_F2_M2 double,R1_F2_M3 double,
  R1_F3_M1 double,R1_F3_M2 double,R1_F3_M3 double,
  R2_F1_M1 double,R2_F1_M2 double,R2_F1_M3 double,
  R2_F2_M1 double,R2_F2_M2 double,R2_F2_M3 double,
  R2_F3_M1 double,R2_F3_M2 double,R2_F3_M3 double,
  R3_F1_M1 double,R3_F1_M2 double,R3_F1_M3 double,
  R3_F2_M1 double,R3_F2_M2 double,R3_F2_M3 double,
  R3_F3_M1 double,R3_F3_M2 double,R3_F3_M3 double,
  new_user double,
  insert_time string);
  
INSERT INTO joshua_453041.rfm_123_hr_record

-- count each group number
select '${hiveconf:DATE}'
    ,collect_list(hr)[0],collect_list(hr)[1],collect_list(hr)[2],collect_list(hr)[3],collect_list(hr)[4]
    ,collect_list(hr)[5],collect_list(hr)[6],collect_list(hr)[7],collect_list(hr)[8],collect_list(hr)[9]
    ,collect_list(hr)[10],collect_list(hr)[11],collect_list(hr)[12],collect_list(hr)[13],collect_list(hr)[14]
	,collect_list(hr)[15],collect_list(hr)[16],collect_list(hr)[17],collect_list(hr)[18],collect_list(hr)[19]
	,collect_list(hr)[20],collect_list(hr)[21],collect_list(hr)[22],collect_list(hr)[23],collect_list(hr)[24]
	,collect_list(hr)[25],collect_list(hr)[26],collect_list(hr)[27]
    ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')
from rfm_hr_tmp
group by rfm
;

--end









