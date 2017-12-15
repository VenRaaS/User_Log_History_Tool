-- daily script to create demo table for gohappy--

-- User Guide:
-- 在linux介面輸入
-- hive -f daily_script_create_demo_table_anyday.sql -hiveconf YEAR=$(date +"%Y" --date="today") -hiveconf MONTH=$(date +"%m" --date="today") -hiveconf DATE=$(date +"%d" --date="today");
-- 來創建當天的table
--

--preparing
use u453360;

drop table if exists step_1 ;
drop table if exists step_2 ;
drop table if exists step_3 ;
drop table if exists step_4 ;
drop table if exists gohappy_temp_goods ;
drop table if exists gohappy_temp_categ ;

--若要以uid為選擇的依據又不想放棄uid=NULL的weblog勢必會在join on ven_guid的時候讓weblog重複,因此決定用ven_guid來取代uid


--1.提取當天的weblog和rec_log,並且join兩個table + 從1筆資料變成10筆(by split rec_id)--
create table step_1 as select * 
    from (select w1.ven_guid,w1.ven_session,w1.gid,w1.device,w1.categ_code,w1.api_logtime,w1.page_type,w1.action,w1.ilist,w1.rec_id,w1.from_rec
                ,w2.group_key,w2.rec_items as rec_json_info 
            from (select ven_guid,ven_session,gid,device_viewtype as device,categ_code,api_logtime,page_type,action,ilist,get_json_object(now_rec,'$[0].rec') as rec_id,from_rec 
                    from gohappy_unima.all_weblog where api_logtime between unix_timestamp('${hiveconf:YEAR}-${hiveconf:MONTH}-${hiveconf:DATE} 00:00:00.0') and unix_timestamp('${hiveconf:YEAR}-${hiveconf:MONTH}-${hiveconf:DATE} 24:00:00.0') ) as w1 
            left join (select distinct * from gohappy_unima.all_reclog where log_mon_i='${hiveconf:YEAR}-${hiveconf:MONTH}') as w2 on w1.rec_id=w2.recomd_id) as tt ;
	
--2-1.先將goods_unima 中的goods_name做字串處理，把其中有"\"的全部去掉，然後創造新的table
create table gohappy_temp_goods as select regexp_replace(goods_name,"\\\\","") as goods_name,gid,sale_price as goods_price,goods_img_url from gohappy_unima.unima_goods;
create table gohappy_temp_categ as select regexp_replace(category_name,"\\\\","") as category_name,category_code from gohappy_unima.unima_category;


--2-2.與goods的table join,提取3樣東西,分別是當前頁面的 (1)商品名稱 (2)商品img_url (3)商品價格
--## hint: gid為null和not null的分開算,再用union把他們合起來,而且要在null的table中加一行空白的column--
create table step_2 as select t1.*,t2.goods_img_url as current_img_url,t2.goods_name as current_goods_name,int(t2.goods_price) as current_price from u453360.step_1 as t1 left join u453360.gohappy_temp_goods as t2 on t1.gid=t2.gid where t1.gid is not NULL and t1.gid!='' union all select t1.*,'' as current_img_url,'' as current_goods_name,NULL as current_goods_price from u453360.step_1 as t1 where t1.gid is NULL or t1.gid='' ;

--3.與category的table join,提取 (1)分類頁的名稱 (方法同上)--
create table step_3 as select t1.*,t2.category_name as categ_name from u453360.step_2 as t1 left join u453360.gohappy_temp_categ as t2 on t1.categ_code=t2.category_code where t1.categ_code is not NULL and t1.categ_code!='' union all select t1.*,'' as categ_name from u453360.step_2 as t1 where t1.categ_code is NULL or t1.categ_code='';

--4.加上ven_guid的index(因為有些ven_guid有斜線,若放到URL會無法讀取,因此用u_index取代)
create table step_4 as select t1.*,t2.u_index from u453360.step_3 AS t1 JOIN (SELECT ven_guid,row_number() over() as u_index from u453360.step_3 where ven_guid!='' GROUP BY ven_guid) AS t2 ON (t1.ven_guid = t2.ven_guid);

--FINAL:排序成好觀察的樣子--(用${hiveconf: .... }去接上面傳入的變數)
drop table if exists RGs_gohappy_${hiveconf:YEAR}${hiveconf:MONTH}${hiveconf:DATE};
create table RGs_gohappy_${hiveconf:YEAR}${hiveconf:MONTH}${hiveconf:DATE} as select distinct * from u453360.step_4 order by ven_guid,ven_session,rec_id,api_logtime;

--drop all table which is not needed.--
drop table step_1;
drop table step_2;
drop table step_3;
drop table step_4;
drop table if exists gohappy_temp_goods ;
drop table if exists gohappy_temp_categ ;
--end

