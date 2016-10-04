--新版 momo RGs_table 查詢script

--daily script to create Rolling Goods table for momo--

--User Guide:
--在linux介面輸入
--hive -f daily_script_create_demo_table_anyday.sql -hiveconf YEAR=$(date +"%Y" --date="today") -hiveconf MONTH=$(date +"%m" --date="today") -hiveconf DATE=$(date +"%d" --date="today");
--來創建當天的table
--

--preparing
use joshua_453041;   --選擇table要放置的database

drop table if exists pre_1;
drop table if exists pre_2_weblog;
drop table if exists pre_2_reclog;
drop table if exists pre_3_1;
drop table if exists pre_3_2;
drop table if exists pre_3_3;
drop table if exists pre_3_4;
drop table if exists pre_4_1;
drop table if exists pre_4_2;
drop table if exists pre_4_3;
drop table if exists pre_4_4;
drop table if exists pre_5;
drop table if exists step_1 ;
drop table if exists step_2 ;
drop table if exists step_3 ;
drop table if exists step_4 ;
drop table if exists step_5 ;
drop table if exists step_6 ;
drop table if exists momo_temp_goods ;
drop table if exists momo_temp_categ ;

--*******************************************************************
--***************	    part-1 重建 now_rec 	    *****************
--*******************************************************************

--準備+切割資料:
--reclog的時間資料中有個'T',記得用substr()先把它去掉成為新的momo_reclog

--當月的:

--reclog
create table if not exists momo_reclog_${hiveconf:YEAR}${hiveconf:MONTH} as 
   select unix_timestamp(regexp_replace(log_datetime,"T"," ")) as timestmp,regexp_replace(log_datetime,"T"," ") as time,* from joshua_453041.momo_reclog 
     where month(regexp_replace(log_datetime,"T"," "))=int('${hiveconf:MONTH}') and cc_session!='' and cc_session is not null;
--weblog
create table if not exists momo_weblog_${hiveconf:YEAR}${hiveconf:MONTH} as 
  select * from momo_unima_20160623.all_weblog 
    where log_mon_i='${hiveconf:YEAR}-${hiveconf:MONTH}' and cc_session!='' and cc_session is not null;

--當天的:
--1.切reclog
create table if not exists momo_reclog_${hiveconf:YEAR}${hiveconf:MONTH}${hiveconf:DATE} as 
   select * from joshua_453041.momo_reclog_${hiveconf:YEAR}${hiveconf:MONTH} 
     where day(time)=int('${hiveconf:DATE}');
--2.切weblog
create table if not exists momo_weblog_${hiveconf:YEAR}${hiveconf:MONTH}${hiveconf:DATE} as 
  select * from joshua_453041.momo_weblog_${hiveconf:YEAR}${hiveconf:MONTH}
    where day(api_logtime)=int('${hiveconf:DATE}');
	
--#################################################################
--####     pre_1 : 將reclog與recomder table join 
--#################################################################
create table pre_1 as select t1.*,t2.traffic_type,t2.page_type as recomder_pgty,t2.group_key
    from joshua_453041.momo_reclog_${hiveconf:YEAR}${hiveconf:MONTH}${hiveconf:DATE} as t1 join momo_tmp.recomdlog_recomder as t2 
    on t1.recomder_id=t2.id;
	
--#################################################################################
--####     pre_2 : 依session和page_type為單位將weblog和reclog的序列做出編號
--####             成為 pre_2
--#################################################################################

create table pre_2_weblog as select *,RANK() OVER(DISTRIBUTE BY cc_session,page_type SORT BY api_logtime ASC) as rnk from joshua_453041.momo_weblog_${hiveconf:YEAR}${hiveconf:MONTH}${hiveconf:DATE};
create table pre_2_reclog as select *,RANK() OVER(DISTRIBUTE BY cc_session,recomder_pgty SORT BY time ASC) as rnk from pre_1;

--################################################################
--####    pre_3 : 將weblog分成四群:
--####      (1)page_type='p'
--####      (2)page_type='gop'
--####      (3)page_type='cap'
--####		(4)其他(edm 等.....)
--################################################################
create table pre_3_1 as select * from joshua_453041.pre_2_weblog where page_type='p';
create table pre_3_2 as select * from joshua_453041.pre_2_weblog where page_type='gop';
create table pre_3_3 as select * from joshua_453041.pre_2_weblog where page_type='cap';
create table pre_3_4 as select * from joshua_453041.pre_2_weblog where page_type!='gop' and page_type!='cap' and page_type!='p';

--##############################################################################################################
--####  pre_4 : 將reclog分成四群:
--####      #1# recomder_pgty='Main'     (此為首頁的商品推薦的reclog)(由此來去除樓層排序的reclog)
--####      #2# recomder_pgty='Goods'    (商品頁:別人也買過)
--####      #3#	recomder_pgty='Goods_CS' (商品頁:歷程推薦)
--####		#4# recomder_pgty='Category' (分類頁)
--####
--##############################################################################################################
create table pre_4_1 as select * from joshua_453041.pre_2_reclog where recomder_pgty='Main';
create table pre_4_2 as select * from joshua_453041.pre_2_reclog where recomder_pgty='Goods';
create table pre_4_3 as select * from joshua_453041.pre_2_reclog where recomder_pgty='Goods_CS';
create table pre_4_4 as select * from joshua_453041.pre_2_reclog where recomder_pgty='Category';
--##############################################################################################
--####   pre_5 : 執行四個查詢並將結果用union合併
--####
--####    {1} 第(1)群的weblog & #1#的reclog 去做join
--####    {2} 第(2)群的weblog & #2#的reclog 去做join
--####    {3} 第(2)群的weblog & #3#的reclog 去做join
--####    {4} 第(3)群的weblog & #4#的reclog 去做join
--####    {5} 第(4)群的weblog + 空欄位
--####    
--####     以上join皆是 on {session,gid,categ_code,rnk}這四個欄位相同
--####     唯獨第一個join例外,不用categ_code,因為首頁的categ_code在weblog會記,但是在reclog不會記
--####    
--##############################################################################################
create table pre_5 as 
        
    select t1.*,t2.time,t2.recomd_list,t2.method,t2.recomder_id,t2.traffic_type,t2.recomder_pgty,t2.group_key
        from joshua_453041.pre_3_1 as t1 left join joshua_453041.pre_4_1 as t2 
        on t1.gid=t2.gid and t1.cc_session=t2.cc_session and t1.rnk=t2.rnk 

    UNION ALL
    
    select t1.*,t2.time,t2.recomd_list,t2.method,t2.recomder_id,t2.traffic_type,t2.recomder_pgty,t2.group_key
        from joshua_453041.pre_3_2 as t1 left join joshua_453041.pre_4_2 as t2 
        on t1.gid=t2.gid and t1.categ_code=t2.categ_code and t1.cc_session=t2.cc_session and t1.rnk=t2.rnk 

    UNION ALL

    select t1.*,t2.time,t2.recomd_list,t2.method,t2.recomder_id,t2.traffic_type,t2.recomder_pgty,t2.group_key
        from joshua_453041.pre_3_2 as t1 left join joshua_453041.pre_4_3 as t2 
        on t1.gid=t2.gid and t1.categ_code=t2.categ_code and t1.cc_session=t2.cc_session and t1.rnk=t2.rnk 

    UNION ALL
    
    select t1.*,t2.time,t2.recomd_list,t2.method,t2.recomder_id,t2.traffic_type,t2.recomder_pgty,t2.group_key
        from joshua_453041.pre_3_3 as t1 left join joshua_453041.pre_4_4 as t2 
        on t1.gid=t2.gid and t1.categ_code=t2.categ_code and t1.cc_session=t2.cc_session and t1.rnk=t2.rnk 

    UNION ALL

    select t1.*,'' time,'' recomd_list,'' method,'' recomder_id,'' traffic_type,'' recomder_pgty,'' group_key from joshua_453041.pre_3_4 as t1 ;

--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  
--
--                     接下來執行和 gohappy_unima版相同的步驟
--
--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 

--1.提取當天的weblog和rec_log,並且join兩個table + 從1筆資料變成10幾筆(by split rec_id)--
create table step_1 as select * from pre_5 as tt LATERAL VIEW OUTER posexplode(split(recomd_list,',')) C AS indx,rec_good ;

--2-1.先將goods_unima 中的goods_name做字串處理，去掉結尾的"\"，然後創造新的table
create table momo_temp_goods as select IF(substr(goods_name,-1) ='\\',substr(goods_name,1,length(goods_name)-1),goods_name) as goods_name,gid,sale_price as goods_price,goods_img_url from momo_unima_20160623.unima_goods;
create table momo_temp_categ as select IF(substr(category_name,-1) ='\\',substr(category_name,1,length(category_name)-1),category_name) as category_name,category_code from momo_unima_20160623.unima_category;

--2-2.與goods的table join,提取3樣東西,分別是被推薦商品的 (1)商品名稱 (2)商品img_url (3)商品價格
create table step_2 as select t1.*,goods_name,goods_img_url,int(goods_price) as goods_price from joshua_453041.step_1 as t1 left join joshua_453041.momo_temp_goods as t2 on t1.rec_good=t2.gid where t1.rec_good is not NULL and t1.rec_good!='' union all select t1.*,'' as goods_name,'' as goods_img_url,'' as goods_price from joshua_453041.step_1 as t1 where t1.rec_good is NULL or t1.rec_good='' order by cc_session,api_logtime,indx;

--3.將data先用collect_list()從10多筆併回一筆 + 再用[]把 img_url 和 goods_name 各分成10多個欄位--
--   因為每一種頁面的推薦商品數量不同,因此用最長的(首頁推薦==18~20個)來當作清單長度

--indx這個變數目的是讓collect_list()在收集所有元素的時候順序不會亂掉
--在這一步完成他的任務後,step_3中就可以不要出現了,掰掰

create table step_3 as select cc_guid,cc_session,api_logtime,categ_code,gid,page_type,from_rec,browser,device_viewtype as device
    ,recomd_list as rec_list,method as rec_method,traffic_type,recomder_pgty,group_key
    ,collect_list(goods_img_url)[0] as goods_img_url_1,collect_list(goods_img_url)[1] as goods_img_url_2,collect_list(goods_img_url)[2] as goods_img_url_3
    ,collect_list(goods_img_url)[3] as goods_img_url_4,collect_list(goods_img_url)[4] as goods_img_url_5,collect_list(goods_img_url)[5] as goods_img_url_6
    ,collect_list(goods_img_url)[6] as goods_img_url_7,collect_list(goods_img_url)[7] as goods_img_url_8,collect_list(goods_img_url)[8] as goods_img_url_9
    ,collect_list(goods_img_url)[9] as goods_img_url_10,collect_list(goods_img_url)[10] as goods_img_url_11,collect_list(goods_img_url)[11] as goods_img_url_12
    ,collect_list(goods_img_url)[12] as goods_img_url_13,collect_list(goods_img_url)[13] as goods_img_url_14,collect_list(goods_img_url)[14] as goods_img_url_15
    ,collect_list(goods_img_url)[15] as goods_img_url_16,collect_list(goods_img_url)[16] as goods_img_url_17,collect_list(goods_img_url)[17] as goods_img_url_18
    ,collect_list(goods_img_url)[18] as goods_img_url_19,collect_list(goods_img_url)[19] as goods_img_url_20

    ,collect_list(goods_name)[0] as goods_name_1,collect_list(goods_name)[1] as goods_name_2,collect_list(goods_name)[2] as goods_name_3
    ,collect_list(goods_name)[3] as goods_name_4,collect_list(goods_name)[4] as goods_name_5,collect_list(goods_name)[5] as goods_name_6
    ,collect_list(goods_name)[6] as goods_name_7,collect_list(goods_name)[7] as goods_name_8,collect_list(goods_name)[8] as goods_name_9
    ,collect_list(goods_name)[9] as goods_name_10,collect_list(goods_name)[10] as goods_name_11,collect_list(goods_name)[11] as goods_name_12
    ,collect_list(goods_name)[12] as goods_name_13,collect_list(goods_name)[13] as goods_name_14,collect_list(goods_name)[14] as goods_name_15
    ,collect_list(goods_name)[15] as goods_name_16,collect_list(goods_name)[16] as goods_name_17,collect_list(goods_name)[17] as goods_name_18
    ,collect_list(goods_name)[18] as goods_name_19,collect_list(goods_name)[19] as goods_name_20

    ,collect_list(goods_price)[0] as goods_price_1,collect_list(goods_price)[1] as goods_price_2,collect_list(goods_price)[2] as goods_price_3
    ,collect_list(goods_price)[3] as goods_price_4,collect_list(goods_price)[4] as goods_price_5,collect_list(goods_price)[5] as goods_price_6
    ,collect_list(goods_price)[6] as goods_price_7,collect_list(goods_price)[7] as goods_price_8,collect_list(goods_price)[8] as goods_price_9
    ,collect_list(goods_price)[9] as goods_price_10,collect_list(goods_price)[10] as goods_price_11,collect_list(goods_price)[11] as goods_price_12
    ,collect_list(goods_price)[12] as goods_price_13,collect_list(goods_price)[13] as goods_price_14,collect_list(goods_price)[14] as goods_price_15
    ,collect_list(goods_price)[15] as goods_price_16,collect_list(goods_price)[16] as goods_price_17,collect_list(goods_price)[17] as goods_price_18
    ,collect_list(goods_price)[18] as goods_price_19,collect_list(goods_price)[19] as goods_price_20
   
    ,split(method,',')[0] as rec_method_1,split(method,',')[1] as rec_method_2,split(method,',')[2] as rec_method_3,split(method,',')[3] as rec_method_4
    ,split(method,',')[4] as rec_method_5,split(method,',')[5] as rec_method_6,split(method,',')[6] as rec_method_7,split(method,',')[7] as rec_method_8
    ,split(method,',')[8] as rec_method_9,split(method,',')[9] as rec_method_10,split(method,',')[10] as rec_method_11,split(method,',')[11] as rec_method_12
    ,split(method,',')[12] as rec_method_13,split(method,',')[13] as rec_method_14,split(method,',')[14] as rec_method_15,split(method,',')[15] as rec_method_16
    ,split(method,',')[16] as rec_method_17,split(method,',')[17] as rec_method_18,split(method,',')[18] as rec_method_19,split(method,',')[19] as rec_method_20

    from joshua_453041.step_2 group by cc_guid,cc_session,gid,device_viewtype,categ_code,api_logtime,page_type,from_rec,recomd_list,method,browser,traffic_type,recomder_pgty,group_key order by cc_guid,api_logtime ;

--4.與goods的table join,提取3樣東西,分別是當前頁面的 (1)商品名稱 (2)商品img_url (3)商品價格
--## hint: gid為null和not null的分開算,再用union把他們合起來,而且要在null的table中加一行空白的column--
--##distinct 和 t1.* 不能一起用--

create table step_4 as select t1.*,t2.goods_img_url as current_img_url,t2.goods_name as current_goods_name,int(t2.goods_price) as current_price from joshua_453041.step_3 as t1 join joshua_453041.momo_temp_goods as t2 on t1.gid=t2.gid where t1.gid is not NULL and t1.gid!='' union all select t1.*,'' as current_img_url,'' as current_goods_name,'' as current_goods_price from joshua_453041.step_3 as t1 where t1.gid is NULL or t1.gid='' ;

--5.與category的table join,提取 (1)分類頁的名稱 (方法同上)--
create table step_5 as select t1.*,t2.category_name as categ_name from joshua_453041.step_4 as t1 join joshua_453041.momo_temp_categ as t2 on t1.categ_code=t2.category_code where t1.categ_code is not NULL and t1.categ_code!='' union all select t1.*,'' as categ_name from joshua_453041.step_4 as t1 where t1.categ_code is NULL or t1.categ_code='';

--6.加上ven_guid的index(因為有些ven_guid有斜線,若放到URL會無法讀取,因此用u_index取代)
create table step_6 as select t1.*,t2.u_index from joshua_453041.step_5 AS t1 JOIN (SELECT cc_guid,row_number() over() as u_index from joshua_453041.step_5 where cc_guid!='' GROUP BY cc_guid) AS t2 ON (t1.cc_guid = t2.cc_guid);

--FINAL:排序成好觀察的樣子--(用${hiveconf: .... }去接上面傳入的變數)
drop table if exists RGs_momo_${hiveconf:YEAR}${hiveconf:MONTH}${hiveconf:DATE};
create table RGs_momo_${hiveconf:YEAR}${hiveconf:MONTH}${hiveconf:DATE} as select distinct * from joshua_453041.step_6 order by cc_guid,cc_session,api_logtime;

--drop all table which is not needed.--
drop table if exists pre_1;
drop table if exists pre_2_weblog;
drop table if exists pre_2_reclog;
drop table if exists pre_3_1;
drop table if exists pre_3_2;
drop table if exists pre_3_3;
drop table if exists pre_3_4;
drop table if exists pre_4_1;
drop table if exists pre_4_2;
drop table if exists pre_4_3;
drop table if exists pre_4_4;
drop table if exists pre_5;
drop table if exists step_1 ;
drop table if exists step_2 ;
drop table if exists step_3 ;
drop table if exists step_4 ;
drop table if exists step_5 ;
drop table if exists step_6 ;
drop table if exists momo_temp_goods ;
drop table if exists momo_temp_categ ;
--drop table if exists momo_weblog_${hiveconf:YEAR}${hiveconf:MONTH}${hiveconf:DATE};
--drop table if exists momo_reclog_${hiveconf:YEAR}${hiveconf:MONTH}${hiveconf:DATE};

--end

