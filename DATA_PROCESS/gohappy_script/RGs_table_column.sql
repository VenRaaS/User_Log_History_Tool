--daily script to create demo table for gohappy--

--User Guide:
--在linux介面輸入
--hive -f daily_script_create_demo_table_anyday.sql -hiveconf YEAR=$(date +"%Y" --date="today") -hiveconf MONTH=$(date +"%m" --date="today") -hiveconf DATE=$(date +"%d" --date="today");
--來創建當天的table
--

--preparing
use joshua_453041;

drop table if exists step_1 ;
drop table if exists step_2 ;
drop table if exists step_3 ;
drop table if exists step_4 ;
drop table if exists step_5 ;
drop table if exists step_6 ;
drop table if exists gohappy_temp_goods ;
drop table if exists gohappy_temp_categ ;

--若要以uid為選擇的依據又不想放棄uid=NULL的weblog勢必會在join on ven_guid的時候讓weblog重複,因此決定用ven_guid來取代uid


--1.提取當天的weblog和rec_log,並且join兩個table + 從1筆資料變成10筆(by split rec_id)--
create table step_1 as select * 
    from (select w1.ven_guid,w1.ven_session,w1.gid,w1.device,w1.categ_code,w1.api_logtime,w1.page_type,w1.rec_id,w1.from_rec
                ,w2.recomd_list as rec_list,w2.recomd_method as rec_method 
            from (select ven_guid,ven_session,gid,device_viewtype as device,categ_code,api_logtime,page_type,get_json_object(now_rec,'$[0].rec') as rec_id,from_rec 
                    from gohappy_unima.all_weblog where api_logtime between unix_timestamp('${hiveconf:YEAR}-${hiveconf:MONTH}-${hiveconf:DATE} 00:00:00.0') and unix_timestamp('${hiveconf:YEAR}-${hiveconf:MONTH}-${hiveconf:DATE} 24:00:00.0') ) as w1 
            left join (select distinct * from gohappy_unima.all_reclog where log_mon_i='${hiveconf:YEAR}-${hiveconf:MONTH}') as w2 on w1.rec_id=w2.recomd_id) as tt 
    LATERAL VIEW OUTER posexplode(split(rec_list,',')) C AS indx,rec_good ;
	
--2-1.先將goods_unima 中的goods_name做字串處理，去掉結尾的"\"，然後創造新的table
create table gohappy_temp_goods as select IF(substr(goods_name,-1) ='\\',substr(goods_name,1,length(goods_name)-1),goods_name) as goods_name,gid,sale_price as goods_price,goods_img_url from gohappy_unima.unima_goods;
create table gohappy_temp_categ as select IF(substr(category_name,-1) ='\\',substr(category_name,1,length(category_name)-1),category_name) as category_name,category_code from gohappy_unima.unima_category;

--2-2.與goods的table join,提取3樣東西,分別是被推薦商品的 (1)商品名稱 (2)商品img_url (3)商品價格
create table step_2 as select t1.*,goods_name,goods_img_url,int(goods_price) as goods_price from joshua_453041.step_1 as t1 left join joshua_453041.gohappy_temp_goods as t2 on t1.rec_good=t2.gid where t1.rec_good is not NULL and t1.rec_good!='' union all select t1.*,'' as goods_name,'' as goods_img_url,'' as price from joshua_453041.step_1 as t1 where t1.rec_good is NULL or t1.rec_good='' order by api_logtime,indx;

--3.將data先用collect_list()從10筆併回一筆 + 再用[]把 img_url 和 goods_name 各分成10個欄位--

--indx這個變數目的是讓collect_list()在收集所有元素的時候順序不會亂掉
--在這一步完成他的任務後,step_3中就可以不要出現了,掰掰

create table step_3 as select ven_guid,ven_session,gid,device,categ_code,api_logtime,page_type,rec_id,from_rec,rec_list
    ,collect_list(goods_img_url)[0] as goods_img_url_1,collect_list(goods_img_url)[1] as goods_img_url_2,collect_list(goods_img_url)[2] as goods_img_url_3
    ,collect_list(goods_img_url)[3] as goods_img_url_4,collect_list(goods_img_url)[4] as goods_img_url_5,collect_list(goods_img_url)[5] as goods_img_url_6
    ,collect_list(goods_img_url)[6] as goods_img_url_7,collect_list(goods_img_url)[7] as goods_img_url_8,collect_list(goods_img_url)[8] as goods_img_url_9
    ,collect_list(goods_img_url)[9] as goods_img_url_10
    ,collect_list(goods_name)[0] as goods_name_1,collect_list(goods_name)[1] as goods_name_2,collect_list(goods_name)[2] as goods_name_3
    ,collect_list(goods_name)[3] as goods_name_4,collect_list(goods_name)[4] as goods_name_5,collect_list(goods_name)[5] as goods_name_6
    ,collect_list(goods_name)[6] as goods_name_7,collect_list(goods_name)[7] as goods_name_8,collect_list(goods_name)[8] as goods_name_9
    ,collect_list(goods_name)[9] as goods_name_10
    ,collect_list(goods_price)[0] as goods_price_1,collect_list(goods_price)[1] as goods_price_2,collect_list(goods_price)[2] as goods_price_3
    ,collect_list(goods_price)[3] as goods_price_4,collect_list(goods_price)[4] as goods_price_5,collect_list(goods_price)[5] as goods_price_6
    ,collect_list(goods_price)[6] as goods_price_7,collect_list(goods_price)[7] as goods_price_8,collect_list(goods_price)[8] as goods_price_9
    ,collect_list(goods_price)[9] as goods_price_10
    ,split(rec_method,',')[0] as rec_method_1,split(rec_method,',')[1] as rec_method_2,split(rec_method,',')[2] as rec_method_3,split(rec_method,',')[3] as rec_method_4
    ,split(rec_method,',')[4] as rec_method_5,split(rec_method,',')[5] as rec_method_6,split(rec_method,',')[6] as rec_method_7,split(rec_method,',')[7] as rec_method_8
    ,split(rec_method,',')[8] as rec_method_9,split(rec_method,',')[9] as rec_method_10

    from joshua_453041.step_2 group by ven_guid,ven_session,gid,device,categ_code,api_logtime,page_type,rec_id,from_rec,rec_list,rec_method order by ven_guid,api_logtime ;

--4.與goods的table join,提取3樣東西,分別是當前頁面的 (1)商品名稱 (2)商品img_url (3)商品價格
--## hint: gid為null和not null的分開算,再用union把他們合起來,而且要在null的table中加一行空白的column--
--##distinct 和 t1.* 不能一起用--

create table step_4 as select t1.*,t2.goods_img_url as current_img_url,t2.goods_name as current_goods_name,int(t2.goods_price) as current_price from joshua_453041.step_3 as t1 join joshua_453041.gohappy_temp_goods as t2 on t1.gid=t2.gid where t1.gid is not NULL and t1.gid!='' union all select t1.*,'' as current_img_url,'' as current_goods_name,'' as current_goods_price from joshua_453041.step_3 as t1 where t1.gid is NULL or t1.gid='' ;

--5.與category的table join,提取 (1)分類頁的名稱 (方法同上)--
create table step_5 as select t1.*,t2.category_name as categ_name from joshua_453041.step_4 as t1 join joshua_453041.gohappy_temp_categ as t2 on t1.categ_code=t2.category_code where t1.categ_code is not NULL and t1.categ_code!='' union all select t1.*,'' as categ_name from joshua_453041.step_4 as t1 where t1.categ_code is NULL or t1.categ_code='';

--6.加上ven_guid的index(因為有些ven_guid有斜線,若放到URL會無法讀取,因此用u_index取代)
create table step_6 as select t1.*,t2.u_index from joshua_453041.step_5 AS t1 JOIN (SELECT ven_guid,row_number() over() as u_index from joshua_453041.step_5 where ven_guid!='' GROUP BY ven_guid) AS t2 ON (t1.ven_guid = t2.ven_guid);

--FINAL:排序成好觀察的樣子--(用${hiveconf: .... }去接上面傳入的變數)
drop table if exists RGs_gohappy_withColumn_${hiveconf:YEAR}${hiveconf:MONTH}${hiveconf:DATE};
create table RGs_gohappy_withColumn_${hiveconf:YEAR}${hiveconf:MONTH}${hiveconf:DATE} as select distinct * from joshua_453041.step_6 order by ven_guid,ven_session,rec_id,api_logtime;

--drop all table which is not needed.--
drop table step_1;
drop table step_2;
drop table step_3;
drop table step_4;
drop table step_5;
drop table step_6;
drop table if exists gohappy_temp_goods ;
drop table if exists gohappy_temp_categ ;
--end

