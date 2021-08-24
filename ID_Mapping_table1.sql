




-- set drop_enable_toggle=on;
-- drop table  kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all;

 
-- create table if not exists kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all 
--  (
--      left_product                       string  comment '关系对的左id产品'
--     ,left_value                         string  comment '关系对的左id值'
--     ,right_product                      string  comment '关系对的右id产品'
--     ,right_value                        string  comment '关系对的右id值'
--     ,confidence                         double  comment '关系对的置信度'
--     ,first_active_timestamp             bigint  comment '关系对的首次活跃时间戳'
--     ,last_active_timestamp              bigint  comment '关系对的最新活跃时间戳'
--     --,relation_type                      bigint  comment '关系对的类型，1直接关系，0为gid转换的关系'
 
-- ) comment '全部id之间的关系表,包括从日志提取的直接关系，和以gid转换的间接关系'
-- partitioned by (
--      p_date                             string  comment 'yyyyMMdd'
--      ,left_type                          string  comment '关系对的左id类型'
--      ,right_type                         string  comment '关系对的右id类型'
-- )
-- stored as parquet;

set hive.execution.engine=spark;

set mapreduce.map.memory.mb=6144;
set mapreduce.reduce.memory.mb=6144;

set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode = nonstrict;

set spark.shuffle.io.retryWait=10s;
set spark.shuffle.io.maxRetries=15;

--可以适当增大spark.executor.memory或者减小spark.executor.cores
set spark.driver.memory = 22g; --默认6g
set spark.executor.memory = 22g; --默认12g
set spark.executor.cores = 4; --默认5

--ALTER table kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all drop IF EXISTS PARTITION (p_date ='{{ ds_nodash }}' );


with log_id_pair_without_gid_table as (
    
    select  
        left_product,
        left_value,
        right_product,
        right_value,
        confidence,
        first_active_timestamp,
        last_active_timestamp ,
        left_type,
        right_type
    from
        kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all_mid1 
    where 
        p_date= '{{ ds_nodash }}' 
) ,
gid_pair_table as(

    select 
        product    
        , id_type
        , id_value                
        , global_id                 
        , confidence                
        , first_active_timestamp 
        , last_active_timestamp	
    from 
        kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all_mid2
    where p_date= '{{ ds_nodash }}' and global_id_type =1
)


INSERT OVERWRITE TABLE kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all
PARTITION (p_date='{{ ds_nodash }}',left_type,right_type)

select  
    coalesce(t4.left_product ,          t3.left_product ) as left_product
    ,coalesce(t4.left_value ,            t3.left_value) as left_value
    ,coalesce(t4.right_product,          t3.right_product ) as right_product
    ,coalesce(t4.right_value ,           t3.right_value ) as right_value
    ,coalesce(t4.confidence,             t3.confidence) as confidence
    ,coalesce(t4.first_active_timestamp, t3.first_active_timestamp) as first_active_timestamp
    ,coalesce(t4.last_active_timestamp , t3.last_active_timestamp ) as last_active_timestamp 
    ,coalesce(t4.left_type ,             t3.left_type ) as left_type
    ,coalesce(t4.right_type ,            t3.right_type) as right_type
from
(
    --通过gid转换的关系
    select
        left_product,
        left_value,
        right_product,
        right_value,
        confidence ,
        first_active_timestamp ,
        last_active_timestamp ,
        left_type,
        right_type
    from 
        kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all_mid3
    where p_date = '{{ ds_nodash }}'
)  t3
full join
(
    select  
        left_product,
        left_value,
        right_product,
        right_value,
        confidence,
        first_active_timestamp,
        last_active_timestamp ,
        left_type,
        right_type
    from
        log_id_pair_without_gid_table  
    union all
    select  
        right_product as left_product,
        right_value as  left_value,
        left_product as right_product,
        left_value as right_value,
        confidence,
        first_active_timestamp,
        last_active_timestamp ,
        right_type as left_type,
        left_type as right_type
    from
        log_id_pair_without_gid_table   

) t4
on 
    t3.left_product = t4.left_product and t3.left_value = t4.left_value
    and t3.right_product = t4.right_product and t3.right_value = t4.right_value 
    and t3.left_type = t4.left_type and t3.right_type = t4.right_type

 -- 其他id到gid的关系   
union all 
select 
    product     as left_product,
    id_value    as left_value,  
    "ALL"       as right_product,
    global_id   as right_value,               
    confidence              ,  
    first_active_timestamp ,
    last_active_timestamp,
    id_type     as left_type,
    "GLOBAL_ID" as right_type
from
    gid_pair_table

--gid到其他id的关系
union all
select 

    "ALL"        as left_product,
    global_id    as left_value,  
    product      as right_product,
    id_value     as right_value,               
    confidence              ,  
    first_active_timestamp ,
    last_active_timestamp,
    "GLOBAL_ID"  as left_type,
    id_type      as right_type
from
    gid_pair_table





    