

-- 快手系用户信息全量表，获取用户电话号码，产品，操作系统并去重
with actv_detail as (
--日活uid
select 
     user_id, 
     product,
     platform,
     p_tag
from
    (
        select 
            phone as user_id,
            product,
            platform,
            'ks' as p_tag
        from 
            kscdm.dim_ks_user_all
        where 
            p_date = '20210621' 
        and 
            phone is not null 
        group by 
            phone,
            product,
            platform
            
        union all
            
            
    ) dau
group by 
    user_id, 
    product,
    platform,
    p_tag
)
,

-- 全部id之间的关系表，获取源ID产品，源ID，目标ID类型
all_pair as (
select 
    left_product, 
    left_value, 
    right_type
from 
    kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all
where 
    p_date = '20210621' 
and 
    left_type = 'PHONE' 
and 
    left_value <> '' 
and 
    right_value is not null
group by å
    left_product, 
    left_value, 
    right_type
)
,


-- 计算每个产品对应的电话号码总量，以及不同操作系统下的电话号码总量
actv_cnt as (
select 
    product,
    p_tag,
    count(1) as actv_cnt,
    sum(if(platform='ANDROID_PHONE',1,0)) as adr_cnt,
    sum(if(platform='IPHONE',1,0)) as iphone_cnt
from
    actv_detail
group by
    product,
    p_tag
having(count(1)>10000)
)
,

-- 计算不同产品及目标ID类型，对应的电话号码数量，以及不同操作系统对应的电话号码数量
map_cc as (
select 
    dau.product as left_product,
    right_type,
    count(distinct user_id) as total_map_cnt,
    count(distinct if(platform='ANDROID_PHONE',user_id,null)) as adr_map_cnt,
    count(distinct if(platform='IPHONE',user_id,null)) as iphone_map_cnt
from 
    actv_detail dau
join
    all_pair ser on  dau.user_id = ser.left_value and dau.product = ser.left_product        
group by 
    dau.product, right_type
)
select
    a.p_tag,
    a.product,
    a.actv_cnt,
    a.adr_cnt,
    a.iphone_cnt,
    m.right_type,
    m.total_map_cnt,
    m.adr_map_cnt,
    m.iphone_map_cnt,
    round(m.total_map_cnt*100.0/a.actv_cnt,5) as actv_rate,
    round(m.adr_map_cnt*100.0/a.adr_cnt,5) as adr_rate,
    round(m.iphone_map_cnt*100.0/a.iphone_cnt,5) as iphone_rate
from
    actv_cnt a
left join
    map_cc m on  a.product = m.left_product 
order by
    a.p_tag,
    a.actv_cnt desc limit 100000






with actv_detail as (
--日活uid
select 
     user_id, product,platform,p_tag
from
    (
    select oaid as user_id,product,platform,'ks' as p_tag
        from kscdm.dwd_ks_tfc_stat_device_hi
        where p_date = '20210621' and oaid''
        group by oaid,product,platform
        
        union all
        
        select oaid as user_id,product,platform,'np' as p_tag
        from npcdm.dwd_pub_tfc_stat_device_hi
        where p_date = '20210621' and oaid''
        group by oaid,product,platform
            ) dau
        group by user_id, product,platform,p_tag
)
,all_pair as (
select left_product, left_value, right_type
        from kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all
        where p_date = '20210621' and left_type = 'OAID' and left_value'' and right_value is not null
        group by left_product, left_value, right_type
)
,actv_cnt as (
select 
    product,
    p_tag,
    count(1) as actv_cnt,
    sum(if(platform='ANDROID_PHONE',1,0)) as adr_cnt,
    sum(if(platform='IPHONE',1,0)) as iphone_cnt
from
    actv_detail
group by
    product,
    p_tag
having(count(1)>10000)
)
,map_cc as (
select 
    dau.product as left_product,
    right_type,
    count(distinct user_id) as total_map_cnt,
    count(distinct if(platform='ANDROID_PHONE',user_id,null)) as adr_map_cnt,
    count(distinct if(platform='IPHONE',user_id,null)) as iphone_map_cnt
from 
    actv_detail dau
join
    all_pair ser on  dau.user_id = ser.left_value        
group by 
    dau.product, right_type
)
select
    a.p_tag,
    a.product,
    a.actv_cnt,
    a.adr_cnt,
    a.iphone_cnt,
    m.right_type,
    m.total_map_cnt,
    m.adr_map_cnt,
    m.iphone_map_cnt,
    round(m.total_map_cnt*100.0/a.actv_cnt,5) as actv_rate,
    round(m.adr_map_cnt*100.0/a.adr_cnt,5) as adr_rate,
    round(m.iphone_map_cnt*100.0/a.iphone_cnt,5) as iphone_rate
from
    actv_cnt a
left join
    map_cc m on  a.product = m.left_product 
order by
    a.p_tag,
    a.actv_cnt desc limit 100000





















with actv_detail as (
--日活uid
select 
     user_id, product,platform,p_tag
from
    (
    select imei as user_id,product,platform,'ks' as p_tag
        from kscdm.dwd_ks_tfc_stat_device_hi
        where p_date = '20210621' and imei''
        group by imei,product,platform
        
        union all
        
        select imei as user_id,product,platform,'np' as p_tag
        from npcdm.dwd_pub_tfc_stat_device_hi
        where p_date = '20210621' and imei''
        group by imei,product,platform
            ) dau
        group by user_id, product,platform,p_tag
)
,all_pair as (
select left_product, left_value, right_type
        from kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all
        where p_date = '20210621' and left_type = 'IMEI' and left_value'' and right_value is not null
        group by left_product, left_value, right_type
)
,actv_cnt as (
select 
    product,
    p_tag,
    count(1) as actv_cnt,
    sum(if(platform='ANDROID_PHONE',1,0)) as adr_cnt,
    sum(if(platform='IPHONE',1,0)) as iphone_cnt
from
    actv_detail
group by
    product,
    p_tag
having(count(1)>10000)
)
,map_cc as (
select 
    dau.product as left_product,
    right_type,
    count(distinct user_id) as total_map_cnt,
    count(distinct if(platform='ANDROID_PHONE',user_id,null)) as adr_map_cnt,
    count(distinct if(platform='IPHONE',user_id,null)) as iphone_map_cnt
from 
    actv_detail dau
join
    all_pair ser on  dau.user_id = ser.left_value        
group by 
    dau.product, right_type
)
select
    a.p_tag,
    a.product,
    a.actv_cnt,
    a.adr_cnt,
    a.iphone_cnt,
    m.right_type,
    m.total_map_cnt,
    m.adr_map_cnt,
    m.iphone_map_cnt,
    round(m.total_map_cnt*100.0/a.actv_cnt,5) as actv_rate,
    round(m.adr_map_cnt*100.0/a.adr_cnt,5) as adr_rate,
    round(m.iphone_map_cnt*100.0/a.iphone_cnt,5) as iphone_rate
from
    actv_cnt a
left join
    map_cc m on  a.product = m.left_product 
order by
    a.p_tag,
    a.actv_cnt desc limit 100000









with actv_detail as (
--日活uid
select 
     user_id, product,platform,p_tag
from
    (
    select idfa as user_id,product,platform,'ks' as p_tag
        from kscdm.dwd_ks_tfc_stat_device_hi
        where p_date = '20210621' and idfa''
        group by idfa,product,platform
        
        union all
        
        select idfa as user_id,product,platform,'np' as p_tag
        from npcdm.dwd_pub_tfc_stat_device_hi
        where p_date = '20210621' and idfa''
        group by idfa,product,platform
            ) dau
        group by user_id, product,platform,p_tag
)
,all_pair as (
select left_product, left_value, right_type
        from kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all
        where p_date = '20210621' and left_type = 'IDFA' and left_value'' and right_value is not null
        group by left_product, left_value, right_type
)
,actv_cnt as (
select 
    product,
    p_tag,
    count(1) as actv_cnt,
    sum(if(platform='ANDROID_PHONE',1,0)) as adr_cnt,
    sum(if(platform='IPHONE',1,0)) as iphone_cnt
from
    actv_detail
group by
    product,
    p_tag
having(count(1)>10000)
)
,map_cc as (
select 
    dau.product as left_product,
    right_type,
    count(distinct user_id) as total_map_cnt,
    count(distinct if(platform='ANDROID_PHONE',user_id,null)) as adr_map_cnt,
    count(distinct if(platform='IPHONE',user_id,null)) as iphone_map_cnt
from 
    actv_detail dau
join
    all_pair ser on  dau.user_id = ser.left_value        
group by 
    dau.product, right_type
)
select
    a.p_tag,
    a.product,
    a.actv_cnt,
    a.adr_cnt,
    a.iphone_cnt,
    m.right_type,
    m.total_map_cnt,
    m.adr_map_cnt,
    m.iphone_map_cnt,
    round(m.total_map_cnt*100.0/a.actv_cnt,5) as actv_rate,
    round(m.adr_map_cnt*100.0/a.adr_cnt,5) as adr_rate,
    round(m.iphone_map_cnt*100.0/a.iphone_cnt,5) as iphone_rate
from
    actv_cnt a
left join
    map_cc m on  a.product = m.left_product 
order by
    a.p_tag,
    a.actv_cnt desc limit 100000










add jar viewfs://hadoop-lt-cluster/home/system/hive/resources/security/infra-unified-device-fingerprint-sdk-1.0.8-SNAPSHOT-jar-with-dependencies.jar;
create temporary function DeGidUDF as 'com.kuaishou.infra.global.device.fingerprint.DeGidUDF';
with actv_detail as (
--日活uid
select 
     user_id, product,platform,p_tag
from
    (
    select 
          get_json_object(extra_json,'$.global_id') as user_id ,func_product as product,platform,'np' as p_tag
    from    
          npcdm.dwd_pub_usr_actv_device_di
    where p_date = '20210621'
        and platform in ('ANDROID_PHONE','IPHONE')
            and get_json_object(extra_json,'$.global_id') ''
            union all
            select
          global_id as user_id
         ,default.product_clean(product,'20210621') product,platform
         ,'ks' as p_tag
     from kscdm.dwd_ks_usr_actv_prod_device_base_hi
     where p_date='20210621' 
     and platform in ('ANDROID_PHONE','IPHONE')
     and log_source in ('launch_event','show_event','app_usage_stat_event','real_show','register','login','system_stat')
     and global_id is not null and global_id > '0000000008f0d180' and (length(trim(global_id)) = 16 or length(DeGidUDF(trim(global_id)) ) = 16)
     group by global_id,default.product_clean(product,'20210621'),platform
            ) dau
        group by user_id, product,platform,p_tag
)
,all_pair as (
select left_product, left_value, right_type
        from kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all
        where p_date = '20210621' and left_type = 'GLOBAL_ID' and left_value'' and right_value is not null
        group by left_product, left_value, right_type
)
,actv_cnt as (
select 
    product,
    p_tag,
    count(1) as actv_cnt,
    sum(if(platform='ANDROID_PHONE',1,0)) as adr_cnt,
    sum(if(platform='IPHONE',1,0)) as iphone_cnt
from
    actv_detail
group by
    product,
    p_tag
having(count(1)>10000)
)
,map_cc as (
select 
    dau.product as left_product,
    right_type,
    count(distinct user_id) as total_map_cnt,
    count(distinct if(platform='ANDROID_PHONE',user_id,null)) as adr_map_cnt,
    count(distinct if(platform='IPHONE',user_id,null)) as iphone_map_cnt
from 
    actv_detail dau
join
    all_pair ser on  dau.user_id = ser.left_value        
group by 
    dau.product, right_type
)
select
    a.p_tag,
    a.product,
    a.actv_cnt,
    a.adr_cnt,
    a.iphone_cnt,
    m.right_type,
    m.total_map_cnt,
    m.adr_map_cnt,
    m.iphone_map_cnt,
    round(m.total_map_cnt*100.0/a.actv_cnt,5) as actv_rate,
    round(m.adr_map_cnt*100.0/a.adr_cnt,5) as adr_rate,
    round(m.iphone_map_cnt*100.0/a.iphone_cnt,5) as iphone_rate
from
    actv_cnt a
left join
    map_cc m on  a.product = m.left_product 
order by
    a.p_tag,
    a.actv_cnt desc limit 100000











with actv_detail as (
--日活uid
select 
     user_id, product,platform,p_tag
from
    (
            select device_id as user_id, product,platform,'np' as p_tag
            from npcdm.dim_pub_device_daily
            where p_date = '20210621' and device_id is not null and is_today_active = 1 and platform in ('ANDROID_PHONE','IPHONE')
            group by device_id, product,platform
            
            union all
            
            select cast(device_id as string) as user_id, product,platform,'ks' as p_tag
            from kscdm.dim_ks_device_daily
            where p_date = '20210621' and device_id  '' and is_today_active = 1 and platform in ('ANDROID_PHONE','IPHONE')
            group by device_id, product,platform
            ) dau
        group by user_id, product,platform,p_tag
)
,all_pair as (
select left_product, left_value, right_type
        from kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all
        where p_date = '20210621' and left_type = 'DEVICE_ID' and left_value'' and right_value is not null
        group by left_product, left_value, right_type
)
,actv_cnt as (
select 
    product,
    p_tag,
    count(1) as actv_cnt,
    sum(if(platform='ANDROID_PHONE',1,0)) as adr_cnt,
    sum(if(platform='IPHONE',1,0)) as iphone_cnt
from
    actv_detail
group by
    product,
    p_tag
having(count(1)>10000)
)
,map_cc as (
select 
    left_product,
    right_type,
    count(distinct user_id) as total_map_cnt,
    count(distinct if(platform='ANDROID_PHONE',user_id,null)) as adr_map_cnt,
    count(distinct if(platform='IPHONE',user_id,null)) as iphone_map_cnt
from 
    actv_detail dau
join
    all_pair ser on dau.product = ser.left_product and dau.user_id = ser.left_value        
group by 
    left_product, right_type
)
select
    a.p_tag,
    a.product,
    a.actv_cnt,
    a.adr_cnt,
    a.iphone_cnt,
    m.right_type,
    m.total_map_cnt,
    m.adr_map_cnt,
    m.iphone_map_cnt,
    round(m.total_map_cnt*100.0/a.actv_cnt,5) as actv_rate,
    round(m.adr_map_cnt*100.0/a.adr_cnt,5) as adr_rate,
    round(m.iphone_map_cnt*100.0/a.iphone_cnt,5) as iphone_rate
from
    actv_cnt a
left join
    map_cc m on  a.product = m.left_product 
order by
    a.p_tag,
    a.actv_cnt desc limit 100000












with actv_detail as (
--日活uid
select 
     user_id, product,platform,p_tag
from
    (
            select user_id, product,platform,'np' as p_tag
            from npcdm.dim_pub_user_daily
            where p_date = '20210621' and user_id is not null and is_today_active = 1 and platform in ('ANDROID_PHONE','IPHONE')
            group by user_id, product,platform
            
            union all
            
            select cast(user_id as string) as user_id, product,platform,'ks' as p_tag
            from kscdm.dim_ks_user_daily
            where p_date = '20210621' and user_id > 0 and is_today_active = 1 and platform in ('ANDROID_PHONE','IPHONE')
            group by user_id, product,platform
            ) dau
        group by user_id, product,platform,p_tag
)
,all_pair as (
select left_product, left_value, right_type
        from kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all
        where p_date = '20210621' and left_type = 'USER_ID' and cast(left_value as bigint) > 0 and right_value is not null
        group by left_product, left_value, right_type
)
,actv_cnt as (
select 
    product,
    p_tag,
    count(1) as actv_cnt,
    sum(if(platform='ANDROID_PHONE',1,0)) as adr_cnt,
    sum(if(platform='IPHONE',1,0)) as iphone_cnt
from
    actv_detail
group by
    product,
    p_tag
having(count(1)>10000)
)
,map_cc as (
select 
    left_product,
    right_type,
    count(distinct user_id) as total_map_cnt,
    count(distinct if(platform='ANDROID_PHONE',user_id,null)) as adr_map_cnt,
    count(distinct if(platform='IPHONE',user_id,null)) as iphone_map_cnt
from 
    actv_detail dau
join
    all_pair ser on dau.product = ser.left_product and dau.user_id = ser.left_value        
group by 
    left_product, right_type
)
select
    a.p_tag,
    a.product,
    a.actv_cnt,
    a.adr_cnt,
    a.iphone_cnt,
    m.right_type,
    m.total_map_cnt,
    m.adr_map_cnt,
    m.iphone_map_cnt,
    round(m.total_map_cnt*100.0/a.actv_cnt,5) as actv_rate,
    round(m.adr_map_cnt*100.0/a.adr_cnt,5) as adr_rate,
    round(m.iphone_map_cnt*100.0/a.iphone_cnt,5) as iphone_rate
from
    actv_cnt a
left join
    map_cc m on  a.product = m.left_product 
order by
    a.p_tag,
    a.actv_cnt desc limit 100000
 
with actv_detail as (
--日活uid
select 
     user_id, product,platform,p_tag
from
    (
    select phone as user_id,product,platform,'ks' as p_tag
        from kscdm.dim_ks_user_all
        where p_date = '20210621' and phone is not null 
        group by phone,product,platform
        
        union all
        
        select phone as user_id,product,platform,'np' as p_tag
        from npcdm.dim_pub_user_all
        where p_date = '20210621' and phone is not null
        group by phone,product,platform
            ) dau
        group by user_id, product,platform,p_tag
)
,all_pair as (
select left_product, left_value, right_type
        from kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all
        where p_date = '20210621' and left_type = 'PHONE' and left_value<>'' and right_value is not null
        group by left_product, left_value, right_type
)
,actv_cnt as (
select 
    product,
    p_tag,
    count(1) as actv_cnt,
    sum(if(platform='ANDROID_PHONE',1,0)) as adr_cnt,
    sum(if(platform='IPHONE',1,0)) as iphone_cnt
from
    actv_detail
group by
    product,
    p_tag
having(count(1)>10000)
)
,map_cc as (
select 
    dau.product as left_product,
    right_type,
    count(distinct user_id) as total_map_cnt,
    count(distinct if(platform='ANDROID_PHONE',user_id,null)) as adr_map_cnt,
    count(distinct if(platform='IPHONE',user_id,null)) as iphone_map_cnt
from 
    actv_detail dau
join
    all_pair ser on  dau.user_id = ser.left_value and dau.product = ser.left_product        
group by 
    dau.product, right_type
)
select
    a.p_tag,
    a.product,
    a.actv_cnt,
    a.adr_cnt,
    a.iphone_cnt,
    m.right_type,
    m.total_map_cnt,
    m.adr_map_cnt,
    m.iphone_map_cnt,
    round(m.total_map_cnt*100.0/a.actv_cnt,5) as actv_rate,
    round(m.adr_map_cnt*100.0/a.adr_cnt,5) as adr_rate,
    round(m.iphone_map_cnt*100.0/a.iphone_cnt,5) as iphone_rate
from
    actv_cnt a
left join
    map_cc m on  a.product = m.left_product 
order by
    a.p_tag,
    a.actv_cnt desc limit 100000






with actv_detail as (
--日活uid
select 
     user_id, product,platform,p_tag
from
    (
    select oaid as user_id,product,platform,'ks' as p_tag
        from kscdm.dwd_ks_tfc_stat_device_hi
        where p_date = '20210621' and oaid<>''
        group by oaid,product,platform
        
        union all
        
        select oaid as user_id,product,platform,'np' as p_tag
        from npcdm.dwd_pub_tfc_stat_device_hi
        where p_date = '20210621' and oaid<>''
        group by oaid,product,platform
            ) dau
        group by user_id, product,platform,p_tag
)
,all_pair as (
select left_product, left_value, right_type
        from kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all
        where p_date = '20210621' and left_type = 'OAID' and left_value<>'' and right_value is not null
        group by left_product, left_value, right_type
)
,actv_cnt as (
select 
    product,
    p_tag,
    count(1) as actv_cnt,
    sum(if(platform='ANDROID_PHONE',1,0)) as adr_cnt,
    sum(if(platform='IPHONE',1,0)) as iphone_cnt
from
    actv_detail
group by
    product,
    p_tag
having(count(1)>10000)
)
,map_cc as (
select 
    dau.product as left_product,
    right_type,
    count(distinct user_id) as total_map_cnt,
    count(distinct if(platform='ANDROID_PHONE',user_id,null)) as adr_map_cnt,
    count(distinct if(platform='IPHONE',user_id,null)) as iphone_map_cnt
from 
    actv_detail dau
join
    all_pair ser on  dau.user_id = ser.left_value        
group by 
    dau.product, right_type
)
select
    a.p_tag,
    a.product,
    a.actv_cnt,
    a.adr_cnt,
    a.iphone_cnt,
    m.right_type,
    m.total_map_cnt,
    m.adr_map_cnt,
    m.iphone_map_cnt,
    round(m.total_map_cnt*100.0/a.actv_cnt,5) as actv_rate,
    round(m.adr_map_cnt*100.0/a.adr_cnt,5) as adr_rate,
    round(m.iphone_map_cnt*100.0/a.iphone_cnt,5) as iphone_rate
from
    actv_cnt a
left join
    map_cc m on  a.product = m.left_product 
order by
    a.p_tag,
    a.actv_cnt desc limit 100000





















with actv_detail as (
--日活uid
select 
     user_id, product,platform,p_tag
from
    (
    select imei as user_id,product,platform,'ks' as p_tag
        from kscdm.dwd_ks_tfc_stat_device_hi
        where p_date = '20210621' and imei<>''
        group by imei,product,platform
        
        union all
        
        select imei as user_id,product,platform,'np' as p_tag
        from npcdm.dwd_pub_tfc_stat_device_hi
        where p_date = '20210621' and imei<>''
        group by imei,product,platform
            ) dau
        group by user_id, product,platform,p_tag
)
,all_pair as (
select left_product, left_value, right_type
        from kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all
        where p_date = '20210621' and left_type = 'IMEI' and left_value<>'' and right_value is not null
        group by left_product, left_value, right_type
)
,actv_cnt as (
select 
    product,
    p_tag,
    count(1) as actv_cnt,
    sum(if(platform='ANDROID_PHONE',1,0)) as adr_cnt,
    sum(if(platform='IPHONE',1,0)) as iphone_cnt
from
    actv_detail
group by
    product,
    p_tag
having(count(1)>10000)
)
,map_cc as (
select 
    dau.product as left_product,
    right_type,
    count(distinct user_id) as total_map_cnt,
    count(distinct if(platform='ANDROID_PHONE',user_id,null)) as adr_map_cnt,
    count(distinct if(platform='IPHONE',user_id,null)) as iphone_map_cnt
from 
    actv_detail dau
join
    all_pair ser on  dau.user_id = ser.left_value        
group by 
    dau.product, right_type
)
select
    a.p_tag,
    a.product,
    a.actv_cnt,
    a.adr_cnt,
    a.iphone_cnt,
    m.right_type,
    m.total_map_cnt,
    m.adr_map_cnt,
    m.iphone_map_cnt,
    round(m.total_map_cnt*100.0/a.actv_cnt,5) as actv_rate,
    round(m.adr_map_cnt*100.0/a.adr_cnt,5) as adr_rate,
    round(m.iphone_map_cnt*100.0/a.iphone_cnt,5) as iphone_rate
from
    actv_cnt a
left join
    map_cc m on  a.product = m.left_product 
order by
    a.p_tag,
    a.actv_cnt desc limit 100000









with actv_detail as (
--日活uid
select 
     user_id, product,platform,p_tag
from
    (
    select idfa as user_id,product,platform,'ks' as p_tag
        from kscdm.dwd_ks_tfc_stat_device_hi
        where p_date = '20210621' and idfa<>''
        group by idfa,product,platform
        
        union all
        
        select idfa as user_id,product,platform,'np' as p_tag
        from npcdm.dwd_pub_tfc_stat_device_hi
        where p_date = '20210621' and idfa<>''
        group by idfa,product,platform
            ) dau
        group by user_id, product,platform,p_tag
)
,all_pair as (
select left_product, left_value, right_type
        from kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all
        where p_date = '20210621' and left_type = 'IDFA' and left_value<>'' and right_value is not null
        group by left_product, left_value, right_type
)
,actv_cnt as (
select 
    product,
    p_tag,
    count(1) as actv_cnt,
    sum(if(platform='ANDROID_PHONE',1,0)) as adr_cnt,
    sum(if(platform='IPHONE',1,0)) as iphone_cnt
from
    actv_detail
group by
    product,
    p_tag
having(count(1)>10000)
)
,map_cc as (
select 
    dau.product as left_product,
    right_type,
    count(distinct user_id) as total_map_cnt,
    count(distinct if(platform='ANDROID_PHONE',user_id,null)) as adr_map_cnt,
    count(distinct if(platform='IPHONE',user_id,null)) as iphone_map_cnt
from 
    actv_detail dau
join
    all_pair ser on  dau.user_id = ser.left_value        
group by 
    dau.product, right_type
)
select
    a.p_tag,
    a.product,
    a.actv_cnt,
    a.adr_cnt,
    a.iphone_cnt,
    m.right_type,
    m.total_map_cnt,
    m.adr_map_cnt,
    m.iphone_map_cnt,
    round(m.total_map_cnt*100.0/a.actv_cnt,5) as actv_rate,
    round(m.adr_map_cnt*100.0/a.adr_cnt,5) as adr_rate,
    round(m.iphone_map_cnt*100.0/a.iphone_cnt,5) as iphone_rate
from
    actv_cnt a
left join
    map_cc m on  a.product = m.left_product 
order by
    a.p_tag,
    a.actv_cnt desc limit 100000










add jar viewfs://hadoop-lt-cluster/home/system/hive/resources/security/infra-unified-device-fingerprint-sdk-1.0.8-SNAPSHOT-jar-with-dependencies.jar;
create temporary function DeGidUDF as 'com.kuaishou.infra.global.device.fingerprint.DeGidUDF';
with actv_detail as (
--日活uid
select 
     user_id, product,platform,p_tag
from
    (
    select 
          get_json_object(extra_json,'$.global_id') as user_id ,func_product as product,platform,'np' as p_tag
    from    
          npcdm.dwd_pub_usr_actv_device_di
    where p_date = '20210621'
        and platform in ('ANDROID_PHONE','IPHONE')
            and get_json_object(extra_json,'$.global_id') <>''
            union all
            select
          global_id as user_id
         ,default.product_clean(product,'20210621') product,platform
         ,'ks' as p_tag
     from kscdm.dwd_ks_usr_actv_prod_device_base_hi
     where p_date='20210621' 
     and platform in ('ANDROID_PHONE','IPHONE')
     and log_source in ('launch_event','show_event','app_usage_stat_event','real_show','register','login','system_stat')
     and global_id is not null and global_id > '0000000008f0d180' and (length(trim(global_id)) = 16 or length(DeGidUDF(trim(global_id)) ) = 16)
     group by global_id,default.product_clean(product,'20210621'),platform
            ) dau
        group by user_id, product,platform,p_tag
)
,all_pair as (
select left_product, left_value, right_type
        from kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all
        where p_date = '20210621' and left_type = 'GLOBAL_ID' and left_value<>'' and right_value is not null
        group by left_product, left_value, right_type
)
,actv_cnt as (
select 
    product,
    p_tag,
    count(1) as actv_cnt,
    sum(if(platform='ANDROID_PHONE',1,0)) as adr_cnt,
    sum(if(platform='IPHONE',1,0)) as iphone_cnt
from
    actv_detail
group by
    product,
    p_tag
having(count(1)>10000)
)
,map_cc as (
select 
    dau.product as left_product,
    right_type,
    count(distinct user_id) as total_map_cnt,
    count(distinct if(platform='ANDROID_PHONE',user_id,null)) as adr_map_cnt,
    count(distinct if(platform='IPHONE',user_id,null)) as iphone_map_cnt
from 
    actv_detail dau
join
    all_pair ser on  dau.user_id = ser.left_value        
group by 
    dau.product, right_type
)
select
    a.p_tag,
    a.product,
    a.actv_cnt,
    a.adr_cnt,
    a.iphone_cnt,
    m.right_type,
    m.total_map_cnt,
    m.adr_map_cnt,
    m.iphone_map_cnt,
    round(m.total_map_cnt*100.0/a.actv_cnt,5) as actv_rate,
    round(m.adr_map_cnt*100.0/a.adr_cnt,5) as adr_rate,
    round(m.iphone_map_cnt*100.0/a.iphone_cnt,5) as iphone_rate
from
    actv_cnt a
left join
    map_cc m on  a.product = m.left_product 
order by
    a.p_tag,
    a.actv_cnt desc limit 100000











with actv_detail as (
--日活uid
select 
     user_id, product,platform,p_tag
from
    (
            select device_id as user_id, product,platform,'np' as p_tag
            from npcdm.dim_pub_device_daily
            where p_date = '20210621' and device_id is not null and is_today_active = 1 and platform in ('ANDROID_PHONE','IPHONE')
            group by device_id, product,platform
            
            union all
            
            select cast(device_id as string) as user_id, product,platform,'ks' as p_tag
            from kscdm.dim_ks_device_daily
            where p_date = '20210621' and device_id <> '' and is_today_active = 1 and platform in ('ANDROID_PHONE','IPHONE')
            group by device_id, product,platform
            ) dau
        group by user_id, product,platform,p_tag
)
,all_pair as (
select left_product, left_value, right_type
        from kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all
        where p_date = '20210621' and left_type = 'DEVICE_ID' and left_value<>'' and right_value is not null
        group by left_product, left_value, right_type
)
,actv_cnt as (
select 
    product,
    p_tag,
    count(1) as actv_cnt,
    sum(if(platform='ANDROID_PHONE',1,0)) as adr_cnt,
    sum(if(platform='IPHONE',1,0)) as iphone_cnt
from
    actv_detail
group by
    product,
    p_tag
having(count(1)>10000)
)
,map_cc as (
select 
    left_product,
    right_type,
    count(distinct user_id) as total_map_cnt,
    count(distinct if(platform='ANDROID_PHONE',user_id,null)) as adr_map_cnt,
    count(distinct if(platform='IPHONE',user_id,null)) as iphone_map_cnt
from 
    actv_detail dau
join
    all_pair ser on dau.product = ser.left_product and dau.user_id = ser.left_value        
group by 
    left_product, right_type
)
select
    a.p_tag,
    a.product,
    a.actv_cnt,
    a.adr_cnt,
    a.iphone_cnt,
    m.right_type,
    m.total_map_cnt,
    m.adr_map_cnt,
    m.iphone_map_cnt,
    round(m.total_map_cnt*100.0/a.actv_cnt,5) as actv_rate,
    round(m.adr_map_cnt*100.0/a.adr_cnt,5) as adr_rate,
    round(m.iphone_map_cnt*100.0/a.iphone_cnt,5) as iphone_rate
from
    actv_cnt a
left join
    map_cc m on  a.product = m.left_product 
order by
    a.p_tag,
    a.actv_cnt desc limit 100000












with actv_detail as (
--日活uid
select 
     user_id, product,platform,p_tag
from
    (
            select user_id, product,platform,'np' as p_tag
            from npcdm.dim_pub_user_daily
            where p_date = '20210621' and user_id is not null and is_today_active = 1 and platform in ('ANDROID_PHONE','IPHONE')
            group by user_id, product,platform
            
            union all
            
            select cast(user_id as string) as user_id, product,platform,'ks' as p_tag
            from kscdm.dim_ks_user_daily
            where p_date = '20210621' and user_id > 0 and is_today_active = 1 and platform in ('ANDROID_PHONE','IPHONE')
            group by user_id, product,platform
            ) dau
        group by user_id, product,platform,p_tag
)
,all_pair as (
select left_product, left_value, right_type
        from kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all
        where p_date = '20210621' and left_type = 'USER_ID' and cast(left_value as bigint) > 0 and right_value is not null
        group by left_product, left_value, right_type
)
,actv_cnt as (
select 
    product,
    p_tag,
    count(1) as actv_cnt,
    sum(if(platform='ANDROID_PHONE',1,0)) as adr_cnt,
    sum(if(platform='IPHONE',1,0)) as iphone_cnt
from
    actv_detail
group by
    product,
    p_tag
having(count(1)>10000)
)
,map_cc as (
select 
    left_product,
    right_type,
    count(distinct user_id) as total_map_cnt,
    count(distinct if(platform='ANDROID_PHONE',user_id,null)) as adr_map_cnt,
    count(distinct if(platform='IPHONE',user_id,null)) as iphone_map_cnt
from 
    actv_detail dau
join
    all_pair ser on dau.product = ser.left_product and dau.user_id = ser.left_value        
group by 
    left_product, right_type
)
select
    a.p_tag,
    a.product,
    a.actv_cnt,
    a.adr_cnt,
    a.iphone_cnt,
    m.right_type,
    m.total_map_cnt,
    m.adr_map_cnt,
    m.iphone_map_cnt,
    round(m.total_map_cnt*100.0/a.actv_cnt,5) as actv_rate,
    round(m.adr_map_cnt*100.0/a.adr_cnt,5) as adr_rate,
    round(m.iphone_map_cnt*100.0/a.iphone_cnt,5) as iphone_rate
from
    actv_cnt a
left join
    map_cc m on  a.product = m.left_product 
order by
    a.p_tag,
    a.actv_cnt desc limit 100000


