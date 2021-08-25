-- kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all：全部ID之间的关系表
-- kscdm.dim_ks_device_daily：分产品设备天级设备维表
-- npcdm.dim_pub_device_daily：新业务天级设备维度表
-- kscdm.dim_ks_user_daily：分产品用户天级设备维表
-- npcdm.dim_pub_user_daily：新业务用户维表
-- kskdp.fvs_ffs_ue_device_product_device_global_mapping_1d：分产品DID/GID映射天级表
-- kscdm.dwd_ks_usr_actv_prod_device_base_hi：日活基础表
-- kscdm.dwd_ks_tfc_stat_device_hi：信息采集（设备）事实表
-- npcdm.dwd_pub_tfc_stat_device_hi：新业务流量域设备统计信息明细事实表-小时级
-- kscdm.dim_ks_user_all：分产品用户全量维表
-- npcdm.dim_pub_user_all：新业务全量用户维度表


-- device_id和user_id这两个业务ID和产品强相关，同一设备，不同app，这两个ID可能一样，统计覆盖率时，必须关联上产品类型
-- imei、idfa、oaid以及global_id是设备ID，不同app，同一设备，都是相同的，因此可以不考虑产品

-- DEVICE_ID
with id_map_data as (
select 
    left_product as product, 
    left_value as m_id
from 
    kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all
where 
    p_date = '20210720' 
and 
    left_type = 'DEVICE_ID' 
and 
    left_value is not null 
and 
    right_value is not null
group by 
    left_product, 
    left_value
),

daily_actv_detail as (
select 
     device_id, 
     product,
     is_new_device,
     'ks' as product_tag
from 
    kscdm.dim_ks_device_daily
where 
    p_date = '20210721' 
and 
    device_id is not null 
and 
    is_today_active = 1 
and 
    upper(trim(device_id)) not like 'KWE%' 
and 
    product is not null
and 
    lower(trim(device_id)) not in ('', '0', 'unknown', 'android_unknown', '0000000000000000','-1','null','test_user_id')
group by 
    device_id, 
    is_new_device,
    product
union all
select 
    device_id,
    product,
    is_today_new as is_new_device,
    'np' as product_tag
from 
    npcdm.dim_pub_device_daily
where 
    p_date = '20210721' 
and 
    device_id is not null 
and 
    is_today_active = 1 
and 
    upper(trim(device_id)) not like 'KWE%' 
and 
    product is not null
and 
    lower(trim(device_id)) not in ('', '0', 'unknown', 'android_unknown', '0000000000000000','-1','null','test_user_id')
group by 
    device_id, 
    is_today_new,
    product
),

daily_actv as (
select
    device_id,
    product,
    is_new_device,
    product_tag
from
    daily_actv_detail
group by
    device_id,
    product,
    is_new_device,
    product_tag
),

result as (
select
    d.product_tag,
    d.product,
    d.is_new_device,
    count(1) as daily_did_cnt,
    sum(if(m.m_id is not null,1,0)) as cover_cnt
    -- sum(if(o.m_id is not null,1,0)) as old_cover_cnt
from
    daily_actv d 
left join
    id_map_data m 
on 
    d.device_id = m.m_id
and 
    d.product = m.product 
group by
    d.product_tag,
    d.product,
    d.is_new_device with rollup
)

select
    coalesce(product_tag,'全部') as product_tag,
    coalesce(product,'全部') as product,
    coalesce(is_new_device,'全部') as is_new_device,
    daily_did_cnt,
    cover_cnt,
    round(cover_cnt*100.0/daily_did_cnt,4) as cover_rate
from
    result




-- USER_ID

select
    coalesce(p_tag,'全部') as product_tag,
    coalesce(product,'全部') as product,
    coalesce(is_new_user,'全部') as is_new_user,
    uid_cnt,
    cover_cnt
from
    (
        select 
            product,
            p_tag, 
            is_new_user,
            count(1) as uid_cnt, 
            sum(if(ser.uid is not null,1,0)) as cover_cnt, 
            'uid_dau' as product_type
        from 
        --转换
            (
                select 
                    left_product, 
                    left_value as uid
                from 
                    kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all
                where 
                    p_date = '20210720' 
                and 
                    left_type = 'USER_ID' 
                and 
                    right_value is not null 
                and 
                    left_value is not null 
                and 
                    left_value not in ('', '0', 'unknown')
                group by 
                    left_product, 
                    left_value
            ) as ser
        right join 
            (
                select 
                    user_id, 
                    product,
                    is_today_new as is_new_user,
                    'np' as p_tag
                from 
                    npcdm.dim_pub_user_daily
                where 
                    p_date = '20210721' 
                and 
                    is_today_active = 1 
                and 
                    user_id is not null 
                and 
                    user_id not in ('', '0', 'unknown')
                group by 
                    user_id, 
                    is_today_new,
                    product
            
                union all
            
                select 
                    cast(user_id as string) as user_id, 
                    product,
                    is_reg_user as is_new_user,
                    'ks' as p_tag
                from 
                    kscdm.dim_ks_user_daily
                where 
                    p_date = '20210721' 
                and 
                    user_id > 0 
                and 
                    is_today_active = 1
                group by 
                    user_id, 
                    is_reg_user,
                    product
            ) dau
        on 
            dau.product = ser.left_product 
        and 
            dau.user_id = ser.uid
        group by
            product,
            p_tag,
            is_new_user with rollup
    ) result


----------------------------------------------------------------------------------------------------------------------------
-- 不区分产品


-- GLOBAL_ID
add jar viewfs://hadoop-lt-cluster/home/system/hive/resources/security/infra-unified-device-fingerprint-sdk-1.0.8-SNAPSHOT-jar-with-dependencies.jar;
create temporary function DeGidUDF as 'com.kuaishou.infra.global.device.fingerprint.DeGidUDF';
select
    coalesce(p_tag,'全部') as product_tag,
    coalesce(product,'全部') as product,
    gid_cnt,
    cover_cnt    
from
    (
        select 
            dau.p_tag,
            dau.product,
            count(1) as gid_cnt,
            sum(if( ser.gid is not null,1,0)) as cover_cnt, 
            'gid_dau' as product_type
        from 
        --转换
            (
                select 
                    left_value as gid
                from 
                    kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all
                where 
                    p_date = '20210720' 
                and 
                    left_type = 'GLOBAL_ID' 
                and 
                    left_value is not null 
                and 
                    right_value is not null
                group by 
                    left_value
            ) as ser
        right join 
            (
                select 
                    global_id,product,p_tag
                from
                    (
                        select 
                            global_id,
                            product,
                            'np' as p_tag
                        from 
                            kskdp.fvs_ffs_ue_device_product_device_global_mapping_1d
                        where 
                            p_date = '20210721' 
                        and 
                            product not in ("KUAISHOU","NEBULA","KUAISHOU_LIVE_MATE","THANOS","KUAISHOU_ANTMAN","KUAISHOU_LANDSCAPE","KS_TV","GAME_ZONE","KWAI_LITE")
                        and 
                            global_id is not null 
                        and 
                            global_id > '0000000008f0d180' 
                        and 
                            (length(trim(global_id)) = 16 or length(DeGidUDF(trim(global_id))) = 16)
                        group by 
                            global_id,
                            product

                        union all
                       
                        select
                            global_id,
                            default.product_clean(product,'20210721') product,
                            'ks' as p_tag
                        from 
                            kscdm.dwd_ks_usr_actv_prod_device_base_hi
                        where 
                            p_date='20210721' 
                        and 
                            log_source in ('launch_event','show_event','app_usage_stat_event','real_show','register','login','system_stat')
                        and 
                            global_id is not null and global_id > '0000000008f0d180' and (length(trim(global_id)) = 16 or length(DeGidUDF(trim(global_id)) ) = 16)
                        group by 
                            global_id,
                            default.product_clean(product,'20210721')
                    ) dau
                group by 
                    global_id,
                    product,
                    p_tag
            ) dau
        on 
            dau.global_id = ser.gid
        group by 
            dau.p_tag,
            dau.product with rollup
    ) result
;


        


-- IMEI
select
    coalesce(p_tag,'全部') as product_tag,
    coalesce(product,'全部') as product,
    imei_cnt,
    cover_cnt 
from
    (
        select 
            dau.p_tag,
            dau.product,
            count(dau.imei) as imei_cnt,
            count(case when ser.left_value is not null then left_value end) as cover_cnt,
            'imei_dau' as product_type
        from 
        --转换
            (
                select
                    left_value 
                from 
                    kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all
                where 
                    p_date = '20210720' 
                and 
                    left_type = 'IMEI' 
                and 
                    left_value is not null 
                and 
                    right_value is not null
                group by 
                    left_value
            ) as ser
        right join 
            (
                select 
                    imei,
                    product,
                    p_tag
                from
                    (
                        select 
                            imei,
                            product,
                            'ks' as p_tag
                        from 
                            kscdm.dwd_ks_tfc_stat_device_hi
                        where 
                            p_date = '20210721' 
                        and 
                            imei is not null 
                        and 
                            length(imei) in (15, 14)
                        group by 
                            imei,
                            product
                
                        union all
                        
                        select 
                            imei,
                            product,
                            'np' as p_tag
                        from 
                            npcdm.dwd_pub_tfc_stat_device_hi
                        where 
                            p_date = '20210721' 
                        and 
                            imei is not null 
                        and 
                            length(imei) in (15, 14)
                        group by 
                            imei,
                            product
                    ) as dau
                group by 
                    imei,
                    product,
                    p_tag
            ) dau
        on 
            dau.imei = ser.left_value
        group by
            dau.p_tag,
            dau.product with rollup
    ) result
;




-- IDFA
select
    coalesce(p_tag,'全部') as product_tag,
    coalesce(product,'全部') as product,
    idfa_cnt,
    cover_cnt 
from
        (
        select
            dau.p_tag,
            dau.product,
            count(dau.idfa) as idfa_cnt,
            count(case when ser.left_value is not null then left_value end) as cover_cnt ,
            -- round(count(case when ser.left_value is not null then left_value end)*100.0/count(dau.idfa),4) as rate,
            'idfa_dau' as product_type
        from
        --转换
            (
                select 
                    left_value
                from 
                    kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all
                where 
                    p_date = '20210720' 
                and 
                    left_type = 'IDFA' 
                and 
                    left_value is not null 
                and 
                    right_value is not null
                group by 
                    left_value
            ) as ser
        right join
            (
                select 
                    idfa,
                    product,
                    p_tag
                from
                    (
                        select 
                            idfa,
                            product,
                            'ks' as p_tag
                        from 
                            kscdm.dwd_ks_tfc_stat_device_hi
                        where 
                            p_date = '20210721' 
                        and 
                            idfa is not null 
                        and 
                            idfa  <> '00000000-0000-0000-0000-000000000000' 
                        and 
                            length(idfa) = 36
                        group by 
                            idfa,
                            product
                        union all

                        select 
                            idfa,
                            product,
                            'np' as p_tag
                        from 
                            npcdm.dwd_pub_tfc_stat_device_hi
                        where 
                            p_date = '20210721' 
                        and 
                            idfa is not null 
                        and 
                            idfa  <> '00000000-0000-0000-0000-000000000000' 
                        and 
                            length(idfa) = 36
                        group by 
                            idfa,
                            product
                    ) as dau
                group by 
                    idfa,
                    product,
                    p_tag
            ) dau
        on 
            dau.idfa = ser.left_value
        group by
            dau.p_tag,
            dau.product with rollup
    ) result
;




-- OAID
select
    coalesce(p_tag,'全部') as product_tag,
    coalesce(product,'全部') as product,
    oaid_cnt,
    cover_cnt 
from
    (
        select
            dau.p_tag,
            dau.product,
            count(dau.oaid) as oaid_cnt,
            count(case when ser.left_value is not null then left_value end) as cover_cnt,
            -- round(count(case when ser.left_value is not null then left_value end)*100.0/count(dau.oaid),4) as rate,
            'oaid_dau' as product_type
        from
        --转换
            (
                select 
                    left_value
                from 
                    kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all
                where 
                    p_date = '20210720' 
                and 
                    left_type = 'OAID' 
                and 
                    left_value is not null 
                and 
                    right_value is not null
                group by 
                    left_value,
                    left_product
            ) as ser
        right join
            (
                select 
                    oaid,
                    product,
                    p_tag
                from
                    (
                        select 
                            oaid,
                            product,
                            'ks' as p_tag
                        from 
                            kscdm.dwd_ks_tfc_stat_device_hi
                        where 
                            p_date = '20210721' 
                        and 
                            oaid is not null 
                        and 
                            trim(oaid)<>'' --and length(oaid) in (15, 14)
                        group by 
                            oaid,
                            product

                        union all

                        select 
                            oaid,
                            product,
                            'np' as p_tag
                        from 
                            npcdm.dwd_pub_tfc_stat_device_hi
                        where 
                            p_date = '20210721' 
                        and 
                            oaid is not null 
                        and 
                            trim(oaid)<>'' --and length(oaid) in (15, 14)
                        group by 
                            oaid,
                            product
                    ) as dau
                group by 
                    oaid,
                    product,
                    p_tag
            ) dau
        on 
            dau.oaid = ser.left_value
        group by
            dau.p_tag,
            dau.product with rollup
    ) result
;




-- PHONE
select
    coalesce(p_tag,'全部') as product_tag,
    coalesce(product,'全部') as product,
    phone_cnt,
    cover_cnt 
from
    (
        select 
            p_tag,
            product,
            count(dau.phone) as phone_cnt, 
            count(case when ser.left_value is not null then left_value end) as cover_cnt 
            -- ,round(count(case when ser.left_value is not null then left_value end)*100.0/count(dau.phone)) as rate
        from 
        --转换
            (
                select 
                    left_value 
                from 
                    kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all
                where 
                    p_date = '20210720' 
                and 
                    left_type = 'PHONE' 
                and 
                    left_value is not null 
                and 
                    right_value is not null
                group by 
                    left_value
            ) as ser
        right join 
            (
                select 
                    phone,
                    product,
                    p_tag
                from
                    (
                        select 
                            phone,
                            product,
                            'ks' as p_tag
                        from 
                            kscdm.dim_ks_user_daily
                        where 
                            p_date = '20210721' 
                        and 
                            phone is not null 
                        and
                            is_today_active = 1 
                        group by 
                            phone,
                            product
                        
                        union all
                        
                        select 
                            phone,
                            product,
                            'np' as p_tag
                        from 
                            npcdm.dim_pub_user_daily
                        where 
                            p_date = '20210721' 
                        and 
                            phone is not null
                        and
                            is_today_active = 1
                        group by 
                            phone,
                            product
                    ) dau
                group by
                    phone,
                    product,
                    p_tag
            ) dau
        on 
            dau.phone = ser.left_value
        group by
            p_tag,
            product with rollup
    ) result
;
















------------------------------------------------------------------------------------------------------------------------------------------------------
-- 分产品


-- GLOBAL_ID
add jar viewfs://hadoop-lt-cluster/home/system/hive/resources/security/infra-unified-device-fingerprint-sdk-1.0.8-SNAPSHOT-jar-with-dependencies.jar;
create temporary function DeGidUDF as 'com.kuaishou.infra.global.device.fingerprint.DeGidUDF';
select
    coalesce(p_tag,'全部') as product_tag,
    coalesce(product,'全部') as product,
    gid_cnt,
    cover_cnt    
from
    (
        select 
            dau.p_tag,
            dau.product,
            count(1) as gid_cnt,
            sum(if( ser.gid is not null,1,0)) as cover_cnt, 
            'gid_dau' as product_type
        from 
        --转换
            (
                select 
                    left_product,
                    left_value as gid
                from 
                    kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all
                where 
                    p_date = '20210720' 
                and 
                    left_type = 'GLOBAL_ID' 
                and 
                    left_value is not null 
                and 
                    right_value is not null
                group by 
                    left_value,
                    left_product
            ) as ser
        right join 
            (
                select 
                    global_id,product,p_tag
                from
                    (
                        select 
                            global_id,
                            product,
                            'np' as p_tag
                        from 
                            kskdp.fvs_ffs_ue_device_product_device_global_mapping_1d
                        where 
                            p_date = '20210721' 
                        and 
                            product not in ("KUAISHOU","NEBULA","KUAISHOU_LIVE_MATE","THANOS","KUAISHOU_ANTMAN","KUAISHOU_LANDSCAPE","KS_TV","GAME_ZONE","KWAI_LITE")
                        and 
                            global_id is not null 
                        and 
                            global_id > '0000000008f0d180' 
                        and 
                            (length(trim(global_id)) = 16 or length(DeGidUDF(trim(global_id))) = 16)
                        group by 
                            global_id,
                            product

                        union all
                       
                        select
                            global_id,
                            default.product_clean(product,'20210721') product,
                            'ks' as p_tag
                        from 
                            kscdm.dwd_ks_usr_actv_prod_device_base_hi
                        where 
                            p_date='20210721' 
                        and 
                            log_source in ('launch_event','show_event','app_usage_stat_event','real_show','register','login','system_stat')
                        and 
                            global_id is not null and global_id > '0000000008f0d180' and (length(trim(global_id)) = 16 or length(DeGidUDF(trim(global_id)) ) = 16)
                        group by 
                            global_id,
                            default.product_clean(product,'20210721')
                    ) dau
                group by 
                    global_id,
                    product,
                    p_tag
            ) dau
        on 
            dau.global_id = ser.gid
        and
            dau.product = ser.left_product
        group by 
            dau.product,
            dau.p_tag with rollup
    ) result
;





-- IMEI
select
    coalesce(p_tag,'全部') as product_tag,
    coalesce(product,'全部') as product,
    imei_cnt,
    cover_cnt 
from
    (
        select 
            dau.p_tag,
            dau.product,
            count(dau.imei) as imei_cnt,
            count(case when ser.left_value is not null then left_value end) as cover_cnt,
            'imei_dau' as product_type
        from 
        --转换
            (
                select
                    left_product, 
                    left_value 
                from 
                    kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all
                where 
                    p_date = '20210720' 
                and 
                    left_type = 'IMEI' 
                and 
                    left_value is not null 
                and 
                    right_value is not null
                group by 
                    left_value,
                    left_product
            ) as ser
        right join 
            (
                select 
                    imei,
                    product,
                    p_tag
                from
                    (
                        select 
                            imei,
                            product,
                            'ks' as p_tag
                        from 
                            kscdm.dwd_ks_tfc_stat_device_hi
                        where 
                            p_date = '20210721' 
                        and 
                            imei is not null 
                        and 
                            length(imei) in (15, 14)
                        group by 
                            imei,
                            product
                
                        union all
                        
                        select 
                            imei,
                            product,
                            'np' as p_tag
                        from 
                            npcdm.dwd_pub_tfc_stat_device_hi
                        where 
                            p_date = '20210721' 
                        and 
                            imei is not null 
                        and 
                            length(imei) in (15, 14)
                        group by 
                            imei,
                            product
                    ) as dau
                group by 
                    imei,
                    product,
                    p_tag
            ) dau
        on 
            dau.imei = ser.left_value
        and
            dau.product = ser.left_product
        group by
            dau.product,
            dau.p_tag with rollup
    ) result
;




-- IDFA
select
    coalesce(p_tag,'全部') as product_tag,
    coal
    idfa_cnt,
    cover_cnt 
from
        (
        select
            dau.p_tag,
            dau.product,
            count(dau.idfa) as idfa_cnt,
            count(case when ser.left_value is not null then left_value end) as cover_cnt ,
            -- round(count(case when ser.left_value is not null then left_value end)*100.0/count(dau.idfa),4) as rate,
            'idfa_dau' as product_type
        from
        --转换
            (
                select 
                    left_value,
                    left_product
                from 
                    kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all
                where 
                    p_date = '20210720' 
                and 
                    left_type = 'IDFA' 
                and 
                    left_value is not null 
                and 
                    right_value is not null
                group by 
                    left_value,
                    left_product
            ) as ser
        right join
            (
                select 
                    idfa,
                    product,
                    p_tag
                from
                    (
                        select 
                            idfa,
                            product,
                            'ks' as p_tag
                        from 
                            kscdm.dwd_ks_tfc_stat_device_hi
                        where 
                            p_date = '20210721' 
                        and 
                            idfa is not null 
                        and 
                            idfa  <> '00000000-0000-0000-0000-000000000000' 
                        and 
                            length(idfa) = 36
                        group by 
                            idfa,
                            product
                        union all

                        select 
                            idfa,
                            product,
                            'np' as p_tag
                        from 
                            npcdm.dwd_pub_tfc_stat_device_hi
                        where 
                            p_date = '20210721' 
                        and 
                            idfa is not null 
                        and 
                            idfa  <> '00000000-0000-0000-0000-000000000000' 
                        and 
                            length(idfa) = 36
                        group by 
                            idfa,
                            product
                    ) as dau
                group by 
                    idfa,
                    product,
                    p_tag
            ) dau
        on 
            dau.idfa = ser.left_value
        and
            dau.product = ser.left_product
        group by
            dau.product,
            dau.p_tag with rollup
    ) result
;




-- OAID
select
    coalesce(p_tag,'全部') as product_tag,
    coalesce(product,'全部') as product,
    oaid_cnt,
    cover_cnt 
from
    (
        select
            dau.p_tag,
            dau.product,
            count(dau.oaid) as oaid_cnt,
            count(case when ser.left_value is not null then left_value end) as cover_cnt,
            -- round(count(case when ser.left_value is not null then left_value end)*100.0/count(dau.oaid),4) as rate,
            'oaid_dau' as product_type
        from
        --转换
            (
                select 
                    left_value,
                    left_product
                from 
                    kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all
                where 
                    p_date = '20210720' 
                and 
                    left_type = 'OAID' 
                and 
                    left_value is not null 
                and 
                    right_value is not null
                group by 
                    left_value,
                    left_product
            ) as ser
        right join
            (
                select 
                    oaid,
                    product,
                    p_tag
                from
                    (
                        select 
                            oaid,
                            product,
                            'ks' as p_tag
                        from 
                            kscdm.dwd_ks_tfc_stat_device_hi
                        where 
                            p_date = '20210721' 
                        and 
                            oaid is not null 
                        and 
                            trim(oaid)<>'' --and length(oaid) in (15, 14)
                        group by 
                            oaid,
                            product

                        union all

                        select 
                            oaid,
                            product,
                            'np' as p_tag
                        from 
                            npcdm.dwd_pub_tfc_stat_device_hi
                        where 
                            p_date = '20210721' 
                        and 
                            oaid is not null 
                        and 
                            trim(oaid)<>'' --and length(oaid) in (15, 14)
                        group by 
                            oaid,
                            product
                    ) as dau
                group by 
                    oaid,
                    product,
                    p_tag
            ) dau
        on 
            dau.oaid = ser.left_value
        and
            dau.product = ser.left_product
        group by
            dau.product,
            dau.p_tag with rollup
    ) result
;




-- PHONE
select
    coalesce(p_tag,'全部') as product_tag,
    coalesce(product,'全部') as product,
    phone_cnt,
    cover_cnt 
from
    (
        select 
            p_tag,
            product,
            count(dau.phone) as phone_cnt, 
            count(case when ser.left_value is not null then left_value end) as cover_cnt 
            -- ,round(count(case when ser.left_value is not null then left_value end)*100.0/count(dau.phone)) as rate
        from 
        --转换
            (
                select 
                    left_value,
                    left_product 
                from 
                    kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all
                where 
                    p_date = '20210720' 
                and 
                    left_type = 'PHONE' 
                and 
                    left_value is not null 
                and 
                    right_value is not null
                group by 
                    left_value,
                    left_product
            ) as ser
        right join 
            (
                select 
                    phone,
                    product,
                    p_tag
                from
                    (
                        select 
                            phone,
                            product,
                            'ks' as p_tag
                        from 
                            kscdm.dim_ks_user_daily
                        where 
                            p_date = '20210721' 
                        and 
                            phone is not null 
                        and
                            is_today_active = 1 
                        group by 
                            phone,
                            product
                        
                        union all
                        
                        select 
                            phone,
                            product,
                            'np' as p_tag
                        from 
                            npcdm.dim_pub_user_daily
                        where 
                            p_date = '20210721' 
                        and 
                            phone is not null
                        and
                            is_today_active = 1
                        group by 
                            phone,
                            product
                    ) dau
                group by
                    phone,
                    product,
                    p_tag
            ) dau
        on 
            dau.phone = ser.left_value
        and
            dau.product = ser.left_product
        group by
            p_tag,
            product with rollup
    ) result
;
















