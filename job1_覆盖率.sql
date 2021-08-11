with id_map_data as (
select 
    left_product as product, 
    left_value as m_id
from 
    kskdp.fvs_ffs_ue_idmp_all_id_pair_rel_all
where 
    p_date = '20210620' 
    and left_type = 'DEVICE_ID' 
    and left_value is not null 
    and right_value is not null
group by 
    left_product, 
    left_value
)
,daily_actv_detail as (
select 
     device_id, 
     product,
     is_new_device,
     'ks' as product_tag
from 
    kscdm.dim_ks_device_daily
where 
    p_date = '20210621' 
    and device_id is not null 
    and is_today_active = 1 
    and upper(trim(device_id)) not like 'KWE%' 
    and product is not null
    and lower(trim(device_id)) not in ('', '0', 'unknown', 'android_unknown', '0000000000000000','-1','null','test_user_id')
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
    p_date = '20210621' 
    and device_id is not null 
    and is_today_active = 1 
    and upper(trim(device_id)) not like 'KWE%' 
    and product is not null
    and lower(trim(device_id)) not in ('', '0', 'unknown', 'android_unknown', '0000000000000000','-1','null','test_user_id')
group by 
    device_id, 
    is_today_new,
    product
)
,daily_actv as (
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
)
,result as (
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
    id_map_data m on d.device_id = m.m_id
                 and d.product = m.product 
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