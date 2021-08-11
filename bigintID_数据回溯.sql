with today_actv as (
select
     raw_id,
     ultima_id,
     p_date,
     id_type,
     business,
     product
from
    kskdp.fvs_ffs_ue_idmp_ultima_id_mapping_1d
where
    p_date = '{{ ds_nodash }}'
and 
    id_type = 'DEVICE_ID'
)
,

history_all as ( 
select
     raw_id,
     ultima_id,
     first_date,
     last_date,
     p_date,
     id_type,
     business,
     product
from
    kskdp.fvs_ffs_ue_idmp_ultima_id_mapping_all
where
    p_date = '{{ ds_nodash - 1 }}'
and 
    id_type = 'DEVICE_ID'
)

insert overwrite table kskdp.fvs_ffs_ue_idmp_ultima_id_mapping_all partition(p_date ='{{ ds_nodash }}', id_type='DEVICE_ID', business, product)
select
     coalesce(t.raw_id, h.raw_id)       as raw_id,
     coalesce(h.ultima_id, t.ultima_id) as ultima_id,
     coalesce(h.first_date, t.p_date)   as first_date,
     coalesce(t.p_date, h.last_date)    as last_date,
     coalesce(h.business, t.business)   as business,
     coalesce(h.product, t.product)     as product
from
    today_actv t
full join
    history_all h 
on 
    t.raw_id   = h.raw_id
and 
    t.id_type  = h.id_type
and 
    t.business = h.business
and 
    t.product  = h.product
; 