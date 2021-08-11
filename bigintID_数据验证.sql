

-- 日表

-- 快手系日活覆盖率
select
    b.product,
    count(1) as actv_cnt,
    count(distinct b.device_id) as actv_unique_cnt,
    count(distinct r.device_id) as ultima_cover_cnt,
    count(distinct b.device_id) - count(distinct r.device_id) as cnt_diff
from
    (
    	select
    		p_product as product ,
    		device_id,
    		p_date 
		from 
		    ks_dw_fact.ptc_device_active_di
		where
		    p_date between '{{ ds_nodash-32 }}' and '{{ ds_nodash }}'
	) b
left join
    (
    	select 
    		raw_id as device_id,
    		product,
    		p_date
		from
		    kskdp.fvs_ffs_ue_idmp_ultima_id_mapping_1d
		where
		    p_date between '{{ ds_nodash-32 }}' and '{{ ds_nodash }}'
		and 
			business = 'KS'
	) r 
on 
	b.device_id = r.device_id 
and 
	b.product = r.product
and
	b.p_date = r.p_date
group by
    b.product,
    b.p_date


-- 新业务日活覆盖率
select
    b.product,
    count(1) as actv_cnt,
    count(distinct b.device_id) as actv_unique_cnt,
    count(distinct r.device_id) as ultima_cover_cnt,
    count(distinct b.device_id) - count(distinct r.device_id) as cnt_diff
from
    (	
    	select
    		func_product as product,
    		device_id
		from
		    npcdm.dwd_pub_usr_actv_device_di
		where
		    p_date = '{{ ds_nodash }}'
	) b
left join
    (
    	select 
    		raw_id as device_id,
    		product
		from
    		kskdp.fvs_ffs_ue_idmp_ultima_id_mapping_1d
		where
    		p_date = '{{ ds_nodash }}'
    	and 
    		business = 'NP'
    ) r 
on 
	b.device_id = r.device_id 
and 
	b.product = r.product
group by
    b.product
;

-- 稳定性
select
    c1.id_type,
    c1.business,
    c1.product,
    count(1) as actv_cross_cnt,
    sum(if(c1.ultima_id=c2.ultima_id,1,0)) as id_equal_cnt,
    count(1) - sum(if(c1.ultima_id=c2.ultima_id,1,0)) as diff
from
    (
		select
		    business,
		    product,
		    raw_id,
		    ultima_id,
		    id_type
		from
		    kskdp.fvs_ffs_ue_idmp_ultima_id_mapping_1d
		where
		    p_date = '{{ ds_nodash }}'
	) c1
join
    (
		select
		    business,
		    product,
		    raw_id,
		    ultima_id,
		    id_type
		from
		    kskdp.fvs_ffs_ue_idmp_ultima_id_mapping_1d
		where
		    p_date = '{{ ds_nodash - 1 }}'
	) c2 
on 
	c1.business = c2.business 
and 
	c1.product = c2.product 
and 
	c1.raw_id = c2.raw_id 
and 
	c1.id_type = c2.id_type
group by
    c1.business,
    c1.product,
    c1.id_type


-- 总表

-- 快手系日活累积覆盖率
kscdm.dwd_ks_usr_prod_device_active_df


-- 新业务日活累积覆盖率
npcdm.dwd_pub_usr_all_device_df


-- 稳定性
