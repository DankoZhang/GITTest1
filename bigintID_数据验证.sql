

-- ks_dw_fact.ptc_device_active_di：活跃设备表
-- npcdm.dwd_pub_usr_actv_device_di：新业务活跃设备表
-- kscdm.dwd_ks_usr_prod_device_active_df：分产品设备活跃累积明细表
-- npcdm.dwd_pub_usr_all_device_df：新业务全量设备表





-- 日表

-- 快手系日活覆盖率
select
	b.p_date,
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
;

-- 新业务日活覆盖率
select
	b.p_date,
    b.product,
    count(1) as actv_cnt,
    count(distinct b.device_id) as actv_unique_cnt,
    count(distinct r.device_id) as ultima_cover_cnt,
    count(distinct b.device_id) - count(distinct r.device_id) as cnt_diff
from
    (	
    	select
    		func_product as product,
    		device_id,
    		p_date
		from
		    npcdm.dwd_pub_usr_actv_device_di
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
    		business = 'NP'
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
;

-- 稳定性
select
	c1.p_date,
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
		    id_type,
		    p_date
		from
		    kskdp.fvs_ffs_ue_idmp_ultima_id_mapping_1d
		where
		    p_date between '{{ ds_nodash-32 }}' and '{{ ds_nodash }}'
	) c1
inner join
    (
		select
		    business,
		    product,
		    raw_id,
		    ultima_id,
		    id_type,
		    from_unixtime(unix_timestamp(cast(date_add(from_unixtime(unix_timestamp(p_date,'yyyymmdd'),'yyyy-mm-dd'), 1) as string),'yyyy-mm-dd'),'yyyymmdd') as p_date
		from
		    kskdp.fvs_ffs_ue_idmp_ultima_id_mapping_1d
		where
			p_date between '{{ ds_nodash-33 }}' and '{{ ds_nodash-1 }}'

	) c2 
on 
	c1.business = c2.business 
and 
	c1.product = c2.product 
and 
	c1.raw_id = c2.raw_id 
and 
	c1.id_type = c2.id_type
and
	c1.p_date = c2.p_date
group by
    c1.business,
    c1.product,
    c1.id_type,
    c1.p_date
;

-- 稳定率存在问题，原因：
-- 1、整体流程：根据kskdp.fvs_ffs_ue_idmp_ultima_id_actv_raw_data_1d（BigintID映射-业务日活原始ID采集）获取每日原始ID，再将原始ID与昨日全量表（kskdp.fvs_ffs_ue_idmp_ultima_id_mapping_all）
-- 			  进行关联，获取今日新增ID，对新增ID生成整型ID，再对原始ID与昨日全量表以及打码后的新增ID表进行关联，对原始ID匹配整型码，且优先使用历史数据，若历史数据为空，则使用新码
-- 2、稳定性存在问题的原因：比如12日日数据正常，而12日全量数据任务未执行，在计算13日数据时，由于12日全量数据分区无数据，所以在将原始ID表与昨日全量表关联时，原始ID全部保留了下来，导致所有原始ID都进行了打码，
--    以此类推，导致每一天的日表，都进行了重新打码，导致相邻两天打码数据不一致，出现稳定性较低的情况



-- 总表

-- 快手系日活累积覆盖率
select
	b.p_date,
    b.product,
    count(1) as actv_cnt,
    count(distinct b.device_id) as actv_unique_cnt,
    count(distinct r.device_id) as ultima_cover_cnt,
    count(distinct b.device_id) - count(distinct r.device_id) as cnt_diff
from
    (
    	select
    		product ,
    		device_id,
    		p_date 
		from 
		    kscdm.dwd_ks_usr_prod_device_active_df
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
		    kskdp.fvs_ffs_ue_idmp_ultima_id_mapping_all
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
;



-- 新业务日活累积覆盖率
select
	b.p_date,
    b.product,
    count(1) as actv_cnt,
    count(distinct b.device_id) as actv_unique_cnt,
    count(distinct r.device_id) as ultima_cover_cnt,
    count(distinct b.device_id) - count(distinct r.device_id) as cnt_diff
from
    (	
    	select
    		func_product as product,
    		device_id,
    		p_date
		from
		    npcdm.dwd_pub_usr_all_device_df
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
    		kskdp.fvs_ffs_ue_idmp_ultima_id_mapping_all
		where
		    p_date between '{{ ds_nodash-32 }}' and '{{ ds_nodash }}'
    	and 
    		business = 'NP'
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
;


-- 稳定性

-- 稳定性
select
	c1.p_date,
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
		    id_type,
		    p_date
		from
		    kskdp.fvs_ffs_ue_idmp_ultima_id_mapping_all
		where
		    p_date between '{{ ds_nodash-32 }}' and '{{ ds_nodash }}'
	) c1
inner join
    (
		select
		    business,
		    product,
		    raw_id,
		    ultima_id,
		    id_type,
		    from_unixtime(unix_timestamp(cast(date_add(from_unixtime(unix_timestamp(p_date,'yyyymmdd'),'yyyy-mm-dd'), 1) as string),'yyyy-mm-dd'),'yyyymmdd') as p_date
		from
		    kskdp.fvs_ffs_ue_idmp_ultima_id_mapping_all
		where
			p_date between '{{ ds_nodash-33 }}' and '{{ ds_nodash-1 }}'

	) c2 
on 
	c1.business = c2.business 
and 
	c1.product = c2.product 
and 
	c1.raw_id = c2.raw_id 
and 
	c1.id_type = c2.id_type
and
	c1.p_date = c2.p_date
group by
    c1.business,
    c1.product,
    c1.id_type,
    c1.p_date
;
























