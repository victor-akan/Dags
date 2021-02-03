--drop table insights_cust_info purge;

create table insights_cust_info as
select a.bra_code,a.cus_num, a.bra_code||a.cus_num as cust_key, cus_name,cus_mobile,cus_telephone,
cus_email,upper(cus_address) as cus_address,date_open,cus_age,cus_pc_code,cust_mgr,bvn,
cus_marital_status,mrtl_sta,risk_lvl,cus_gender,substr(nvl(cus_pc_code,cust_mgr),1,3) team_code,cust_grade_seg,map_acc_no,
from_bus, d.des_eng business,
c.prof_code, c.prof, c.edu_lvl, c.edu,
town,city_loc_code,b.des_eng cus_city, b.region cus_region, b.state cus_state,
int_oblig_rate,e.des_eng as sme_class,
(case when has_card='YES' then 1 else 0 end) as has_card,
f.account_status,f.fund_sta,f.is_closed,f.is_inactive
from stg.src_customer_extd a
left join
(select * from stg.src_text_tab x
left join ifeoluwa_akande.nigeria_states y
on x.con_val_2=y.state_code
where tab_id=190) b
on a.city_loc_code=b.tab_ent
left join 
(
select x.bra_code, x.cus_num, x.prof_code, x.edu_lvl, y.des_eng prof, z.des_eng edu
from stg.src_cust_pro x
left join (select * from stg.src_text_tab where tab_id=130) y
on x.prof_code=y.tab_ent
left join (select * from stg.src_text_tab where tab_id=571) z
on x.edu_lvl=z.tab_ent
) c
on a.bra_code=c.bra_code and a.cus_num=c.cus_num
left join 
(select * from stg.src_text_tab where tab_id=811) d
on a.from_bus=d.tab_ent
left join
(select * from stg.src_text_tab where tab_id=9257) e
on a.int_oblig_rate=e.tab_ent
left join 
sas_va.cust_status f
on a.bra_code=f.bra_code and a.cus_num=f.cus_num
where a.cus_num>99999;

