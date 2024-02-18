/*
CREATE OR REPLACE FUNCTION pg_temp.decode_url_part(p varchar) -- function
 RETURNS varchar
AS $$
	select convert_from
	(cast(E'\\x' || string_agg(
	  case when lenght(r.m[1]) = 1 then encode(convert_to(r.m[1], 'SQL_ASCII'), 'hex')
	  else substring(r.m[1] from 2 for 2) end, '') as bytea), 'UTF8')
from regexp_matches($1, '%[0-9a-f]|.', 'gi') as r(m);
$$ language sql immutable strict;
*/
with   totalFB_CTE as (
select fabd.ad_date,
       case
         when lower(substring(decode_url_part(url_parameters), 'utm_campaign=([\w|\d]+)')) = 'nan' then null
         else lower(substring(decode_url_part(url_parameters), 'utm_campaign=([\w|\d]+)'))
         end as utm_campaign,
       coalesce (spend, 0) as spend,
       coalesce (impressions, 0) as impressions,
       coalesce (reach, 0) as reach,
       coalesce (clicks, 0) as clicks,
       coalesce (leads, 0) as leads,
       coalesce (value, 0) as value
from facebook_ads_basic_daily fabd
     left join facebook_adset fa
     on fabd.adset_id = fa.adset_id
     left join facebook_campaign fc
     on fabd.campaign_id = fc.campaign_id
),
total_FB_Google_CTE as ( 
select ad_date,
       utm_campaign, spend, impressions, 
       reach, clicks, leads, value
from totalFB_CTE
union all
select ad_date,
       case
         when lower(substring(decode_url_part(url_parameters), 'utm_campaign=([\w|\d]+)')) = 'nan' then null
         else lower(substring(decode_url_part(url_parameters), 'utm_campaign=([\w|\d]+)'))
         end as utm_campaign,
       coalesce (spend, 0) as spend,
       coalesce (impressions, 0) as impressions,
       coalesce (reach, 0) as reach,
       coalesce (clicks, 0) as clicks,
       coalesce (leads, 0) as leads,
       coalesce (value, 0) as value
 from google_ads_basic_daily
),
month_FB_Google_CTE as (
select date_trunc('month', ad_date) as ad_month,
       utm_campaign,
       sum(spend) as total_spend,
       sum(impressions) as total_impressions,
       sum(clicks) as total_clicks,
       sum(value) as total_value,
       case 
         when sum(clicks)>0 then round(sum(spend)/sum(clicks)::numeric,4)
         end CPC,
       case
         when sum(impressions)>0 then round(sum(clicks)::numeric/sum(impressions),4)
         end CTR,
       case
         when sum(impressions)>0 then round(1000*sum(spend)::numeric/sum(impressions),4)
         end CPM,
       case
         when sum(spend)>0 then round(sum(value)::numeric/sum(spend),4)
         end ROMI
from total_FB_Google_CTE
group by ad_month, utm_campaign
)
select to_char(ad_month, 'YYYY-MM-DD') as ad_month, 
       utm_campaign,
       total_spend, total_impressions, total_clicks, total_value,
       CPC, CTR, CPM, ROMI,
       round(CTR/lag(CTR, 1) over (partition by utm_campaign
       order by ad_month)*100-100, 4) as CTR_diff_percent,
       round(CPM/lag(CPM, 1) over (partition by utm_campaign
       order by ad_month)*100-100, 4) as CPM_diff_percent,
       round(ROMI/lag(ROMI, 1) over (partition by utm_campaign
       order by ad_month)*100-100, 4) as ROMI_diff_percent
from month_FB_Google_CTE
order by utm_campaign;
