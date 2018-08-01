with base as (
  select 1 as step, 'index' as pagename
  union all
  select 2 as step, 'search' as pagename
  union all
  select 3 as step, 'services' as pagename
  union all
  select 4 as step, 'inquiry' as pagename
  union all
  select 5 as step, 'complete' as pagename
)
, aa_fallout as (
  select
    concat(h.post_visid_high, h.post_visid_low, cast(visit_num as string)) as session
    ,b.step
    ,b.pagename
    ,max(h.hit_time_gmt) as max_time
    ,min(h.hit_time_gmt) as min_time
  from
    base as b
  join `adobe_gl.hit_data` as h on b.pagename = h.post_pagename
  where
    date(h.hit_time_gmt, 'Asia/Tokyo') >= '2018-06-01'
    and date(h.hit_time_gmt, 'Asia/Tokyo') < '2018-06-30'
    and h.hit_source = 1
    and h.exclude_hit = 0
    and h.post_page_event = 0
    and h.duplicate_purchase = 0
  group by
    session, b.step, b.pagename
)
, aa_fallout_v2 as (
  select
    session
    ,step
    ,pagename
    ,max_time
    ,min_time
    ,lag(min_time) over(partition by session order by step) as lag_min_time
    ,min(step) over(partition by session) as min_step
    ,count(1)
    over(partition by session order by step rows between unbounded preceding and current row) as ttl_count
  from
    aa_fallout
)
, aa_fallout_v3 as (
  select
    session, step, pagename
    from aa_fallout_v2
    where
    min_step = 1
    and step = ttl_count
    and (lag_min_time is null or max_time >= lag_min_time)
)
select
  step
  ,pagename
  ,count(1) as count
  ,round((100 * count(1) / first_value(count(1)) over(order by step rows between unbounded preceding and unbounded following)), 2) as fallout
  ,round((100 * count(1) / lag(count(1)) over(order by step)), 2) as step_fallout
from
  aa_fallout_v3
group by
  step, pagename
order by
  step
