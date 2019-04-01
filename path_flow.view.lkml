view: path_flow {
  derived_table: {
    sql: with t1 as (SELECT
concat(post_visid_high, post_visid_low, safe_cast(visit_num as string)) as ssid,
post_pagename end as page,
row_number() over(partition by concat(post_visid_high, post_visid_low, safe_cast(visit_num as string)) order by hit_time_gmt) as hit_num
FROM
  `my-gcp-project.datafeed.hit_data`
WHERE
  {% condition date_filter %} hit_time_gmt {% endcondition %}
  and exclude_hit = 0
  and post_page_event = 0)

, t2 as (select
ssid,
max(case when hit_num = 1 then page else '' end) as step1,
max(case when hit_num = 2 then page else '' end) as step2,
max(case when hit_num = 3 then page else '' end) as step3
from t1
group by ssid)

select step1, step2, step3, count(1) as pathview
from t2
group by 1, 2, 3
order by 4 desc
 ;;
  }

  filter: date_filter {
    type: date
  }

  parameter: date_view {
    type: unquoted
    default_value: "Day"
    allowed_value: {
      label: "Year"
      value: "YEAR"
    }
    allowed_value: {
      label: "Month"
      value: "MONTH"
    }
    allowed_value: {
      label: "Day"
      value: "DATE"
    }
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: step1 {
    type: string
    sql: ${TABLE}.step1 ;;
  }

  dimension: step2 {
    type: string
    sql: ${TABLE}.step2 ;;
  }

  dimension: step3 {
    type: string
    sql: ${TABLE}.step3 ;;
  }

  dimension: pathview {
    type: number
    sql: ${TABLE}.pathview ;;
  }

  measure: pathview_count {
    label: "PathView"
    type: sum
    sql: ${pathview} ;;
  }

  set: detail {
    fields: [step1, step2, step3, pathview]
  }
}