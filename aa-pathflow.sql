WITH
  base AS (
  SELECT
    CONCAT(visid_high, visid_low, CAST(visit_num AS string)) AS session,
    datetime(hit_time_gmt, 'Asita/Tokyo') AS time,
    post_pagename AS start,
    LEAD(post_pagename, 1) OVER(PARTITION BY CONCAT(post_visid_high, post_visid_low, CAST(visit_num AS string)) ORDER BY hit_time_gmt) AS next1,
    LEAD(post_pagename, 2) OVER(PARTITION BY CONCAT(post_visid_high, post_visid_low, CAST(visit_num AS string)) ORDER BY hit_time_gmt) AS next2
  FROM
    `datafeed.hit_data`
  WHERE
    DATE(hit_time_gmt, 'Asia/Tokyo') >= '2018-08-01'
    AND DATE(hit_time_gmt, 'Asia/Tokyo') < '2018-09-01'
    AND exclude_hit = 0
    AND post_page_event = 0
    AND duplicate_purchase = 0 
  ),
  flow AS (
  SELECT
    start,
    SUM(COUNT(1)) OVER() AS start_cnt, next1 AS next1b,
    SUM(COUNT(1)) OVER(PARTITION BY start, next1) next1_cnt, next2 AS next2,
    COUNT(1) AS next2_cnt
  FROM
    base
  WHERE
    start = 'ext:index.html'
  GROUP BY
    start, next1, next2 
  ),
  base2 AS (
  SELECT
    start,
    start_cnt,
    next1b AS next1,
    next1_cnt,
    ROUND(100 * next1_cnt / start_cnt, 2) AS next1_rate,
    ROW_NUMBER() OVER(PARTITION BY next1b ORDER BY next2_cnt DESC) AS rnk,
    next2,
    next2_cnt,
    ROUND(100 * next2_cnt /next1_cnt, 2) AS next2_rate
  FROM
    flow
  ORDER BY
    next1_cnt DESC,
    next2_cnt DESC)
SELECT
  start,
  start_cnt,
  next1,
  next1_cnt,
  next1_rate,
  next2,
  next2_cnt,
  next2_rate
FROM
  base2
WHERE
  rnk <=5
ORDER BY
  next1_cnt DESC, next2_cnt DESC
