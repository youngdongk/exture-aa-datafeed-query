WITH
  t1 AS (
  SELECT
    CONCAT(post_visid_high, post_visid_low) AS vid,
    hit_time_gmt,
    post_campaign AS cmpid,
    SIGN(SUM(CASE
          WHEN CONCAT(',', post_event_list, ',') LIKE '%,201,%' THEN 1
          ELSE 0 END) OVER(PARTITION BY CONCAT(post_visid_high, post_visid_low)
      ORDER BY
        hit_time_gmt DESC ROWS BETWEEN UNBOUNDED PRECEDING
        AND CURRENT row)) AS cvflag
  FROM
    `exture.hit_data`
  WHERE
    DATE(hit_time_gmt, 'Asia/Tokyo') >= '2018-10-01'
    AND DATE(hit_time_gmt, 'Asia/Tokyo') < '2018-11-01'
    AND exclude_hit = 0 )
SELECT
  vid,
  hit_time_gmt,
  cmpid,
  CASE
    WHEN ROW_NUMBER() OVER(PARTITION BY vid ORDER BY hit_time_gmt) = 1 THEN 1
    ELSE 0
  END AS first_touch,
  CASE
    WHEN ROW_NUMBER() OVER(PARTITION BY vid ORDER BY hit_time_gmt DESC) = 1 THEN 1
    ELSE 0
  END AS last_touch,
  CASE
    WHEN cmpid = LAG(cmpid) OVER(PARTITION BY vid ORDER BY hit_time_gmt) THEN 0
    ELSE 1
  END AS participation
FROM
  t1
WHERE
  cvflag = 1
  AND cmpid IS NOT NULL
ORDER BY
  vid,
  hit_time_gmt
