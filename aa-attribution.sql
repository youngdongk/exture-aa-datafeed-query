WITH
  t1 AS (
  SELECT
    CONCAT(post_visid_high, post_visid_low) AS vid,
    hit_time_gmt,
    SPLIT(post_evar41, ',')[OFFSET(0)] AS cmpid,
    SIGN(SUM(CASE
          WHEN CONCAT(',', post_event_list, ',') LIKE '%,271,%' THEN 1
          ELSE 0 END) OVER(PARTITION BY CONCAT(post_visid_high, post_visid_low)
      ORDER BY
        hit_time_gmt DESC ROWS BETWEEN UNBOUNDED PRECEDING
        AND CURRENT row)) AS cvflag
  FROM
    `exture.mydata`
  WHERE
    DATE(hit_time_gmt, 'Asia/Tokyo') >= '2018-10-01'
    AND DATE(hit_time_gmt, 'Asia/Tokyo') < '2018-10-08'
    AND exclude_hit = 0 ),
  t2 AS (
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
    hit_time_gmt )
SELECT
  cmpid,
  SUM(first_touch) AS first_touch,
  SUM(last_touch) AS last_touch,
  SUM(participation) AS participation
FROM
  t2
GROUP BY
  cmpid
ORDER BY
  4 DESC
