WITH
  list AS (
  SELECT
    hit_id, l4
  FROM
    `my_stats.hit_data`, 
    UNNEST(SPLIT(listvar, ',') ) AS l4
  WHERE
    hit_date >= '2018-08-21'
    AND hit_date < '2018-08-22')
SELECT
  list.l4 AS list4, 
  COUNT(h.post_event_list) AS event1
FROM
  `datafeed.hit_data` AS h
LEFT JOIN
  list
ON
  h.post_evar21 = list.hit_id
WHERE
  h.exclude_hit = 0
  AND CONCAT(',', h.post_event_list, ',') LIKE '%,200,%'
  AND h.hit_time_gmt >= TIMESTAMP("2018-08-21")
  AND h.hit_time_gmt < TIMESTAMP("2018-08-22")
GROUP BY
  list4
