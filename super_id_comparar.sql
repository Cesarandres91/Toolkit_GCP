WITH all_ids AS (
  SELECT super_id FROM T1
  UNION DISTINCT
  SELECT super_id FROM T2
  UNION DISTINCT
  SELECT super_id FROM T3
)
SELECT 
  all_ids.super_id AS j_super_id,
  CASE WHEN T1.super_id IS NOT NULL THEN 'OK' ELSE 'F' END AS T1_super_id,
  CASE WHEN T2.super_id IS NOT NULL THEN 'OK' ELSE 'F' END AS T2_super_id,
  CASE WHEN T3.super_id IS NOT NULL THEN 'OK' ELSE 'F' END AS T3_super_id
FROM all_ids
LEFT JOIN T1 ON all_ids.super_id = T1.super_id
LEFT JOIN T2 ON all_ids.super_id = T2.super_id
LEFT JOIN T3 ON all_ids.super_id = T3.super_id
