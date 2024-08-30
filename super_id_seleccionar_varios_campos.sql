WITH all_ids AS (
  SELECT super_id FROM T1
  UNION DISTINCT
  SELECT super_id FROM T2
  UNION DISTINCT
  SELECT super_id FROM T3
)
SELECT 
  all_ids.super_id AS j_super_id,
  T1.super_id AS T1_super_id,
  T1.fecha AS T1_fecha,
  T1.ventas AS T1_ventas,
  T1.compras AS T1_compras,
  T1.folio AS T1_folio,
  T2.super_id AS T2_super_id,
  T2.fecha AS T2_fecha,
  T2.ventas AS T2_ventas,
  T2.compras AS T2_compras,
  T2.folio AS T2_folio,
  T3.super_id AS T3_super_id,
  T3.fecha AS T3_fecha,
  T3.ventas AS T3_ventas,
  T3.compras AS T3_compras,
  T3.folio AS T3_folio
FROM all_ids
LEFT JOIN T1 ON all_ids.super_id = T1.super_id
LEFT JOIN T2 ON all_ids.super_id = T2.super_id
LEFT JOIN T3 ON all_ids.super_id = T3.super_id
