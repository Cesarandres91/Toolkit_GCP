WITH 
T1_with_key AS (
  SELECT 
    CONCAT(super_id, '|', usuario, '|', email, '|', normal_id) AS key,
    super_id, usuario, email, normal_id, fecha, ventas, compras, folio
  FROM T1
),
T2_with_key AS (
  SELECT 
    CONCAT(super_id, '|', usuario, '|', email, '|', normal_id) AS key,
    super_id, usuario, email, normal_id, fecha, ventas, compras, folio
  FROM T2
),
T3_with_key AS (
  SELECT 
    CONCAT(super_id, '|', usuario, '|', email, '|', normal_id) AS key,
    super_id, usuario, email, normal_id, fecha, ventas, compras, folio
  FROM T3
),
all_keys AS (
  SELECT key FROM T1_with_key
  UNION DISTINCT
  SELECT key FROM T2_with_key
  UNION DISTINCT
  SELECT key FROM T3_with_key
)
SELECT 
  all_keys.key AS j_key,
  T1.key AS T1_key,
  T1.super_id AS T1_super_id,
  T1.usuario AS T1_usuario,
  T1.email AS T1_email,
  T1.normal_id AS T1_normal_id,
  T1.fecha AS T1_fecha,
  T1.ventas AS T1_ventas,
  T1.compras AS T1_compras,
  T1.folio AS T1_folio,
  T2.key AS T2_key,
  T2.super_id AS T2_super_id,
  T2.usuario AS T2_usuario,
  T2.email AS T2_email,
  T2.normal_id AS T2_normal_id,
  T2.fecha AS T2_fecha,
  T2.ventas AS T2_ventas,
  T2.compras AS T2_compras,
  T2.folio AS T2_folio,
  T3.key AS T3_key,
  T3.super_id AS T3_super_id,
  T3.usuario AS T3_usuario,
  T3.email AS T3_email,
  T3.normal_id AS T3_normal_id,
  T3.fecha AS T3_fecha,
  T3.ventas AS T3_ventas,
  T3.compras AS T3_compras,
  T3.folio AS T3_folio
FROM all_keys
LEFT JOIN T1_with_key T1 ON all_keys.key = T1.key
LEFT JOIN T2_with_key T2 ON all_keys.key = T2.key
LEFT JOIN T3_with_key T3 ON all_keys.key = T3.key


/*
Esta consulta hace lo siguiente:

Crea tres CTEs (T1_with_key, T2_with_key, T3_with_key) que añaden un campo key a cada tabla, concatenando super_id, usuario, email y normal_id.
Crea una CTE all_keys que contiene todas las key únicas de las tres tablas.
Hace un LEFT JOIN de all_keys con cada una de las tablas modificadas.
Selecciona todos los campos requeridos de cada tabla, incluyendo la nueva key, super_id, usuario, email, normal_id, fecha, ventas, compras y folio.

El resultado será una tabla con las siguientes columnas:

j_key: Todas las key únicas de las tres tablas.
Para cada tabla (T1, T2, T3):

Tn_key: La key de la tabla (será NULL si no existe en esa tabla)
Tn_super_id: El campo super_id de la tabla
Tn_usuario: El campo usuario de la tabla
Tn_email: El campo email de la tabla
Tn_normal_id: El campo normal_id de la tabla
Tn_fecha: El campo fecha de la tabla
Tn_ventas: El campo ventas de la tabla
Tn_compras: El campo compras de la tabla
Tn_folio: El campo folio de la tabla


Donde n es 1, 2 o 3 dependiendo de la tabla.
 Si una key no existe en una tabla específica, los campos correspondientes a esa tabla serán NULL.
*/
