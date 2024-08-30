--Las funciones de ventana son muy útiles para realizar cálculos a través de un conjunto de filas que están relacionadas con la fila actual.

--Ranking

--Supongamos que tenemos una tabla 'ventas' con columnas: vendedor, fecha, monto
SELECT
  vendedor,
  fecha,
  monto,
  -- Asigna un ranking a cada venta por vendedor, ordenado por monto de forma descendente
  RANK() OVER (PARTITION BY vendedor ORDER BY monto DESC) as ranking_ventas,
  -- Asigna un ranking denso (sin huecos) a cada venta por vendedor
  DENSE_RANK() OVER (PARTITION BY vendedor ORDER BY monto DESC) as dense_ranking_ventas,
  -- Asigna un número de fila a cada venta por vendedor
  ROW_NUMBER() OVER (PARTITION BY vendedor ORDER BY fecha) as numero_venta
FROM ventas

--Cálculos móviles
-- Usando la misma tabla 'ventas'
SELECT
  vendedor,
  fecha,
  monto,
  -- Calcula la media móvil de las últimas 3 ventas para cada vendedor
  AVG(monto) OVER (
    PARTITION BY vendedor 
    ORDER BY fecha 
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) as media_movil_3,
  -- Calcula la suma acumulada de ventas para cada vendedor
  SUM(monto) OVER (
    PARTITION BY vendedor 
    ORDER BY fecha
    ROWS UNBOUNDED PRECEDING
  ) as ventas_acumuladas
FROM ventas

--Comparaciones con filas anteriores o siguientes

-- Supongamos una tabla 'stock' con columnas: producto, fecha, cantidad
SELECT
  producto,
  fecha,
  cantidad,
  -- Obtiene la cantidad del día anterior
  LAG(cantidad) OVER (PARTITION BY producto ORDER BY fecha) as cantidad_dia_anterior,
  -- Calcula la diferencia con el día anterior
  cantidad - LAG(cantidad) OVER (PARTITION BY producto ORDER BY fecha) as diferencia_dia_anterior,
  -- Obtiene la cantidad del día siguiente
  LEAD(cantidad) OVER (PARTITION BY producto ORDER BY fecha) as cantidad_dia_siguiente
FROM stock

Percentiles y distribución

-- Usando la tabla 'ventas' nuevamente
SELECT
  vendedor,
  monto,
  -- Calcula el percentil de cada venta dentro de las ventas del vendedor
  PERCENT_RANK() OVER (PARTITION BY vendedor ORDER BY monto) as percentil_venta,
  -- Divide las ventas de cada vendedor en 4 cuartiles
  NTILE(4) OVER (PARTITION BY vendedor ORDER BY monto) as cuartil_venta
FROM ventas

--Primero y último valor en un grupo

-- Supongamos una tabla 'sesiones' con columnas: usuario, fecha_inicio, duracion
SELECT
  usuario,
  -- Obtiene la fecha de la primera sesión del usuario
  FIRST_VALUE(fecha_inicio) OVER (
    PARTITION BY usuario 
    ORDER BY fecha_inicio
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) as primera_sesion,
  -- Obtiene la duración de la última sesión del usuario
  LAST_VALUE(duracion) OVER (
    PARTITION BY usuario 
    ORDER BY fecha_inicio
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) as duracion_ultima_sesion
FROM sesiones

--Cálculos de la diferencia con la media del grupo

-- Usando la tabla 'ventas' una vez más
SELECT
  vendedor,
  fecha,
  monto,
  -- Calcula la media de ventas para cada vendedor
  AVG(monto) OVER (PARTITION BY vendedor) as media_ventas_vendedor,
  -- Calcula la diferencia entre cada venta y la media del vendedor
  monto - AVG(monto) OVER (PARTITION BY vendedor) as diferencia_con_media
FROM ventas
Estos ejemplos muestran diferentes usos de las funciones de ventana en BigQuery. Algunas cosas importantes a tener en cuenta:

PARTITION BY divide los datos en grupos para los cálculos.
ORDER BY determina el orden de las filas dentro de cada partición.
La cláusula ROWS BETWEEN permite definir el marco de la ventana para los cálculos.
