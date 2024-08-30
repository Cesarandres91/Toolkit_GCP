-- Obtenemos la fecha actual
WITH 
current_date AS (
  SELECT DATE(CURRENT_TIMESTAMP()) AS date
),

-- Generamos una lista de días hábiles para los últimos 3 meses
workdays AS (
  SELECT 
    date,
    DATE_TRUNC(date, MONTH) AS month_start,
    -- Contamos los días hábiles dentro de cada mes
    COUNT(*) OVER (PARTITION BY DATE_TRUNC(date, MONTH) ORDER BY date) AS workday_of_month
  FROM UNNEST(GENERATE_DATE_ARRAY(
    (SELECT DATE_SUB(DATE_TRUNC(date, MONTH), INTERVAL 2 MONTH) FROM current_date),
    (SELECT date FROM current_date),
    INTERVAL 1 DAY
  )) AS date
  WHERE EXTRACT(DAYOFWEEK FROM date) BETWEEN 2 AND 6  -- Lunes a Viernes
),

-- Obtenemos el número de día hábil del mes actual
current_workday AS (
  SELECT workday_of_month
  FROM workdays
  WHERE date = (SELECT date FROM current_date)
),

-- Calculamos las compras mensuales por usuario
monthly_purchases AS (
  SELECT
    t.usuario,
    DATE_TRUNC(t.fecha, MONTH) AS month,
    SUM(t.compras) AS total_compras
  FROM tu_tabla t
  JOIN workdays w ON t.fecha = w.date
  WHERE 
    -- Filtramos los últimos 3 meses
    t.fecha >= (SELECT DATE_SUB(DATE_TRUNC(date, MONTH), INTERVAL 2 MONTH) FROM current_date)
    -- Aseguramos que solo consideramos hasta el mismo día hábil en cada mes
    AND w.workday_of_month <= (SELECT workday_of_month FROM current_workday)
  GROUP BY t.usuario, DATE_TRUNC(t.fecha, MONTH)
)

-- Consulta principal
SELECT
  mp.usuario,
  -- Compras del mes actual
  MAX(CASE WHEN mp.month = DATE_TRUNC((SELECT date FROM current_date), MONTH) THEN mp.total_compras END) AS compras_mes_actual,
  -- Compras del mes pasado
  MAX(CASE WHEN mp.month = DATE_TRUNC(DATE_SUB((SELECT date FROM current_date), INTERVAL 1 MONTH), MONTH) THEN mp.total_compras END) AS compras_mes_pasado,
  -- Compras del mes previo al pasado
  MAX(CASE WHEN mp.month = DATE_TRUNC(DATE_SUB((SELECT date FROM current_date), INTERVAL 2 MONTH), MONTH) THEN mp.total_compras END) AS compras_mes_previo,
  -- Diferencia entre el mes actual y el mes pasado
  MAX(CASE WHEN mp.month = DATE_TRUNC((SELECT date FROM current_date), MONTH) THEN mp.total_compras END) -
  MAX(CASE WHEN mp.month = DATE_TRUNC(DATE_SUB((SELECT date FROM current_date), INTERVAL 1 MONTH), MONTH) THEN mp.total_compras END) AS diferencia_actual_pasado,
  -- Porcentaje de variación entre el mes actual y el mes pasado
  SAFE_DIVIDE(
    (MAX(CASE WHEN mp.month = DATE_TRUNC((SELECT date FROM current_date), MONTH) THEN mp.total_compras END) -
     MAX(CASE WHEN mp.month = DATE_TRUNC(DATE_SUB((SELECT date FROM current_date), INTERVAL 1 MONTH), MONTH) THEN mp.total_compras END)),
    MAX(CASE WHEN mp.month = DATE_TRUNC(DATE_SUB((SELECT date FROM current_date), INTERVAL 1 MONTH), MONTH) THEN mp.total_compras END)
  ) * 100 AS porcentaje_variacion
FROM monthly_purchases mp
GROUP BY mp.usuario
