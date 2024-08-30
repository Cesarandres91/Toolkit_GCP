  SELECT
    EXTRACT(YEAR FROM PARSE_DATE('%Y-%m-%d', fecha)) AS ayear,
    EXTRACT(MONTH FROM PARSE_DATE('%Y-%m-%d', fecha)) AS ames,
    EXTRACT(DAY FROM PARSE_DATE('%Y-%m-%d', fecha)) AS adia,
    MAX(WD) AS max_wd
  FROM
    tabla
  GROUP BY
    ayear, ames, adia
  ORDER BY
    ayear DESC, ames DESC, adia DESC
  LIMIT 1
