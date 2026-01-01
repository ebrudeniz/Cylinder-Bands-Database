USE cylinder_bands_db;

-- SORU 1: What is the average press speed grouped by press type?

-- İhtiyacımız olan kolonlar:
--   - press_speed (numeric)
--   - press_type (string)

SELECT 
    sv.string_value AS press_type,
    AVG(nv.numeric_value) AS avg_press_speed,
    COUNT(DISTINCT r.run_id) AS run_count
FROM runs r
-- Press type için JOIN (string)
JOIN runid_stringvalues sv ON r.run_id = sv.run_id
JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id 
    AND sc.column_name = 'press_type'
-- Press speed için JOIN (numeric)
JOIN runid_numericvalues nv ON r.run_id = nv.run_id
JOIN numericcols nc ON nv.numericcol_id = nc.numericcol_id
    AND nc.column_name = 'press_speed'
WHERE nv.numeric_value IS NOT NULL  -- NULL değerleri atla
GROUP BY sv.string_value
ORDER BY avg_press_speed DESC;