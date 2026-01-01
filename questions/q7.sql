USE cylinder_bands_db;
-- ============================================================================
-- SORU 7: Find the average humidity for each paper type
-- ============================================================================
-- İhtiyacımız olan kolonlar:
--   - paper_type (string)
--   - humidity (numeric)

SELECT 
    sv.string_value AS paper_type,
    AVG(nv.numeric_value) AS avg_humidity,
    COUNT(DISTINCT r.run_id) AS run_count
FROM runs r
-- Paper type için JOIN
JOIN runid_stringvalues sv ON r.run_id = sv.run_id
JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
    AND sc.column_name = 'paper_type'
-- Humidity için JOIN
JOIN runid_numericvalues nv ON r.run_id = nv.run_id
JOIN numericcols nc ON nv.numericcol_id = nc.numericcol_id
    AND nc.column_name = 'humidity'
WHERE nv.numeric_value IS NOT NULL
GROUP BY sv.string_value
ORDER BY avg_humidity DESC;

