USE cylinder_bands_db;
-- ============================================================================
-- SORU 9: Compute the average chrome content grouped by press type
-- ============================================================================
-- İhtiyacımız olan kolonlar:
--   - press_type (string)
--   - chrome_content (numeric)

SELECT 
    sv.string_value AS press_type,
    AVG(nv.numeric_value) AS avg_chrome_content,
    COUNT(DISTINCT r.run_id) AS run_count
FROM runs r
-- Press type için JOIN
JOIN runid_stringvalues sv ON r.run_id = sv.run_id
JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
    AND sc.column_name = 'press_type'
-- Chrome content için JOIN
JOIN runid_numericvalues nv ON r.run_id = nv.run_id
JOIN numericcols nc ON nv.numericcol_id = nc.numericcol_id
    AND nc.column_name = 'chrome_content'
WHERE nv.numeric_value IS NOT NULL
GROUP BY sv.string_value
ORDER BY avg_chrome_content DESC;

