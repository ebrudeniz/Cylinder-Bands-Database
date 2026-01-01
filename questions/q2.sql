USE cylinder_bands_db;

-- ============================================================================
-- SORU 2: Compute the maximum ink temperature for each cylinder size
-- ============================================================================
-- İhtiyacımız olan kolonlar:
--   - cylinder_size (string)
--   - ink_temperature (numeric)

SELECT 
    sv.string_value AS cylinder_size,
    MAX(nv.numeric_value) AS max_ink_temperature,
    COUNT(DISTINCT r.run_id) AS run_count
FROM runs r
-- Cylinder size için JOIN
JOIN runid_stringvalues sv ON r.run_id = sv.run_id
JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
    AND sc.column_name = 'cylinder_size'
-- Ink temperature için JOIN
JOIN runid_numericvalues nv ON r.run_id = nv.run_id
JOIN numericcols nc ON nv.numericcol_id = nc.numericcol_id
    AND nc.column_name = 'ink_temperature'
WHERE nv.numeric_value IS NOT NULL
GROUP BY sv.string_value
ORDER BY max_ink_temperature DESC;

