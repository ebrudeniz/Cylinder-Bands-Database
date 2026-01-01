USE cylinder_bands_db;
-- ============================================================================
-- SORU 4: Calculate the average roughness grouped by cylinder ID
-- ============================================================================
-- İhtiyacımız olan kolonlar:
--   - cylinder_number (string) - bu "cylinder ID" olarak kabul ediyoruz
--   - roughness (numeric)

SELECT 
    sv.string_value AS cylinder_id,
    AVG(nv.numeric_value) AS avg_roughness,
    COUNT(DISTINCT r.run_id) AS run_count
FROM runs r
-- Cylinder number için JOIN
JOIN runid_stringvalues sv ON r.run_id = sv.run_id
JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
    AND sc.column_name = 'cylinder_number'
-- Roughness için JOIN
JOIN runid_numericvalues nv ON r.run_id = nv.run_id
JOIN numericcols nc ON nv.numericcol_id = nc.numericcol_id
    AND nc.column_name = 'roughness'
WHERE nv.numeric_value IS NOT NULL
GROUP BY sv.string_value
ORDER BY avg_roughness DESC;

