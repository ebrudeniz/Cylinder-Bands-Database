USE cylinder_bands_db;
-- ============================================================================
-- SORU 5: What is the total ink percentage used by each press machine?
-- ============================================================================
-- İhtiyacımız olan kolonlar:
--   - press (string veya numeric?) - "press" kolonu muhtemelen string
--   - ink_pct (numeric)

SELECT 
    sv.string_value AS press_machine,
    SUM(nv.numeric_value) AS total_ink_pct,
    AVG(nv.numeric_value) AS avg_ink_pct,
    COUNT(DISTINCT r.run_id) AS run_count
FROM runs r
-- Press için JOIN
JOIN runid_stringvalues sv ON r.run_id = sv.run_id
JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
    AND sc.column_name = 'press'
-- Ink percentage için JOIN
JOIN runid_numericvalues nv ON r.run_id = nv.run_id
JOIN numericcols nc ON nv.numericcol_id = nc.numericcol_id
    AND nc.column_name = 'ink_pct'
WHERE nv.numeric_value IS NOT NULL
GROUP BY sv.string_value
ORDER BY total_ink_pct DESC;

