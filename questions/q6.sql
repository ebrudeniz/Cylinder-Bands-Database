USE cylinder_bands_db;

-- ============================================================================
-- SORU 6: Compute the minimum viscosity value grouped by job ID
-- ============================================================================
-- İhtiyacımız olan kolonlar:
--   - job_number (string veya numeric?) - muhtemelen numeric olarak saklanmış
--   - viscosity (numeric)

-- ÇÖZÜM 1: Eğer job_number NUMERIC kolonsa:
SELECT 
    nv_job.numeric_value AS job_id,
    MIN(nv_visc.numeric_value) AS min_viscosity,
    COUNT(DISTINCT r.run_id) AS run_count
FROM runs r
-- Job number için JOIN (numeric)
JOIN runid_numericvalues nv_job ON r.run_id = nv_job.run_id
JOIN numericcols nc_job ON nv_job.numericcol_id = nc_job.numericcol_id
    AND nc_job.column_name = 'job_number'
-- Viscosity için JOIN
JOIN runid_numericvalues nv_visc ON r.run_id = nv_visc.run_id
JOIN numericcols nc_visc ON nv_visc.numericcol_id = nc_visc.numericcol_id
    AND nc_visc.column_name = 'viscosity'
WHERE nv_visc.numeric_value IS NOT NULL
GROUP BY nv_job.numeric_value
ORDER BY min_viscosity;

-- ÇÖZÜM 2: Eğer job_number STRING kolonsa:
/*
SELECT 
    sv.string_value AS job_id,
    MIN(nv.numeric_value) AS min_viscosity,
    COUNT(DISTINCT r.run_id) AS run_count
FROM runs r
JOIN runid_stringvalues sv ON r.run_id = sv.run_id
JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
    AND sc.column_name = 'job_number'
JOIN runid_numericvalues nv ON r.run_id = nv.run_id
JOIN numericcols nc ON nv.numericcol_id = nc.numericcol_id
    AND nc.column_name = 'viscosity'
WHERE nv.numeric_value IS NOT NULL
GROUP BY sv.string_value
ORDER BY min_viscosity;
*/


