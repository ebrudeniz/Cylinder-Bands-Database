USE cylinder_bands_db;
-- ============================================================================
-- SORU 10: What is the sum of varnish percentage for each solvent type?
-- ============================================================================
-- İhtiyacımız olan kolonlar:
--   - solvent_type (string)
--   - varnish_pct (numeric)

SELECT 
    sv.string_value AS solvent_type,
    SUM(nv.numeric_value) AS total_varnish_pct,
    AVG(nv.numeric_value) AS avg_varnish_pct,
    COUNT(DISTINCT r.run_id) AS run_count
FROM runs r
-- Solvent type için JOIN
JOIN runid_stringvalues sv ON r.run_id = sv.run_id
JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
    AND sc.column_name = 'solvent_type'
-- Varnish percentage için JOIN
JOIN runid_numericvalues nv ON r.run_id = nv.run_id
JOIN numericcols nc ON nv.numericcol_id = nc.numericcol_id
    AND nc.column_name = 'varnish_pct'
WHERE nv.numeric_value IS NOT NULL
GROUP BY sv.string_value
ORDER BY total_varnish_pct DESC;


-- ============================================================================
-- BONUS: GENEL QUERY PATTERN
-- ============================================================================
/*
EAV modelinde tüm sorgular aşağıdaki pattern'i takip eder:

1. runs tablosundan başla
2. Her STRING kolon için:
   - runid_stringvalues'a JOIN
   - stringcols'a JOIN (column_name ile filtrele)
3. Her NUMERIC kolon için:
   - runid_numericvalues'a JOIN
   - numericcols'a JOIN (column_name ile filtrele)
4. GROUP BY, ORDER BY, WHERE ekle

TEMPLATE:
--------
SELECT 
    sv.string_value AS kolon_adi,
    AGG(nv.numeric_value) AS sonuc
FROM runs r
JOIN runid_stringvalues sv ON r.run_id = sv.run_id
JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
    AND sc.column_name = 'KOLON_ADI'
JOIN runid_numericvalues nv ON r.run_id = nv.run_id
JOIN numericcols nc ON nv.numericcol_id = nc.numericcol_id
    AND nc.column_name = 'NUMERIC_KOLON'
WHERE nv.numeric_value IS NOT NULL
GROUP BY sv.string_value
ORDER BY sonuc DESC;
*/

-- ============================================================================
-- PERFORMANS İPUCU
-- ============================================================================
/*
Eğer sorguların çok yavaş çalışıyorsa:

1. Index'leri kontrol et:
   SHOW INDEX FROM runid_stringvalues;
   SHOW INDEX FROM runid_numericvalues;

2. EXPLAIN kullan:
   EXPLAIN SELECT ...;

3. Sık kullanılan column_name'ler için covering index ekle:
   CREATE INDEX idx_stringcol_customer 
   ON runid_stringvalues(stringcol_id, run_id) 
   WHERE stringcol_id = (SELECT stringcol_id FROM stringcols WHERE column_name = 'customer');
*/