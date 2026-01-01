USE cylinder_bands_db;

-- ============================================================================
-- SORU 3: Find the number of production runs performed by each customer
-- ============================================================================
-- İhtiyacımız olan kolonlar:
--   - customer (string)
-- NOT: Sadece count gerektiği için numeric JOIN'e gerek yok!

SELECT 
    sv.string_value AS customer,
    COUNT(DISTINCT r.run_id) AS total_runs
FROM runs r
-- Customer için JOIN
JOIN runid_stringvalues sv ON r.run_id = sv.run_id
JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
    AND sc.column_name = 'customer'
GROUP BY sv.string_value
ORDER BY total_runs DESC;

