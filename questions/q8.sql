USE cylinder_bands_db;
-- ============================================================================
-- SORU 8: Find the total number of production runs grouped by month
-- ============================================================================
-- İhtiyacımız olan kolonlar:
--   - timestamp (runs tablosunda direkt var!)
-- NOT: Bu soru çok basit çünkü timestamp zaten runs tablosunda!

SELECT 
    DATE_FORMAT(timestamp, '%Y-%m') AS month,
    YEAR(timestamp) AS year,
    MONTH(timestamp) AS month_num,
    COUNT(*) AS total_runs
FROM runs
GROUP BY DATE_FORMAT(timestamp, '%Y-%m'), YEAR(timestamp), MONTH(timestamp)
ORDER BY year, month_num;

-- Alternatif: Sadece ay adı ile
SELECT 
    DATE_FORMAT(timestamp, '%Y-%m') AS month,
    MONTHNAME(timestamp) AS month_name,
    COUNT(*) AS total_runs
FROM runs
GROUP BY DATE_FORMAT(timestamp, '%Y-%m'), MONTHNAME(timestamp)
ORDER BY DATE_FORMAT(timestamp, '%Y-%m');

