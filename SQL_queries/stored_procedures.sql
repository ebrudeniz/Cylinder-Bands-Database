-- ============================================================================
-- STORED PROCEDURES - IN, OUT, INOUT Examples
-- ============================================================================

-- ============================================================================
-- 1. IN PARAMETER EXAMPLE
-- Get all data for a specific run
-- ============================================================================
DELIMITER $$

CREATE PROCEDURE get_run_details(IN p_run_id INT)
BEGIN
    -- Get run timestamp
    SELECT run_id, timestamp FROM runs WHERE run_id = p_run_id;
    
    -- Get string values
    SELECT sc.column_name, sv.string_value
    FROM runid_stringvalues sv
    JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
    WHERE sv.run_id = p_run_id;
    
    -- Get numeric values
    SELECT nc.column_name, nv.numeric_value
    FROM runid_numericvalues nv
    JOIN numericcols nc ON nv.numericcol_id = nc.numericcol_id
    WHERE nv.run_id = p_run_id;
END$$

DELIMITER ;

-- Usage: CALL get_run_details(1);

-- ============================================================================
-- 2. OUT PARAMETER EXAMPLE
-- Count band types and return counts
-- ============================================================================
DELIMITER $$

CREATE PROCEDURE count_band_types(
    OUT band_count INT,
    OUT noband_count INT
)
BEGIN
    SELECT COUNT(*) INTO band_count
    FROM runid_stringvalues sv
    JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
    WHERE sc.column_name = 'band_type' AND sv.string_value = 'band';
    
    SELECT COUNT(*) INTO noband_count
    FROM runid_stringvalues sv
    JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
    WHERE sc.column_name = 'band_type' AND sv.string_value = 'noband';
END$$

DELIMITER ;

-- Usage: 
-- CALL count_band_types(@band, @noband);
-- SELECT @band, @noband;

-- ============================================================================
-- 3. INOUT PARAMETER EXAMPLE
-- Get customer run count (pass customer name, return count)
-- ============================================================================
DELIMITER $$

CREATE PROCEDURE get_customer_run_count(
    INOUT customer_name VARCHAR(100)
)
BEGIN
    DECLARE run_count INT;
    
    SELECT COUNT(*) INTO run_count
    FROM runid_stringvalues sv
    JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
    WHERE sc.column_name = 'customer' 
    AND sv.string_value = customer_name;
    
    -- Return format: "customer_name: X runs"
    SET customer_name = CONCAT(customer_name, ': ', run_count, ' runs');
END$$

DELIMITER ;

-- Usage:
-- SET @customer = 'kmart';
-- CALL get_customer_run_count(@customer);
-- SELECT @customer;

-- ============================================================================
-- 4. MIXED PARAMETERS EXAMPLE
-- Search runs by date range and return statistics
-- ============================================================================
DELIMITER $$

CREATE PROCEDURE search_runs_by_date(
    IN start_date DATE,
    IN end_date DATE,
    OUT total_runs INT,
    OUT band_percentage DECIMAL(5,2)
)
BEGIN
    -- Count total runs in date range
    SELECT COUNT(*) INTO total_runs
    FROM runs
    WHERE timestamp BETWEEN start_date AND end_date;
    
    -- Calculate band percentage
    SELECT 
        ROUND(
            (SUM(CASE WHEN sv.string_value = 'band' THEN 1 ELSE 0 END) / COUNT(*)) * 100,
            2
        ) INTO band_percentage
    FROM runs r
    JOIN runid_stringvalues sv ON r.run_id = sv.run_id
    JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
    WHERE r.timestamp BETWEEN start_date AND end_date
    AND sc.column_name = 'band_type';
    
    -- Also return the runs
    SELECT r.run_id, r.timestamp
    FROM runs r
    WHERE r.timestamp BETWEEN start_date AND end_date
    ORDER BY r.timestamp;
END$$

DELIMITER ;

-- Usage:
-- CALL search_runs_by_date('1991-01-01', '1991-12-31', @total, @band_pct);
-- SELECT @total, @band_pct;

-- ============================================================================
-- 5. COMPLEX PROCEDURE - Analytics
-- Get customer performance report
-- ============================================================================
DELIMITER $$

CREATE PROCEDURE get_customer_report(
    IN p_customer_name VARCHAR(100),
    OUT total_runs INT,
    OUT band_runs INT,
    OUT band_rate DECIMAL(5,2)
)
BEGIN
    -- Get total runs for this customer
    SELECT COUNT(DISTINCT r.run_id) INTO total_runs
    FROM runs r
    JOIN runid_stringvalues sv ON r.run_id = sv.run_id
    JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
    WHERE sc.column_name = 'customer' 
    AND sv.string_value = p_customer_name;
    
    -- Get band runs
    SELECT COUNT(DISTINCT r.run_id) INTO band_runs
    FROM runs r
    JOIN runid_stringvalues sv1 ON r.run_id = sv1.run_id
    JOIN stringcols sc1 ON sv1.stringcol_id = sc1.stringcol_id
    JOIN runid_stringvalues sv2 ON r.run_id = sv2.run_id
    JOIN stringcols sc2 ON sv2.stringcol_id = sc2.stringcol_id
    WHERE sc1.column_name = 'customer' AND sv1.string_value = p_customer_name
    AND sc2.column_name = 'band_type' AND sv2.string_value = 'band';
    
    -- Calculate band rate
    IF total_runs > 0 THEN
        SET band_rate = ROUND((band_runs / total_runs) * 100, 2);
    ELSE
        SET band_rate = 0;
    END IF;
    
    -- Return detailed runs
    SELECT 
        r.run_id,
        r.timestamp,
        sv_band.string_value as band_type
    FROM runs r
    JOIN runid_stringvalues sv_cust ON r.run_id = sv_cust.run_id
    JOIN stringcols sc_cust ON sv_cust.stringcol_id = sc_cust.stringcol_id
    LEFT JOIN runid_stringvalues sv_band ON r.run_id = sv_band.run_id
    LEFT JOIN stringcols sc_band ON sv_band.stringcol_id = sc_band.stringcol_id
        AND sc_band.column_name = 'band_type'
    WHERE sc_cust.column_name = 'customer'
    AND sv_cust.string_value = p_customer_name
    ORDER BY r.timestamp;
END$$

DELIMITER ;

-- Usage:
-- CALL get_customer_report('kmart', @total, @band, @rate);
-- SELECT @total AS 'Total Runs', @band AS 'Band Runs', @rate AS 'Band Rate %';

-- ============================================================================
-- 6. UTILITY PROCEDURE - Delete old runs
-- ============================================================================
DELIMITER $$

CREATE PROCEDURE delete_runs_before_date(
    IN cutoff_date DATE,
    OUT deleted_count INT
)
BEGIN
    -- Count runs to be deleted
    SELECT COUNT(*) INTO deleted_count
    FROM runs
    WHERE timestamp < cutoff_date;
    
    -- Delete (CASCADE will handle related records)
    DELETE FROM runs
    WHERE timestamp < cutoff_date;
    
    SELECT CONCAT('Deleted ', deleted_count, ' runs before ', cutoff_date) AS result;
END$$

DELIMITER ;

-- Usage:
-- CALL delete_runs_before_date('1991-01-01', @deleted);
-- SELECT @deleted;

-- ============================================================================
-- ADDITIONAL PROCEDURES FOR INTERFACE
-- ============================================================================

-- ============================================================================
-- Get filtered runs list
-- ============================================================================
DELIMITER $$

CREATE PROCEDURE get_runs_list(
    IN p_date_from DATE,
    IN p_date_to DATE,
    IN p_customer VARCHAR(100),
    IN p_band_type VARCHAR(50),
    IN p_sort_by VARCHAR(50),
    IN p_sort_order VARCHAR(4)
)
BEGIN
    SET @query = 'SELECT DISTINCT r.run_id, r.timestamp FROM runs r';
    SET @where_clause = '';
    SET @join_customer = '';
    SET @join_band = '';
    
    -- Build JOINs
    IF p_customer IS NOT NULL AND p_customer != '' THEN
        SET @join_customer = ' JOIN runid_stringvalues sv_cust ON r.run_id = sv_cust.run_id
            JOIN stringcols sc_cust ON sv_cust.stringcol_id = sc_cust.stringcol_id';
    END IF;
    
    IF p_band_type IS NOT NULL AND p_band_type != '' THEN
        SET @join_band = ' JOIN runid_stringvalues sv_band ON r.run_id = sv_band.run_id
            JOIN stringcols sc_band ON sv_band.stringcol_id = sc_band.stringcol_id';
    END IF;
    
    SET @query = CONCAT(@query, @join_customer, @join_band);
    
    -- Build WHERE
    SET @conditions = '';
    
    IF p_date_from IS NOT NULL THEN
        SET @conditions = CONCAT(@conditions, ' r.timestamp >= ''', p_date_from, '''');
    END IF;
    
    IF p_date_to IS NOT NULL THEN
        IF @conditions != '' THEN
            SET @conditions = CONCAT(@conditions, ' AND');
        END IF;
        SET @conditions = CONCAT(@conditions, ' r.timestamp <= ''', p_date_to, '''');
    END IF;
    
    IF p_customer IS NOT NULL AND p_customer != '' THEN
        IF @conditions != '' THEN
            SET @conditions = CONCAT(@conditions, ' AND');
        END IF;
        SET @conditions = CONCAT(@conditions, ' sc_cust.column_name = ''customer'' AND sv_cust.string_value LIKE ''%', p_customer, '%''');
    END IF;
    
    IF p_band_type IS NOT NULL AND p_band_type != '' THEN
        IF @conditions != '' THEN
            SET @conditions = CONCAT(@conditions, ' AND');
        END IF;
        SET @conditions = CONCAT(@conditions, ' sc_band.column_name = ''band_type'' AND sv_band.string_value = ''', p_band_type, '''');
    END IF;
    
    IF @conditions != '' THEN
        SET @query = CONCAT(@query, ' WHERE', @conditions);
    END IF;
    
    -- Add ORDER BY
    IF p_sort_by IS NOT NULL AND p_sort_order IS NOT NULL THEN
        SET @query = CONCAT(@query, ' ORDER BY r.', p_sort_by, ' ', p_sort_order);
    ELSE
        SET @query = CONCAT(@query, ' ORDER BY r.run_id DESC');
    END IF;
    
    -- Execute
    PREPARE stmt FROM @query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END$$

DELIMITER ;

-- ============================================================================
-- Quick search by run_id or customer
-- ============================================================================
DELIMITER $$

CREATE PROCEDURE quick_search(
    IN search_query VARCHAR(100),
    OUT search_type VARCHAR(50)
)
BEGIN
    -- Check if numeric (run_id)
    IF search_query REGEXP '^[0-9]+$' THEN
        SET search_type = 'Run ID';
        SELECT * FROM runs WHERE run_id = CAST(search_query AS UNSIGNED);
    ELSE
        -- Search by customer
        SET search_type = 'Customer Name';
        SELECT DISTINCT r.run_id, r.timestamp, sv.string_value as customer
        FROM runs r
        JOIN runid_stringvalues sv ON r.run_id = sv.run_id
        JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
        WHERE sc.column_name = 'customer' 
        AND sv.string_value LIKE CONCAT('%', search_query, '%')
        ORDER BY r.run_id
        LIMIT 50;
    END IF;
END$$

DELIMITER ;

-- ============================================================================
-- Search by string attribute
-- ============================================================================
DELIMITER $$

CREATE PROCEDURE search_string_attribute(
    IN p_column_name VARCHAR(50),
    IN p_search_value VARCHAR(200)
)
BEGIN
    SELECT r.run_id, r.timestamp, sv.string_value as value
    FROM runs r
    JOIN runid_stringvalues sv ON r.run_id = sv.run_id
    JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
    WHERE sc.column_name = p_column_name 
    AND sv.string_value LIKE CONCAT('%', p_search_value, '%')
    ORDER BY r.run_id
    LIMIT 100;
END$$

DELIMITER ;

-- ============================================================================
-- Search by numeric attribute (range)
-- ============================================================================
DELIMITER $$

CREATE PROCEDURE search_numeric_attribute(
    IN p_column_name VARCHAR(50),
    IN p_min_value DECIMAL(10,5),
    IN p_max_value DECIMAL(10,5)
)
BEGIN
    IF p_min_value IS NOT NULL AND p_max_value IS NOT NULL THEN
        SELECT r.run_id, r.timestamp, nv.numeric_value as value
        FROM runs r
        JOIN runid_numericvalues nv ON r.run_id = nv.run_id
        JOIN numericcols nc ON nv.numericcol_id = nc.numericcol_id
        WHERE nc.column_name = p_column_name 
        AND nv.numeric_value BETWEEN p_min_value AND p_max_value
        ORDER BY r.run_id
        LIMIT 100;
    ELSEIF p_min_value IS NOT NULL THEN
        SELECT r.run_id, r.timestamp, nv.numeric_value as value
        FROM runs r
        JOIN runid_numericvalues nv ON r.run_id = nv.run_id
        JOIN numericcols nc ON nv.numericcol_id = nc.numericcol_id
        WHERE nc.column_name = p_column_name 
        AND nv.numeric_value >= p_min_value
        ORDER BY r.run_id
        LIMIT 100;
    ELSEIF p_max_value IS NOT NULL THEN
        SELECT r.run_id, r.timestamp, nv.numeric_value as value
        FROM runs r
        JOIN runid_numericvalues nv ON r.run_id = nv.run_id
        JOIN numericcols nc ON nv.numericcol_id = nc.numericcol_id
        WHERE nc.column_name = p_column_name 
        AND nv.numeric_value <= p_max_value
        ORDER BY r.run_id
        LIMIT 100;
    ELSE
        SELECT r.run_id, r.timestamp, nv.numeric_value as value
        FROM runs r
        JOIN runid_numericvalues nv ON r.run_id = nv.run_id
        JOIN numericcols nc ON nv.numericcol_id = nc.numericcol_id
        WHERE nc.column_name = p_column_name
        ORDER BY r.run_id
        LIMIT 100;
    END IF;
END$$

DELIMITER ;

-- ============================================================================
-- Get statistics for dashboard
-- ============================================================================
DELIMITER $$

CREATE PROCEDURE get_statistics(
    OUT total_runs INT,
    OUT band_count INT,
    OUT noband_count INT,
    OUT band_percentage DECIMAL(5,2)
)
BEGIN
    -- Total runs
    SELECT COUNT(*) INTO total_runs FROM runs;
    
    -- Band counts
    SELECT COUNT(*) INTO band_count
    FROM runid_stringvalues sv
    JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
    WHERE sc.column_name = 'band_type' AND sv.string_value = 'band';
    
    SELECT COUNT(*) INTO noband_count
    FROM runid_stringvalues sv
    JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
    WHERE sc.column_name = 'band_type' AND sv.string_value = 'noband';
    
    -- Band percentage
    IF total_runs > 0 THEN
        SET band_percentage = ROUND((band_count / total_runs) * 100, 2);
    ELSE
        SET band_percentage = 0;
    END IF;
    
    -- Also return top cylinder
    SELECT sv.string_value as cylinder, COUNT(*) as count
    FROM runid_stringvalues sv
    JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
    WHERE sc.column_name = 'cylinder_number'
    GROUP BY sv.string_value
    ORDER BY count DESC
    LIMIT 1;
    
    -- Top customers
    SELECT sv.string_value as customer, COUNT(*) as count
    FROM runid_stringvalues sv
    JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
    WHERE sc.column_name = 'customer'
    GROUP BY sv.string_value
    ORDER BY count DESC
    LIMIT 10;
    
    -- Ink distribution
    SELECT sv.string_value as ink_type, COUNT(*) as count
    FROM runid_stringvalues sv
    JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
    WHERE sc.column_name = 'ink_type'
    GROUP BY sv.string_value
    ORDER BY count DESC;
    
    -- Press distribution
    SELECT sv.string_value as press, COUNT(*) as count
    FROM runid_stringvalues sv
    JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
    WHERE sc.column_name = 'press'
    GROUP BY sv.string_value
    ORDER BY count DESC;
    
    -- Avg numeric values
    SELECT 
        AVG(CASE WHEN nc.column_name = 'proof_cut' THEN nv.numeric_value END) as avg_proof_cut,
        AVG(CASE WHEN nc.column_name = 'viscosity' THEN nv.numeric_value END) as avg_viscosity,
        AVG(CASE WHEN nc.column_name = 'wax' THEN nv.numeric_value END) as avg_wax,
        AVG(CASE WHEN nc.column_name = 'hardener' THEN nv.numeric_value END) as avg_hardener
    FROM runid_numericvalues nv
    JOIN numericcols nc ON nv.numericcol_id = nc.numericcol_id;
END$$

DELIMITER ;

-- Usage:
-- CALL get_statistics(@total, @band, @noband, @pct);
-- SELECT @total, @band, @noband, @pct;