DROP DATABASE IF EXISTS cylinder_bands_db;
CREATE DATABASE cylinder_bands_db;
USE cylinder_bands_db;

-- ============================================================================
-- RUNS TABLE - Temel bilgiler
-- ============================================================================
CREATE TABLE runs (
    run_id INT AUTO_INCREMENT PRIMARY KEY,
    timestamp DATE NOT NULL,
    INDEX idx_timestamp (timestamp)  -- Performance için timestamp index
);

-- ============================================================================
-- METADATA TABLES - Sütun isimleri
-- ============================================================================

-- String sütun isimleri
CREATE TABLE stringcols (
    stringcol_id INT AUTO_INCREMENT PRIMARY KEY,
    column_name VARCHAR(50) NOT NULL UNIQUE
);

-- Numeric sütun isimleri
CREATE TABLE numericcols (
    numericcol_id INT AUTO_INCREMENT PRIMARY KEY,
    column_name VARCHAR(50) NOT NULL UNIQUE
);

-- ============================================================================
-- VALUE TABLES - Değerler
-- ============================================================================

-- String değerler
CREATE TABLE runid_stringvalues (
    run_id INT NOT NULL,
    stringcol_id INT NOT NULL,
    string_value VARCHAR(200),
    PRIMARY KEY (run_id, stringcol_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE,
    FOREIGN KEY (stringcol_id) REFERENCES stringcols(stringcol_id) ON DELETE CASCADE
);

-- Numeric değerler
CREATE TABLE runid_numericvalues (
    run_id INT NOT NULL,
    numericcol_id INT NOT NULL,
    numeric_value DECIMAL(10,5),
    PRIMARY KEY (run_id, numericcol_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE,
    FOREIGN KEY (numericcol_id) REFERENCES numericcols(numericcol_id) ON DELETE CASCADE
);

-- ============================================================================
-- ADDITIONAL INDEXES - Query performance optimization
-- ============================================================================

-- String value aramaları için index (opsiyonel - büyük datada faydalı)
-- CREATE INDEX idx_string_value ON runid_stringvalues(string_value(50));

-- Composite index - stringcol_id ile filtreleme ve run_id ile join
CREATE INDEX idx_stringcol_run ON runid_stringvalues(stringcol_id, run_id);

-- Composite index - numericcol_id ile filtreleme ve run_id ile join
CREATE INDEX idx_numericcol_run ON runid_numericvalues(numericcol_id, run_id);
