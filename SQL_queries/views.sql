use cylinder_bands_db;
-- View 1: String değerleri göster (sütun isimleri ile)
CREATE VIEW v_string_data AS
SELECT 
    rv.run_id,
    r.timestamp,
    sc.column_name,
    rv.string_value
FROM runid_stringvalues rv
JOIN runs r ON rv.run_id = r.run_id
JOIN stringcols sc ON rv.stringcol_id = sc.stringcol_id;

-- View 2: Numeric değerleri göster (sütun isimleri ile)
CREATE VIEW v_numeric_data AS
SELECT 
    rv.run_id,
    r.timestamp,
    nc.column_name,
    rv.numeric_value
FROM runid_numericvalues rv
JOIN runs r ON rv.run_id = r.run_id
JOIN numericcols nc ON rv.numericcol_id = nc.numericcol_id;

-- View 3: Tüm değerleri birleştir (UNION)
CREATE VIEW v_all_data AS
SELECT 
    run_id,
    timestamp,
    column_name,
    string_value AS value,
    'string' AS data_type
FROM v_string_data

UNION ALL

SELECT 
    run_id,
    timestamp,
    column_name,
    CAST(numeric_value AS CHAR) AS value,
    'numeric' AS data_type
FROM v_numeric_data;

-- View 4: Pivot format (orijinal tablo gibi)
CREATE VIEW v_production_runs AS
SELECT 
    r.run_id,
    r.timestamp,
    
    -- String sütunlar
    MAX(CASE WHEN sc.column_name = 'cylinder_number' THEN sv.string_value END) AS cylinder_number,
    MAX(CASE WHEN sc.column_name = 'customer' THEN sv.string_value END) AS customer,
    MAX(CASE WHEN sc.column_name = 'band_type' THEN sv.string_value END) AS band_type,
    MAX(CASE WHEN sc.column_name = 'job_number' THEN sv.string_value END) AS job_number,
    MAX(CASE WHEN sc.column_name = 'grain_screened' THEN sv.string_value END) AS grain_screened,
    MAX(CASE WHEN sc.column_name = 'proof_on_ctd_ink' THEN sv.string_value END) AS proof_on_ctd_ink,
    MAX(CASE WHEN sc.column_name = 'blade_mfg' THEN sv.string_value END) AS blade_mfg,
    MAX(CASE WHEN sc.column_name = 'paper_type' THEN sv.string_value END) AS paper_type,
    MAX(CASE WHEN sc.column_name = 'ink_type' THEN sv.string_value END) AS ink_type,
    MAX(CASE WHEN sc.column_name = 'direct_steam' THEN sv.string_value END) AS direct_steam,
    MAX(CASE WHEN sc.column_name = 'solvent_type' THEN sv.string_value END) AS solvent_type,
    MAX(CASE WHEN sc.column_name = 'type_on_cylinder' THEN sv.string_value END) AS type_on_cylinder,
    MAX(CASE WHEN sc.column_name = 'press_type' THEN sv.string_value END) AS press_type,
    MAX(CASE WHEN sc.column_name = 'press' THEN sv.string_value END) AS press,
    MAX(CASE WHEN sc.column_name = 'unit_number' THEN sv.string_value END) AS unit_number,
    MAX(CASE WHEN sc.column_name = 'cylinder_size' THEN sv.string_value END) AS cylinder_size,
    MAX(CASE WHEN sc.column_name = 'paper_mill_location' THEN sv.string_value END) AS paper_mill_location,
    MAX(CASE WHEN sc.column_name = 'plating_tank' THEN sv.string_value END) AS plating_tank,
    
    -- Numeric sütunlar
    MAX(CASE WHEN nc.column_name = 'proof_cut' THEN nv.numeric_value END) AS proof_cut,
    MAX(CASE WHEN nc.column_name = 'viscosity' THEN nv.numeric_value END) AS viscosity,
    MAX(CASE WHEN nc.column_name = 'caliper' THEN nv.numeric_value END) AS caliper,
    MAX(CASE WHEN nc.column_name = 'ink_temperature' THEN nv.numeric_value END) AS ink_temperature,
    MAX(CASE WHEN nc.column_name = 'humifity' THEN nv.numeric_value END) AS humifity,
    MAX(CASE WHEN nc.column_name = 'roughness' THEN nv.numeric_value END) AS roughness,
    MAX(CASE WHEN nc.column_name = 'blade_pressure' THEN nv.numeric_value END) AS blade_pressure,
    MAX(CASE WHEN nc.column_name = 'varnish_pct' THEN nv.numeric_value END) AS varnish_pct,
    MAX(CASE WHEN nc.column_name = 'press_speed' THEN nv.numeric_value END) AS press_speed,
    MAX(CASE WHEN nc.column_name = 'ink_pct' THEN nv.numeric_value END) AS ink_pct,
    MAX(CASE WHEN nc.column_name = 'solvent_pct' THEN nv.numeric_value END) AS solvent_pct,
    MAX(CASE WHEN nc.column_name = 'ESA_Voltage' THEN nv.numeric_value END) AS ESA_Voltage,
    MAX(CASE WHEN nc.column_name = 'ESA_Amperage' THEN nv.numeric_value END) AS ESA_Amperage,
    MAX(CASE WHEN nc.column_name = 'wax' THEN nv.numeric_value END) AS wax,
    MAX(CASE WHEN nc.column_name = 'hardener' THEN nv.numeric_value END) AS hardener,
    MAX(CASE WHEN nc.column_name = 'roller_durometer' THEN nv.numeric_value END) AS roller_durometer,
    MAX(CASE WHEN nc.column_name = 'current_density' THEN nv.numeric_value END) AS current_density,
    MAX(CASE WHEN nc.column_name = 'anode_space_ratio' THEN nv.numeric_value END) AS anode_space_ratio,
    MAX(CASE WHEN nc.column_name = 'chrome_content' THEN nv.numeric_value END) AS chrome_content

FROM runs r
LEFT JOIN runid_stringvalues sv ON r.run_id = sv.run_id
LEFT JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
LEFT JOIN runid_numericvalues nv ON r.run_id = nv.run_id
LEFT JOIN numericcols nc ON nv.numericcol_id = nc.numericcol_id
GROUP BY r.run_id, r.timestamp;


