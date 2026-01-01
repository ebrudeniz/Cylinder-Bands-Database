#Bir run'ın tüm string değerlerini göster:
SELECT * FROM v_string_data WHERE run_id = 1;

#Bir run'ın tüm numeric değerlerini göster:
SELECT * FROM v_numeric_data WHERE run_id = 1;

#Bir run'ın tüm verilerini göster (long format):
SELECT * FROM v_all_data WHERE run_id = 1 ORDER BY column_name;

#Orijinal tablo formatında göster:
SELECT * FROM v_production_runs WHERE run_id = 1;

#Customer değerini bul:
SELECT rv.string_value
FROM runid_stringvalues rv
JOIN stringcols sc ON rv.stringcol_id = sc.stringcol_id
WHERE rv.run_id = 1 AND sc.column_name = 'customer';

#Proof_cut değerini bul:
SELECT rv.numeric_value
FROM runid_numericvalues rv
JOIN numericcols nc ON rv.numericcol_id = nc.numericcol_id
WHERE rv.run_id = 1 AND nc.column_name = 'proof_cut';

#Band oluşan run'ları bul:
SELECT DISTINCT rv.run_id
FROM runid_stringvalues rv
JOIN stringcols sc ON rv.stringcol_id = sc.stringcol_id
WHERE sc.column_name = 'band_type' AND rv.string_value = 'band';