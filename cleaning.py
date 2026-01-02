import pandas as pd
import numpy as np
import difflib

def execute_senior_db_cleaning(input_file, output_csv, report_file):
    """
    Veritabanı yöneticisi perspektifiyle 1NF hazırlık ve veri temizleme süreci.
    """
    # Veriyi Yükle
    df = pd.read_csv(input_file)
    report = [
        "SENIOR DATABASE MANAGER - VERI TEMIZLEME VE ŞEMA OPTIMIZASYON RAPORU",
        "="*70,
        f"Girdi Dosyası: {input_file}",
        f"Başlangıç Satır Sayısı: {len(df)}",
        f"Başlangıç Sütun Sayısı: {len(df.columns)}",
        "-"*70,
        ""
    ]

    # PHASE 1: STANDART NULL YÖNETIMI
    initial_null_placeholders = (df == '?').sum().sum()
    df.replace('?', np.nan, inplace=True)
    report.append(f"[NULL_MGT] {initial_null_placeholders} adet '?' karakteri NaN yapıldı.")

    # PHASE 2: ZAMANSAL VERI DÖNÜŞÜMÜ
    if 'timestamp' in df.columns:
        # YYYYMMDD formatından Date formatına
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y%m%d', errors='coerce')
        report.append("[SCHEMA] 'timestamp' sütunu Integer -> Date (YYYY-MM-DD) formatına çevrildi.")

    # PHASE 3: SABİT (CONSTANT) SÜTUNLARIN ELENMESİ
    constant_cols = [col for col in df.columns if df[col].nunique(dropna=True) <= 1]
    if constant_cols:
        df.drop(columns=constant_cols, inplace=True)
        report.append(f"[SCHEMA] Varyasyon içermeyen sabit sütunlar silindi: {constant_cols}")

    # PHASE 4: METİN NORMALİZASYONU
    object_cols = df.select_dtypes(include=['object']).columns
    for col in object_cols:
        df[col] = df[col].astype(str).str.lower().str.strip()
    df.replace('nan', np.nan, inplace=True) # string dönüşümü sonrası oluşan 'nan'ları düzelt
    report.append("[TEXT_NORM] Tüm metinler küçük harfe çevrildi ve boşluklar temizlendi.")

    # PHASE 5: ODAKLANMIŞ TYPO DÜZELTME (Sadece Customer)
    if 'customer' in df.columns:
        report.append("[TYPO_CLEAN] 'customer' sütunu üzerinde Fuzzy Matching çalıştırılıyor...")
        counts = df['customer'].value_counts()
        unique_customers = counts.index.tolist()
        replacements = {}
        
        for i, val in enumerate(unique_customers):
            if val in replacements.values() or pd.isna(val): continue
            
            # %85 benzerlik eşiği ile hataları yakala
            matches = difflib.get_close_matches(val, unique_customers[i+1:], n=5, cutoff=0.85)
            for match in matches:
                if counts[val] > counts[match]: # Sık kullanılan doğru kabul edilir
                    replacements[match] = val
                    report.append(f"  - Typo Düzeltildi: '{match}' -> '{val}'")
        
        if replacements:
            df['customer'] = df['customer'].replace(replacements)

    # PHASE 6: VERİ TİPİ OPTİMİZASYONU (Schema Management)
    for col in df.columns:
        # A. Metin olarak kalmış sayısal sütunları zorla
        if df[col].dtype == 'object':
            converted = pd.to_numeric(df[col], errors='coerce')
            if converted.notnull().sum() / len(df) > 0.5:
                df[col] = converted
                report.append(f"[SCHEMA] '{col}' sütunu sayısal tipe (Float) zorlandı.")

        # B. Ondalıksız Floatları Integer'a Çevir (Bellek ve SQL Uyumu)
        if df[col].dtype == 'float64':
            non_null = df[col].dropna()
            if len(non_null) > 0 and all(x.is_integer() for x in non_null):
                df[col] = df[col].astype('Int64')
                report.append(f"[OPTIMIZE] '{col}' sütunu Float -> Nullable Integer (Int64) yapıldı.")

    # C. Binary Standardizasyonu (yes/no -> Boolean)
    binary_map = {'yes': True, 'no': False, 'true': True, 'false': False}
    for col in df.select_dtypes(include=['object']).columns:
        unique_vals = set(df[col].dropna().unique())
        if unique_vals.issubset(set(binary_map.keys())) and len(unique_vals) > 0:
            df[col] = df[col].map(binary_map).astype('boolean')
            report.append(f"[SCHEMA] '{col}' sütunu Boolean tipine sabitlendi.")

    # PHASE 7: MÜKERRER KAYIT TEMİZLİĞİ (Deduplication)
    initial_count = len(df)
    df.drop_duplicates(inplace=True)
    if len(df) != initial_count:
        report.append(f"[CLEANUP] {initial_count - len(df)} adet mükerrer satır silindi.")

    # PHASE 8: ÇIKTI VE RAPORLAMA
    df.to_csv(output_csv, index=False)
    report.append("\n" + "="*70)
    report.append(f"FINAL ÖZET:")
    report.append(f"Son Satır Sayısı: {len(df)}")
    report.append(f"Son Sütun Sayısı: {len(df.columns)}")
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("\n".join(report))

    return "İşlem başarıyla tamamlandı. Dosyalar: cleaned_cylinder.csv, cleaning_report.txt"

# Kodu Çalıştır
print(execute_senior_db_cleaning('cylinder.csv', 'cleaned_cylinder.csv', 'cleaning_report.txt'))
