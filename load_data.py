import pandas as pd
import mysql.connector
from datetime import datetime


# MYSQL Connection
def connect_to_db():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            port=3306,
            user='root',
            password='***CHANGE_THIS***',
            database='cylinder_bands_db'
        )
        print("✓ Veritabanına bağlanıldı")
        return connection
    except Exception as e:
        print(f"✗ Bağlantı hatası: {e}")
        return None



def load_and_clean_csv(filepath):
    """CSV'yi oku ve temizle"""
    print("\n📂 CSV yükleniyor...")
    df = pd.read_csv("/home/ebru/Desktop/database_project/data/cylinder_band.csv")
    
    # Veri temizleme
    print("🧹 Veri temizleniyor...")
    
    if 'humifity' in df.columns:
        df.rename(columns={'humifity': 'humidity'}, inplace=True)
        print("✓ 'humifity' → 'humidity' düzeltildi")

    # Customer typo'larını düzelt
    customer_mapping = {
        'abbey': 'Abbey Press',
        'abbeypress': 'Abbey Press',
        'abbypress': 'Abbey Press',
        'best' : 'bestprod',
        'hanhouse': 'Hanover House',
        'hanoverhouse': 'Hanover House',
        'hanovrhous': 'Hanover House',
        'homeshop': 'Home Shopping',
        'homeshopping': 'Home Shopping',
        'jcp': 'JCPenney',
        'jcpenny': 'JCPenney',
        'penney': 'JCPenney',
        'casualliving': 'Casual Living',
        'casliving': 'Casual Living',
        'global': 'Global Equipment',
        'globalequp': 'Global Equipment'
    }
    
    df['customer'] = df['customer'].replace(customer_mapping)
    
    # Paper_mill_location typo düzelt
    df['paper_mill_location'] = df['paper_mill_location'].replace({
        'scandanavian': 'scandinavian'
    })
    
    # Duplicate'leri sil
    df = df.drop_duplicates(subset=['timestamp', 'cylinder_number'], keep='first')
    
    print(f"✓ Temizleme tamamlandı. Toplam kayıt: {len(df)}")

    output_path = '/home/ebru/Desktop/database_project/data/cleaned_cylinder_band.csv'
    df.to_csv(output_path, index=False)
    
    print(f"✅ Temizlenmiş CSV kaydedildi: {output_path}")
    return df



def categorize_columns(df):
    """Sütunları string ve numeric olarak ayır"""
    string_cols = []
    numeric_cols = []
    
    for col in df.columns:
        if col == 'timestamp':
            continue  # runs tablosunda
        
        dtype = df[col].dtype
        if dtype == 'object':
            string_cols.append(col)
        elif dtype in ['int64', 'float64']:
            # Boolean gibi görünenler -> string
            if col in ['grain_screened', 'proof_on_ctd_ink', 'direct_steam', 'type_on_cylinder']:
                string_cols.append(col)
            # Integer ama string gibi saklanacak
            elif col in ['job_number', 'press', 'unit_number', 'plating_tank']:
                string_cols.append(col)
            else:
                numeric_cols.append(col)
    
    print(f"\n📋 String sütunlar: {len(string_cols)}")
    print(f"📋 Numeric sütunlar: {len(numeric_cols)}")
    
    return string_cols, numeric_cols



def populate_metadata_tables(connection, string_cols, numeric_cols):
    """stringcols ve numericcols tablolarını doldur"""
    cursor = connection.cursor()
    
    print("\n📝 Metadata tabloları dolduruluyor...")
    
    # stringcols tablosunu doldur
    for col in string_cols:
        try:
            cursor.execute(
                "INSERT INTO stringcols (column_name) VALUES (%s)",
                (col,)
            )
        except mysql.connector.IntegrityError:
            pass  # Zaten varsa devam et
    
    # numericcols tablosunu doldur
    for col in numeric_cols:
        try:
            cursor.execute(
                "INSERT INTO numericcols (column_name) VALUES (%s)",
                (col,)
            )
        except mysql.connector.IntegrityError:
            pass  # Zaten varsa devam et
    
    connection.commit()
    print("✓ Metadata tabloları dolduruldu")
    
    # ID'leri cache'e al
    cursor.execute("SELECT stringcol_id, column_name FROM stringcols")
    string_col_ids = {row[1]: row[0] for row in cursor.fetchall()}
    
    cursor.execute("SELECT numericcol_id, column_name FROM numericcols")
    numeric_col_ids = {row[1]: row[0] for row in cursor.fetchall()}
    
    return string_col_ids, numeric_col_ids




def populate_runs_table(connection, df):
    """runs tablosunu doldur"""
    cursor = connection.cursor()
    
    print("\n🏃 Runs tablosu dolduruluyor...")
    
    for idx, row in df.iterrows():
        cursor.execute(
            "INSERT INTO runs (timestamp) VALUES (%s)",
            (row['timestamp'],)
        )
    
    connection.commit()
    print(f"✓ {len(df)} kayıt runs tablosuna eklendi")

# ============================================================================
# 6. ADIM: String Değerlerini Yükle
# ============================================================================

def populate_string_values(connection, df, string_cols, string_col_ids):
    """runid_stringvalues tablosunu doldur"""
    cursor = connection.cursor()
    
    print("\n📝 String değerler yükleniyor...")
    
    total = 0
    for run_id, (idx, row) in enumerate(df.iterrows(), start=1):
        for col in string_cols:
            value = row[col]
            
            # NULL kontrolü - NULL ise EKLEME!
            if pd.isna(value):
                continue  # ← SKIP!
            
            value = str(value)
            stringcol_id = string_col_ids[col]
            
            cursor.execute(
                "INSERT INTO runid_stringvalues (run_id, stringcol_id, string_value) VALUES (%s, %s, %s)",
                (run_id, stringcol_id, value)
            )
            total += 1
            
            if total % 1000 == 0:
                print(f"  {total} string değer yüklendi...")
                connection.commit()
    
    connection.commit()
    print(f"✓ Toplam {total} string değer yüklendi (NULL'lar atlandı)")

# ============================================================================
# 7. ADIM: Numeric Değerlerini Yükle
# ============================================================================

def populate_numeric_values(connection, df, numeric_cols, numeric_col_ids):
    """runid_numericvalues tablosunu doldur"""
    cursor = connection.cursor()
    
    print("\n🔢 Numeric değerler yükleniyor...")
    
    total = 0
    for run_id, (idx, row) in enumerate(df.iterrows(), start=1):
        for col in numeric_cols:
            value = row[col]
            
            # NULL kontrolü - NULL ise EKLEME!
            if pd.isna(value):
                continue  # ← SKIP!
            
            value = float(value)
            numericcol_id = numeric_col_ids[col]
            
            cursor.execute(
                "INSERT INTO runid_numericvalues (run_id, numericcol_id, numeric_value) VALUES (%s, %s, %s)",
                (run_id, numericcol_id, value)
            )
            total += 1
            
            if total % 1000 == 0:
                print(f"  {total} numeric değer yüklendi...")
                connection.commit()
    
    connection.commit()
    print(f"✓ Toplam {total} numeric değer yüklendi (NULL'lar atlandı)")

# ============================================================================
# 8. ADIM: Doğrulama
# ============================================================================

def verify_data(connection):
    """Yüklenen veriyi doğrula"""
    cursor = connection.cursor()
    
    print("\n✅ Doğrulama yapılıyor...")
    
    cursor.execute("SELECT COUNT(*) FROM runs")
    run_count = cursor.fetchone()[0]
    print(f"  Runs: {run_count} kayıt")
    
    cursor.execute("SELECT COUNT(*) FROM stringcols")
    stringcol_count = cursor.fetchone()[0]
    print(f"  String sütunlar: {stringcol_count}")
    
    cursor.execute("SELECT COUNT(*) FROM numericcols")
    numericcol_count = cursor.fetchone()[0]
    print(f"  Numeric sütunlar: {numericcol_count}")
    
    cursor.execute("SELECT COUNT(*) FROM runid_stringvalues")
    string_value_count = cursor.fetchone()[0]
    print(f"  String değerler: {string_value_count}")
    
    cursor.execute("SELECT COUNT(*) FROM runid_numericvalues")
    numeric_value_count = cursor.fetchone()[0]
    print(f"  Numeric değerler: {numeric_value_count}")
    
    # Örnek sorgu
    print("\n📊 Örnek veri:")
    cursor.execute("""
        SELECT r.run_id, r.timestamp, sc.column_name, sv.string_value
        FROM runs r
        JOIN runid_stringvalues sv ON r.run_id = sv.run_id
        JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
        WHERE r.run_id = 1 AND sc.column_name = 'customer'
    """)
    result = cursor.fetchone()
    if result:
        print(f"  Run 1 -> customer: {result[3]}")

# ============================================================================
# ANA FONKSİYON
# ============================================================================

def main():
    """Ana yükleme fonksiyonu"""
    print("="*80)
    print("CSV -> METADATA-DRIVEN ER DİYAGRAM VERİ YÜKLEME")
    print("="*80)
    
    # CSV dosya yolu
    csv_path = '/mnt/user-data/uploads/cleaned_cylinder.csv'
    
    # 1. MySQL'e bağlan
    connection = connect_to_db()
    if not connection:
        return
    
    try:
        # 2. CSV'yi yükle ve temizle
        df = load_and_clean_csv(csv_path)
        
        # 3. Sütunları kategorize et
        string_cols, numeric_cols = categorize_columns(df)
        
        # 4. Metadata tablolarını doldur
        string_col_ids, numeric_col_ids = populate_metadata_tables(
            connection, string_cols, numeric_cols
        )
        
        # 5. Runs tablosunu doldur
        populate_runs_table(connection, df)
        
        # 6. String değerlerini yükle
        populate_string_values(connection, df, string_cols, string_col_ids)
        
        # 7. Numeric değerlerini yükle
        populate_numeric_values(connection, df, numeric_cols, numeric_col_ids)
        
        # 8. Doğrulama
        verify_data(connection)
        
        print("\n" + "="*80)
        print("✅ VERİ YÜKLEME TAMAMLANDI!")
        print("="*80)
        
    except Exception as e:
        print(f"\n❌ HATA: {e}")
        connection.rollback()
    
    finally:
        connection.close()
        print("\n👋 Bağlantı kapatıldı")

if __name__ == "__main__":
    main()




