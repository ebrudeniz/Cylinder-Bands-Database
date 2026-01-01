"""
CYLINDER BANDS DATABASE - FRONTEND USER INTERFACE
Flask web application
"""

from flask import Flask, render_template, request, jsonify
import mysql.connector
import pandas as pd

app = Flask(__name__)

# ============================================================================
# Database Connection
# ============================================================================

def get_db_connection():
    """MySQL bağlantısı"""
    return mysql.connector.connect(
        host='localhost',
            port=3306,
            user='root',
            password='***CHANGE_THIS***',
            database='cylinder_bands_db'
    )

# ============================================================================
# HOME PAGE
# ============================================================================

@app.route('/')
def index():
    """Ana sayfa"""
    return render_template('index.html')

# ============================================================================
# QUICK SEARCH
# ============================================================================

@app.route('/quick-search')
def quick_search():
    """Hızlı arama - Stored Procedure ile"""
    query = request.args.get('q', '').strip()
    
    if not query:
        return redirect('/')
    
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    
    try:
        # STORED PROCEDURE KULLAN!
        cursor.callproc('quick_search', [query, ''])
        
        # İlk result set - runs
        results = []
        for result in cursor.stored_results():
            results = result.fetchall()
        
        # Search type OUT parameter al
        cursor.execute("SELECT @_quick_search_1 AS search_type")
        search_type_row = cursor.fetchone()
        search_type = search_type_row['search_type'] if search_type_row else 'Unknown'
        
        # Eğer tek run ise direkt detay sayfasına git
        if search_type == 'Run ID' and len(results) == 1:
            connection.close()
            return redirect(f'/run/{results[0]["run_id"]}')
        
        connection.close()
        
        return render_template('quick_search_results.html', 
                             query=query, 
                             results=results,
                             search_type=search_type)
    except Exception as e:
        connection.close()
        return f"Error: {str(e)}", 500

# ============================================================================
# VIEW ALL RUNS
# ============================================================================

@app.route('/runs')
def view_runs():
    """Tüm run'ları listele - Stored Procedure ile"""
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    
    # Filter parametreleri
    date_from = request.args.get('date_from') or None
    date_to = request.args.get('date_to') or None
    customer = request.args.get('customer') or None
    band_type = request.args.get('band_type') or None
    sort_by = request.args.get('sort_by', 'run_id')
    sort_order = request.args.get('sort_order', 'DESC')
    
    try:
        # STORED PROCEDURE KULLAN!
        cursor.callproc('get_runs_list', [date_from, date_to, customer, band_type, sort_by, sort_order])
        
        runs = []
        for result in cursor.stored_results():
            runs = result.fetchall()
        
        # Get all customers for dropdown
        cursor.execute("""
            SELECT DISTINCT sv.string_value as customer
            FROM runid_stringvalues sv
            JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
            WHERE sc.column_name = 'customer'
            ORDER BY sv.string_value
        """)
        customers = [row['customer'] for row in cursor.fetchall()]
        
        connection.close()
        
        return render_template('runs.html', 
                             runs=runs,
                             customers=customers,
                             filters={
                                 'date_from': date_from or '',
                                 'date_to': date_to or '',
                                 'customer': customer or '',
                                 'band_type': band_type or '',
                                 'sort_by': sort_by,
                                 'sort_order': sort_order
                             })
    except Exception as e:
        connection.close()
        return f"Error: {str(e)}", 500

# ============================================================================
# RUN DETAIL
# ============================================================================

@app.route('/run/<int:run_id>')
def run_detail(run_id):
    """Run detayı - Stored Procedure ile"""
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    
    try:
        # STORED PROCEDURE KULLAN!
        cursor.callproc('get_run_details', [run_id])
        
        # 3 result set gelir
        results_list = []
        for result in cursor.stored_results():
            results_list.append(result.fetchall())
        
        if len(results_list) < 3:
            connection.close()
            return "Run not found", 404
        
        run_info = results_list[0][0] if results_list[0] else None
        string_data = results_list[1] if len(results_list) > 1 else []
        numeric_data = results_list[2] if len(results_list) > 2 else []
        
        if not run_info:
            connection.close()
            return "Run not found", 404
        
        connection.close()
        
        return render_template('run_detail.html',
                             run=run_info,
                             string_attributes=string_data,
                             numeric_attributes=numeric_data)
    except Exception as e:
        connection.close()
        return f"Error: {str(e)}", 500

# ============================================================================
# SEARCH
# ============================================================================

@app.route('/search', methods=['GET', 'POST'])
def search():
    """Arama sayfası - Stored Procedure ile"""
    if request.method == 'POST':
        search_type = request.form.get('search_type')
        column_name = request.form.get('column_name')
        
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        try:
            if search_type == 'string':
                search_value = request.form.get('search_value')
                # STORED PROCEDURE KULLAN!
                cursor.callproc('search_string_attribute', [column_name, search_value])
            else:  # numeric
                min_value = request.form.get('min_value')
                max_value = request.form.get('max_value')
                
                min_val = float(min_value) if min_value else None
                max_val = float(max_value) if max_value else None
                
                # STORED PROCEDURE KULLAN!
                cursor.callproc('search_numeric_attribute', [column_name, min_val, max_val])
            
            results = []
            for result in cursor.stored_results():
                results = result.fetchall()
            
            connection.close()
            
            return render_template('search_results.html', 
                                 results=results, 
                                 column_name=column_name,
                                 search_type=search_type)
        except Exception as e:
            connection.close()
            return f"Error: {str(e)}", 500
    
    # GET request - arama formunu göster
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    
    cursor.execute("SELECT column_name FROM stringcols ORDER BY column_name")
    string_columns = [row['column_name'] for row in cursor.fetchall()]
    
    cursor.execute("SELECT column_name FROM numericcols ORDER BY column_name")
    numeric_columns = [row['column_name'] for row in cursor.fetchall()]
    
    connection.close()
    
    return render_template('search.html', 
                         string_columns=string_columns,
                         numeric_columns=numeric_columns)

@app.route('/statistics')
def statistics():
    """İstatistikler sayfası - Direkt SQL ile (fallback)"""
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    
    # Toplam run sayısı
    cursor.execute("SELECT COUNT(*) as total_runs FROM runs")
    total_runs = cursor.fetchone()['total_runs']
    
    # Band type dağılımı
    cursor.execute("""
        SELECT sv.string_value, COUNT(*) as count
        FROM runid_stringvalues sv
        JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
        WHERE sc.column_name = 'band_type'
        GROUP BY sv.string_value
    """)
    band_distribution = cursor.fetchall()
    
    # Band yüzdesi hesapla
    band_count = sum([item['count'] for item in band_distribution if item['string_value'] == 'band'])
    band_percentage = round((band_count / total_runs) * 100, 1) if total_runs > 0 else 0
    
    # En çok kullanılan silindir
    cursor.execute("""
        SELECT sv.string_value as cylinder, COUNT(*) as count
        FROM runid_stringvalues sv
        JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
        WHERE sc.column_name = 'cylinder_number'
        GROUP BY sv.string_value
        ORDER BY count DESC
        LIMIT 1
    """)
    top_cylinder = cursor.fetchone()
    
    # Top 10 Customers
    cursor.execute("""
        SELECT sv.string_value as customer, COUNT(*) as count
        FROM runid_stringvalues sv
        JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
        WHERE sc.column_name = 'customer'
        GROUP BY sv.string_value
        ORDER BY count DESC
        LIMIT 10
    """)
    top_customers = cursor.fetchall()
    
    # Ink Type dağılımı
    cursor.execute("""
        SELECT sv.string_value as ink_type, COUNT(*) as count
        FROM runid_stringvalues sv
        JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
        WHERE sc.column_name = 'ink_type'
        GROUP BY sv.string_value
        ORDER BY count DESC
    """)
    ink_distribution = cursor.fetchall()
    
    # Press kullanımı
    cursor.execute("""
        SELECT sv.string_value as press, COUNT(*) as count
        FROM runid_stringvalues sv
        JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
        WHERE sc.column_name = 'press'
        GROUP BY sv.string_value
        ORDER BY count DESC
    """)
    press_distribution = cursor.fetchall()
    
    # Ortalama numeric değerler
    cursor.execute("""
        SELECT 
            AVG(CASE WHEN nc.column_name = 'proof_cut' THEN nv.numeric_value END) as avg_proof_cut,
            AVG(CASE WHEN nc.column_name = 'viscosity' THEN nv.numeric_value END) as avg_viscosity,
            AVG(CASE WHEN nc.column_name = 'wax' THEN nv.numeric_value END) as avg_wax,
            AVG(CASE WHEN nc.column_name = 'hardener' THEN nv.numeric_value END) as avg_hardener
        FROM runid_numericvalues nv
        JOIN numericcols nc ON nv.numericcol_id = nc.numericcol_id
    """)
    numeric_stats = cursor.fetchone()
    
    connection.close()
    
    return render_template('statistics.html',
                         total_runs=total_runs,
                         band_percentage=band_percentage,
                         top_cylinder=top_cylinder,
                         band_distribution=band_distribution,
                         top_customers=top_customers,
                         ink_distribution=ink_distribution,
                         press_distribution=press_distribution,
                         numeric_stats=numeric_stats)

# ============================================================================
# PROCEDURES & VIEWS DEMONSTRATION
# ============================================================================

@app.route('/procedures-views')
def procedures_views():
    """Stored Procedures ve Views demo sayfası"""
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    
    results = {}
    
    # 1. View kullanımı - v_string_data
    try:
        cursor.execute("SELECT * FROM v_string_data LIMIT 10")
        results['view_string'] = cursor.fetchall()
    except:
        results['view_string'] = []
    
    # 2. View kullanımı - v_numeric_data  
    try:
        cursor.execute("SELECT * FROM v_numeric_data LIMIT 10")
        results['view_numeric'] = cursor.fetchall()
    except:
        results['view_numeric'] = []
    
    # 3. Stored Procedure - OUT parameter
    try:
        cursor.execute("CALL count_band_types(@band, @noband)")
        cursor.execute("SELECT @band AS band_count, @noband AS noband_count")
        results['band_counts'] = cursor.fetchone()
    except Exception as e:
        results['band_counts'] = {'error': str(e)}
    
    # 4. Stored Procedure - INOUT parameter
    try:
        cursor.execute("SET @customer = 'kmart'")
        cursor.execute("CALL get_customer_run_count(@customer)")
        cursor.execute("SELECT @customer AS result")
        results['customer_count'] = cursor.fetchone()
    except Exception as e:
        results['customer_count'] = {'error': str(e)}
    
    # 5. Stored Procedure - Mixed parameters
    try:
        cursor.execute("CALL search_runs_by_date('1991-01-01', '1991-12-31', @total, @band_pct)")
        cursor.execute("SELECT @total AS total_runs, @band_pct AS band_percentage")
        results['date_search'] = cursor.fetchone()
    except Exception as e:
        results['date_search'] = {'error': str(e)}
    
    # 6. Stored Procedure - Customer report
    try:
        cursor.execute("CALL get_customer_report('kmart', @total, @band, @rate)")
        cursor.execute("SELECT @total AS total_runs, @band AS band_runs, @rate AS band_rate")
        results['customer_report'] = cursor.fetchone()
    except Exception as e:
        results['customer_report'] = {'error': str(e)}
    
    connection.close()
    
    return render_template('procedures_views.html', results=results)

# ============================================================================
# RUN APP
# ============================================================================

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)


# http://127.0.0.1:5000
# http://localhost:5000

# http://127.0.0.1:5000
# http://localhost:5000
