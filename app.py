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
    """Hızlı arama - Run ID veya Customer"""
    query = request.args.get('q', '').strip()
    
    if not query:
        return redirect('/')
    
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    
    results = []
    search_type = None
    
    # Try as Run ID (numeric)
    if query.isdigit():
        search_type = 'Run ID'
        cursor.execute("SELECT * FROM runs WHERE run_id = %s", (int(query),))
        run = cursor.fetchone()
        if run:
            connection.close()
            return redirect(f'/run/{run["run_id"]}')
    
    # Search as Customer name
    search_type = 'Customer Name'
    cursor.execute("""
        SELECT DISTINCT r.run_id, r.timestamp, sv.string_value as customer
        FROM runs r
        JOIN runid_stringvalues sv ON r.run_id = sv.run_id
        JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
        WHERE sc.column_name = 'customer' AND sv.string_value LIKE %s
        ORDER BY r.run_id
        LIMIT 50
    """, (f'%{query}%',))
    results = cursor.fetchall()
    
    connection.close()
    
    return render_template('quick_search_results.html', 
                         query=query, 
                         results=results,
                         search_type=search_type)

# ============================================================================
# VIEW ALL RUNS
# ============================================================================

@app.route('/runs')
def view_runs():
    """Tüm run'ları listele - Filters ile"""
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    
    # Filter parametreleri
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    customer = request.args.get('customer', '')
    band_type = request.args.get('band_type', '')
    sort_by = request.args.get('sort_by', 'run_id')
    sort_order = request.args.get('sort_order', 'DESC')
    
    # Base query
    query = "SELECT DISTINCT r.run_id, r.timestamp FROM runs r"
    conditions = []
    params = []
    
    # Date filter
    if date_from:
        conditions.append("r.timestamp >= %s")
        params.append(date_from)
    if date_to:
        conditions.append("r.timestamp <= %s")
        params.append(date_to)
    
    # Customer filter
    if customer:
        query += """
            JOIN runid_stringvalues sv_cust ON r.run_id = sv_cust.run_id
            JOIN stringcols sc_cust ON sv_cust.stringcol_id = sc_cust.stringcol_id
        """
        conditions.append("sc_cust.column_name = 'customer' AND sv_cust.string_value LIKE %s")
        params.append(f'%{customer}%')
    
    # Band type filter
    if band_type:
        query += """
            JOIN runid_stringvalues sv_band ON r.run_id = sv_band.run_id
            JOIN stringcols sc_band ON sv_band.stringcol_id = sc_band.stringcol_id
        """
        conditions.append("sc_band.column_name = 'band_type' AND sv_band.string_value = %s")
        params.append(band_type)
    
    # Add WHERE clause
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    # Add ORDER BY
    valid_sort = ['run_id', 'timestamp']
    valid_order = ['ASC', 'DESC']
    if sort_by in valid_sort and sort_order in valid_order:
        query += f" ORDER BY r.{sort_by} {sort_order}"
    else:
        query += " ORDER BY r.run_id ASC"
    
    cursor.execute(query, tuple(params))
    runs = cursor.fetchall()
    
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
                             'date_from': date_from,
                             'date_to': date_to,
                             'customer': customer,
                             'band_type': band_type,
                             'sort_by': sort_by,
                             'sort_order': sort_order
                         })

# ============================================================================
# VIEW RUN DETAILS
# ============================================================================

@app.route('/run/<int:run_id>')
def view_run_detail(run_id):
    """Bir run'ın detaylarını göster"""
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    
    # String değerleri getir
    cursor.execute("""
        SELECT sc.column_name, sv.string_value
        FROM runid_stringvalues sv
        JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
        WHERE sv.run_id = %s
        ORDER BY sc.column_name
    """, (run_id,))
    string_data = cursor.fetchall()
    
    # Numeric değerleri getir
    cursor.execute("""
        SELECT nc.column_name, nv.numeric_value
        FROM runid_numericvalues nv
        JOIN numericcols nc ON nv.numericcol_id = nc.numericcol_id
        WHERE nv.run_id = %s
        ORDER BY nc.column_name
    """, (run_id,))
    numeric_data = cursor.fetchall()
    
    connection.close()
    return render_template('run_detail.html', 
                         run_id=run_id, 
                         string_data=string_data,
                         numeric_data=numeric_data)

# ============================================================================
# SEARCH
# ============================================================================

@app.route('/search', methods=['GET', 'POST'])
def search():
    """Arama sayfası"""
    if request.method == 'POST':
        search_type = request.form.get('search_type')  # 'string' veya 'numeric'
        column_name = request.form.get('column_name')
        
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)
        
        if search_type == 'string':
            search_value = request.form.get('search_value')
            cursor.execute("""
                SELECT r.run_id, r.timestamp, sv.string_value as value
                FROM runs r
                JOIN runid_stringvalues sv ON r.run_id = sv.run_id
                JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
                WHERE sc.column_name = %s AND sv.string_value LIKE %s
                ORDER BY r.run_id
                LIMIT 100
            """, (column_name, f'%{search_value}%'))
        else:  # numeric
            min_value = request.form.get('min_value')
            max_value = request.form.get('max_value')
            
            # Eğer sadece min veya max varsa
            if min_value and max_value:
                cursor.execute("""
                    SELECT r.run_id, r.timestamp, nv.numeric_value as value
                    FROM runs r
                    JOIN runid_numericvalues nv ON r.run_id = nv.run_id
                    JOIN numericcols nc ON nv.numericcol_id = nc.numericcol_id
                    WHERE nc.column_name = %s 
                    AND nv.numeric_value BETWEEN %s AND %s
                    ORDER BY r.run_id
                    LIMIT 100
                """, (column_name, float(min_value), float(max_value)))
            elif min_value:
                cursor.execute("""
                    SELECT r.run_id, r.timestamp, nv.numeric_value as value
                    FROM runs r
                    JOIN runid_numericvalues nv ON r.run_id = nv.run_id
                    JOIN numericcols nc ON nv.numericcol_id = nc.numericcol_id
                    WHERE nc.column_name = %s 
                    AND nv.numeric_value >= %s
                    ORDER BY r.run_id
                    LIMIT 100
                """, (column_name, float(min_value)))
            elif max_value:
                cursor.execute("""
                    SELECT r.run_id, r.timestamp, nv.numeric_value as value
                    FROM runs r
                    JOIN runid_numericvalues nv ON r.run_id = nv.run_id
                    JOIN numericcols nc ON nv.numericcol_id = nc.numericcol_id
                    WHERE nc.column_name = %s 
                    AND nv.numeric_value <= %s
                    ORDER BY r.run_id
                    LIMIT 100
                """, (column_name, float(max_value)))
            else:
                # Hiçbiri yoksa hepsini getir
                cursor.execute("""
                    SELECT r.run_id, r.timestamp, nv.numeric_value as value
                    FROM runs r
                    JOIN runid_numericvalues nv ON r.run_id = nv.run_id
                    JOIN numericcols nc ON nv.numericcol_id = nc.numericcol_id
                    WHERE nc.column_name = %s
                    ORDER BY r.run_id
                    LIMIT 100
                """, (column_name,))
        
        results = cursor.fetchall()
        connection.close()
        
        return render_template('search_results.html', 
                             results=results, 
                             column_name=column_name,
                             search_type=search_type)
    
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

# ============================================================================
# STATISTICS
# ============================================================================

@app.route('/statistics')
def statistics():
    """İstatistikler sayfası"""
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    
    # 1. Toplam run sayısı
    cursor.execute("SELECT COUNT(*) as total_runs FROM runs")
    total_runs = cursor.fetchone()['total_runs']
    
    # 2. Band type dağılımı
    cursor.execute("""
        SELECT sv.string_value, COUNT(*) as count
        FROM runid_stringvalues sv
        JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
        WHERE sc.column_name = 'band_type'
        GROUP BY sv.string_value
    """)
    band_distribution = cursor.fetchall()
    
    # 3. Band yüzdesi hesapla
    band_count = sum([item['count'] for item in band_distribution if item['string_value'] == 'band'])
    band_percentage = round((band_count / total_runs) * 100, 1) if total_runs > 0 else 0
    
    # 4. En çok kullanılan silindir
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
    
    # 5. Top 10 Customers
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
    
    # 6. Ink Type dağılımı
    cursor.execute("""
        SELECT sv.string_value as ink_type, COUNT(*) as count
        FROM runid_stringvalues sv
        JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
        WHERE sc.column_name = 'ink_type'
        GROUP BY sv.string_value
        ORDER BY count DESC
    """)
    ink_distribution = cursor.fetchall()
    
    # 7. Press kullanımı
    cursor.execute("""
        SELECT sv.string_value as press, COUNT(*) as count
        FROM runid_stringvalues sv
        JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
        WHERE sc.column_name = 'press'
        GROUP BY sv.string_value
        ORDER BY count DESC
    """)
    press_distribution = cursor.fetchall()
    
    # 8. Ortalama numeric değerler
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
# API ENDPOINTS
# ============================================================================

@app.route('/api/run/<int:run_id>')
def api_get_run(run_id):
    """API: Run detaylarını JSON olarak döndür"""
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    
    # Run bilgisi
    cursor.execute("SELECT * FROM runs WHERE run_id = %s", (run_id,))
    run = cursor.fetchone()
    
    if not run:
        return jsonify({'error': 'Run not found'}), 404
    
    # String değerler
    cursor.execute("""
        SELECT sc.column_name, sv.string_value
        FROM runid_stringvalues sv
        JOIN stringcols sc ON sv.stringcol_id = sc.stringcol_id
        WHERE sv.run_id = %s
    """, (run_id,))
    string_data = {row['column_name']: row['string_value'] for row in cursor.fetchall()}
    
    # Numeric değerler
    cursor.execute("""
        SELECT nc.column_name, nv.numeric_value
        FROM runid_numericvalues nv
        JOIN numericcols nc ON nv.numericcol_id = nc.numericcol_id
        WHERE nv.run_id = %s
    """, (run_id,))
    numeric_data = {row['column_name']: float(row['numeric_value']) if row['numeric_value'] else None 
                    for row in cursor.fetchall()}
    
    connection.close()
    
    return jsonify({
        'run_id': run_id,
        'timestamp': str(run['timestamp']),
        'string_attributes': string_data,
        'numeric_attributes': numeric_data
    })

# ============================================================================
# RUN APP
# ============================================================================

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)


# http://127.0.0.1:5000
# http://localhost:5000