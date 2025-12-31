# Cylinder Bands Database

Rotogravure Printing Process Analysis Database System

## 📋 Project Overview

This project implements a **Metadata-Driven EAV (Entity-Attribute-Value) Model** for analyzing the Cylinder Bands dataset from UCI Machine Learning Repository. The system predicts banding defects in rotogravure printing processes.

## 🎯 Features

- **Metadata-Driven ER Diagram** with 5 core tables
- **Flask Web Interface** with interactive dashboard
- **Advanced Filtering** and search capabilities
- **Statistical Analysis** with Chart.js visualizations
- **Quick Search** functionality
- **539 production runs** with 37 attributes

## 🗄️ Database Structure

### Core Tables
- `runs` - Production run records
- `stringcols` - String attribute metadata
- `numericcols` - Numeric attribute metadata
- `runid_stringvalues` - String attribute values
- `runid_numericvalues` - Numeric attribute values

## 🚀 Installation

### Prerequisites
- Python 3.8+
- MySQL 8.0+
- pip

### Setup

1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/Cylinder-Bands-Database.git
cd cylinder-bands-database
```

2. Install dependencies
```bash
pip install flask mysql-connector-python pandas
```

3. Create MySQL database
```bash
mysql -u root -p
CREATE DATABASE cylinder_bands_db;
SOURCE cylinder_bands_metadata_driven.sql;
exit
```

4. Load data
```bash
python load_data.py
```

5. Run the application
```bash
python app.py
```

6. Open browser
```
http://localhost:5000
```

## 📊 Dataset

- **Source**: UCI Machine Learning Repository - Cylinder Bands Dataset
- **Original size**: 540 rows, 40 columns
- **Cleaned size**: 534 rows (5 duplicates removed) , 38 columns
- **String attributes**: 18
- **Numeric attributes**: 19

## 🎨 Web Interface

- **Home**: Quick search and dashboard
- **View Runs**: Browse all production runs with advanced filters
- **Search**: Attribute-based search (string/numeric)
- **Statistics**: Interactive charts and analytics
- **Run Details**: Complete attribute breakdown

## 🛠️ Technologies

- **Backend**: Flask (Python)
- **Database**: MySQL 8.0
- **Frontend**: HTML5, CSS3, JavaScript
- **Charts**: Chart.js
- **Design Pattern**: Metadata-Driven EAV Model

## 📚 Database Design Principles

- **3NF Compliance**
- **NULL Optimization**: NULL values not stored (sparse data optimization)
- **Vertical Partitioning**: Attributes separated by data type
- **Foreign Key Constraints**: Referential integrity maintained
