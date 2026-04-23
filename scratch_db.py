import sqlite3
conn = sqlite3.connect(r'c:\Users\dones\OneDrive\Documents\Investments\Valuation\screener_data.db')
cursor = conn.cursor()
cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='table';")
with open("schema_out.txt", "w") as f:
    for name, sql in cursor.fetchall():
        f.write(f"Table: {name}\n")
        f.write(f"Schema: {sql}\n")
        
        cursor.execute(f"SELECT * FROM {name} LIMIT 3;")
        cols = [description[0] for description in cursor.description]
        f.write(f"Columns: {cols}\n")
        f.write("Sample Data:\n")
        for row in cursor.fetchall():
            f.write(f"{row}\n")
        f.write("\n")
conn.close()
