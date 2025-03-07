import pymysql
import csv
import os
from pymysql.cursors import DictCursor

# Database Configuration
db_config = {
    "host": "localhost",
    "database": "godatabase",
    "user": "admin",
    "password": ".Gocredit2024",
    "charset": "utf8mb4",
    "allow_local_infile": True
}

# Export path
EXPORT_PATH = r"C:\Users\Go Credit\Documents\DATA\EXPORTS"

class DatabaseExporter:
    """Handles MySQL Connection and Exports Tables to CSV."""
    
    def __init__(self, config):
        self.config = config
        self.connection = None

    def connect(self):
        """Establishes a connection to the database."""
        try:
            self.connection = pymysql.connect(
                host=self.config["host"],
                user=self.config["user"],
                password=self.config["password"],
                database=self.config["database"],
                charset="utf8mb4",  # Ensure utf8mb4 charset
                local_infile=self.config["allow_local_infile"],
                cursorclass=DictCursor
            )
        except pymysql.MySQLError as e:
            print(f"Error connecting to database: {e}")
            self.connection = None

    def get_tables(self):
        """Retrieves the list of tables in the database."""
        if self.connection is None:
            self.connect()

        if self.connection:
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute("SHOW TABLES;")
                    return [table[f"Tables_in_{self.config['database']}"] for table in cursor.fetchall()]
            except pymysql.MySQLError as e:
                print(f"Error retrieving tables: {e}")
        return []

    def export_table_to_csv(self, table_name):
        """Exports the selected table to a CSV file with UTF-8 encoding."""
        if self.connection is None:
            self.connect()

        if self.connection:
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(f"SELECT * FROM `{table_name}`;")
                    rows = cursor.fetchall()
                    if not rows:
                        print(f"The table '{table_name}' is empty. No CSV generated.")
                        return

                    # Prepare file path
                    os.makedirs(EXPORT_PATH, exist_ok=True)
                    file_path = os.path.join(EXPORT_PATH, f"{table_name}.csv")

                    # Writing to CSV (UTF-8-SIG to support special characters in Excel)
                    with open(file_path, mode='w', newline='', encoding='utf-8-sig') as file:
                        writer = csv.DictWriter(file, fieldnames=rows[0].keys())
                        writer.writeheader()
                        writer.writerows(rows)

                    print(f"✅ Table '{table_name}' exported successfully to: {file_path}")

            except pymysql.MySQLError as e:
                print(f"Error exporting table '{table_name}': {e}")

    def close(self):
        """Closes the database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None

# Main execution
if __name__ == "__main__":
    db_exporter = DatabaseExporter(db_config)
    
    # Get tables
    tables = db_exporter.get_tables()
    if not tables:
        print("No tables found in the database.")
    else:
        print("\nAvailable Tables:")
        for idx, table in enumerate(tables, 1):
            print(f"{idx}.- {table}")

        # Ask for user input
        try:
            choice = int(input("\nEnter the number of the table you want to export: "))
            if 1 <= choice <= len(tables):
                selected_table = tables[choice - 1]
                db_exporter.export_table_to_csv(selected_table)
            else:
                print("Invalid selection. Please enter a valid number.")
        except ValueError:
            print("Invalid input. Please enter a numeric value.")

    db_exporter.close()