import sqlite3
import asyncio
import csv

def read_id_from_file(filename):
    try:
        with open(filename, 'r') as f:
            return int(f.read().strip())
    except FileNotFoundError:
        return 0

def write_id_to_file(filename, id):
    with open(filename, 'w') as f:
        f.write(str(id))

def save_to_csv_file(data, filename):
    with open(f'csv_files/{filename}', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow(['First_Name', 'Last_Name', 'Email', 'Secondary_Email',
                         'Tertiary_Email', 'LinkedIn_URL', 'Mobile', 'Work_Phone', 'Country'])
        for row in data:
            writer.writerow(row.values())

async def main():

    id_filename = 'last_id.txt'
    sqlite_db = 'contacts.db'
    start_id = read_id_from_file(id_filename)

    conn = sqlite3.connect(sqlite_db)
    c = conn.cursor()

    c.execute(f"SELECT * FROM contacts WHERE id > ? ORDER BY id LIMIT 19999", (start_id,))
    rows = c.fetchall()

    total_records = start_id

    while rows:
        contacts_data = [{'First_Name': row[1], 'Last_Name': row[2], 'Email': row[3],
                          'Secondary_Email': row[4], 'Tertiary_Email': row[5], 'LinkedIn_URL': row[6],
                          'Mobile': row[7], 'Work_Phone': row[8], 'Country': row[9]} for row in rows]

        file_id = read_id_from_file(id_filename)
        csv_filename = f'contacts_{file_id}.csv'
        save_to_csv_file(contacts_data, csv_filename)

        total_records += len(contacts_data)
        formatted_total_records = format(total_records, ",")
        print(f"Общее количество отправленных записей: {formatted_total_records}")

        with open("file_id.txt", "a") as f:
            f.write(f"{file_id}\n")

        last_id = rows[-1][0]
        write_id_to_file(id_filename, last_id)

        c.execute(f"SELECT * FROM contacts WHERE id > ? ORDER BY id LIMIT 19999", (last_id,))
        rows = c.fetchall()

    conn.close()

if __name__ == '__main__':
    asyncio.run(main())


