import sys
import time
import sqlite3
import asyncio
import aiohttp
import json
import zipfile
import csv
from contextlib import asynccontextmanager
from config import REFRESH_TOKEN, CLIENT_ID, CLIENT_SECRET, ZGID

ZOHO_API_TOKEN = ""  # Изначально устанавливаем пустой токен
GRANT_TYPE = "refresh_token"
module_api_name = "Contacts"

async def send_file_and_request_processing(zip_filename):
    upload_url = "https://upload.zoho.com/recruit/v2/upload"
    headers = {
        "Authorization": f"Zoho-oauthtoken {ZOHO_API_TOKEN}",
        "X-RECRUIT-ORG": ZGID,
        "feature": "bulk-write"
    }

    print("ZAPI: ", ZOHO_API_TOKEN)

    # Отправляем файл
    async with aiohttp.ClientSession() as session:
        with open(zip_filename, "rb") as file:
            form_data = aiohttp.FormData()
            form_data.add_field("file", file, filename=zip_filename)
            async with session.post(upload_url, headers=headers, data=form_data) as response:
                try:
                    response.raise_for_status()
                    result = await response.json()

                    if "details" in result:
                        file_id = result["details"]["id"]

                        # Записываем file_id в файл
                        with open("api_file_id.txt", "a") as f:
                            f.write(f"{file_id}\n")

                        # Запрос на обработку файла с file_id (заглушка)
                        job_id = await process_file(file_id)

                        # Записываем job_id в файл
                        with open("api_jobs_id", "a") as f:
                            f.write(f"{job_id}\n")

                    else:
                        print("Error:", result)
                except Exception as e:
                    print(f"Error occurred: {e}")
                    sys.exit(1)


async def process_file(file_id):
    # Заглушка для отправки запроса на обработку файла
    # Вместо этого кода напишите ваш запрос на обработку файла и возврат job_id
    job_id = "example_job_id"
    return job_id

def compress_file_to_zip(csv_filename, zip_filename):
    with zipfile.ZipFile(zip_filename, 'w') as zipf:
        zipf.write(csv_filename)

async def request_new_token():
    refresh_token_url = "https://accounts.zoho.com/oauth/v2/token"
    refresh_token_data = {
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": GRANT_TYPE,
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(refresh_token_url, data=refresh_token_data) as resp:
            result = await resp.json()
            if "access_token" in result:
                global ZOHO_API_TOKEN
                ZOHO_API_TOKEN = result["access_token"]
                print("Token updated successfully.")
            else:
                print("Error updating token:", result)

# Ваш код функции upload_contacts_batch
async def upload_contacts_batch(contacts_data, session):
    # Замените module_api_name и ZOHO_API_TOKEN на соответствующие значения
    url = f'https://recruit.zoho.com/recruit/v2/{module_api_name}/upsert'
    headers = {
        'Authorization': f'Zoho-oauthtoken {ZOHO_API_TOKEN}',
        'Content-Type': 'application/json'
    }

    async with session.post(url, headers=headers, json={'data': contacts_data}) as response:
        try:
            response.raise_for_status()
            # print(f'Successfully added {len(contacts_data)} contacts.')

        except Exception as e:
            print(f'Error occurred: {e}')
            sys.exit(1)

def read_id_from_file(filename):
    try:
        with open(filename, 'r') as f:
            return int(f.read().strip())
    except FileNotFoundError:
        return 0

def write_id_to_file(filename, id):
    with open(filename, 'w') as f:
        f.write(str(id))

@asynccontextmanager
async def create_session():
    async with aiohttp.ClientSession() as session:
        yield session

def save_to_csv_file(data, filename):
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(['First_Name', 'Last_Name', 'Email', 'Secondary_Email',
                         'Tertiary_Email', 'LinkedIn_URL', 'Mobile', 'Work_Phone', 'Country'])
        for row in data:
            writer.writerow(row.values())


async def main():
    await request_new_token()

    id_filename = 'last_id.txt'
    sqlite_db = 'contacts.db'
    start_id = read_id_from_file(id_filename)

    conn = sqlite3.connect(sqlite_db)
    c = conn.cursor()

    c.execute(f"SELECT * FROM contacts WHERE id > ? ORDER BY id LIMIT 24999", (start_id,))
    rows = c.fetchall()

    start_time = time.time()
    total_records = start_id

    while rows:
        contacts_data = [{'First_Name': row[1], 'Last_Name': row[2], 'Email': row[3],
                          'Secondary_Email': row[4], 'Tertiary_Email': row[5], 'LinkedIn_URL': row[6],
                          'Mobile': row[7], 'Work_Phone': row[8], 'Country': row[9]} for row in rows]

        csv_filename = f'contacts_{start_id}.csv'
        zip_filename = f'contacts_{start_id}.csv.zip'

        save_to_csv_file(contacts_data, csv_filename)
        compress_file_to_zip(csv_filename, zip_filename)

        file_id = await send_file_and_request_processing(zip_filename)

        total_records += len(contacts_data)
        formatted_total_records = format(total_records, ",")
        print(f"Общее количество отправленных записей: {formatted_total_records}")

        with open("file_id.txt", "a") as f:
            f.write(f"{file_id}\n")

        last_id = rows[-1][0]
        write_id_to_file(id_filename, last_id)

        time_now = int(time.time())
        elapsed_time = time_now - start_time
        if elapsed_time >= 3000:
            await request_new_token()
            start_time = time.time()

        c.execute(f"SELECT * FROM contacts WHERE id > ? ORDER BY id LIMIT 24999", (last_id,))
        rows = c.fetchall()

    conn.close()

if __name__ == '__main__':
    asyncio.run(main())


