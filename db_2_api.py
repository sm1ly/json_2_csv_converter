import sys
import time
import sqlite3
import asyncio
import aiohttp
from contextlib import asynccontextmanager
from config import REFRESH_TOKEN, CLIENT_ID, CLIENT_SECRET

ZOHO_API_TOKEN = ""  # Изначально устанавливаем пустой токен
GRANT_TYPE = "refresh_token"
module_api_name = "Contacts"


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


async def main():
    await request_new_token()

    id_filename = 'last_id.txt'
    sqlite_db = 'contacts.db'
    start_id = read_id_from_file(id_filename)

    conn = sqlite3.connect(sqlite_db)
    c = conn.cursor()

    c.execute(f"SELECT * FROM contacts WHERE id > ? ORDER BY id LIMIT 100", (start_id,))
    rows = c.fetchall()

    start_time = time.time()
    total_records = start_id

    while rows:
        contacts_data = [{'id': row[0], 'First_Name': row[1], 'Last_Name': row[2], 'Email': row[3],
                          'Secondary_Email': row[4], 'Tertiary_Email': row[5], 'LinkedIn_URL': row[6],
                          'Mobile': row[7], 'Work_Phone': row[8], 'Country': row[9]} for row in rows]

        async with create_session() as session:
            await upload_contacts_batch(contacts_data, session)

        total_records += len(contacts_data)
        formatted_total_records = format(total_records, ",")
        print(f"Общее количество отправленных записей: {formatted_total_records}")

        last_id = rows[-1][0]
        write_id_to_file(id_filename, last_id)

        time_now = int(time.time())
        elapsed_time = time_now - start_time
        if elapsed_time >= 3000:
            await request_new_token()
            start_time = time.time()

        c.execute(f"SELECT * FROM contacts WHERE id > ? ORDER BY id LIMIT 100", (last_id,))
        rows = c.fetchall()

    conn.close()


if __name__ == '__main__':
    asyncio.run(main())
