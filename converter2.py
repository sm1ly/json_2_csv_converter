import json
import os
import asyncio
import aiohttp
import sys
import time
from config import ZOHO_API_TOKEN, REFRESH_TOKEN, CLIENT_ID, CLIENT_SECRET

module_api_name = "Contacts"
GRANT_TYPE = 'refresh_token'
input_dir = 'cutted_files/'
current_file_name = 'current_file.txt'


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


async def upload_contacts_from_file(file_path):
    async with aiohttp.ClientSession() as session:
        with open(file_path, 'r') as infile:
            contacts = []
            for line in infile:
                entry = json.loads(line)
                contacts.append(entry)

                if len(contacts) == 100:  # Загружаем контакты пакетами по 100
                    await upload_contacts_batch(contacts, session)
                    contacts = []

            if contacts:  # Загрузить оставшиеся контакты, если есть
                await upload_contacts_batch(contacts, session)


async def upload_contacts_batch(contacts_data, session):
    url = f'https://recruit.zoho.com/recruit/v2/{module_api_name}/upsert'
    headers = {
        'Authorization': f'Zoho-oauthtoken {ZOHO_API_TOKEN}',
        'Content-Type': 'application/json'
    }

    async with session.post(url, headers=headers, json={'data': contacts_data}) as response:
        try:
            response.raise_for_status()
            # response_data = await response.json()  # Получить данные ответа в формате JSON
            print(f'Successfully added {len(contacts_data)} contacts.')

        except Exception as e:
            print(f'Error occurred: {e}')
            sys.exit(1)


async def main():
    await request_new_token()
    await asyncio.sleep(2)
    start_time = int(time.time())

    # название файла, в котором хранится последний обработанный файл
    filename_log = "last_processed_file.txt"

    # если файл с логом существует, прочитаем из него последний обработанный файл
    if os.path.isfile(filename_log):
        with open(filename_log, "r") as f:
            last_processed_file = f.read().strip()
    else:
        # если лога нет, обработаем первый файл из директории
        files = os.listdir(input_dir)
        files = sorted([f for f in files if f.startswith("prepare_v02_")])
        last_processed_file = files[0]

    # обработаем оставшиеся файлы
    files = os.listdir(input_dir)
    files = sorted([f for f in files if f.startswith("prepare_v02_")], key=lambda x: int(x.split("_")[2].split(".")[0]))

    for file in files:
        # прочитаем первую строку файла
        try:
            with open(input_dir + file, "r") as f:
                # check 50 min
                time_now = int(time.time())
                print(time_now)
                elapsed_time = time_now - start_time
                if elapsed_time >= 3000:
                    await request_new_token()
                    start_time = time.time()

                first_line = f.readline().strip()

            file_path = input_dir + file
            await upload_contacts_from_file(file_path)

        except Exception as e:
            # если возникла ошибка, запомним название файла и перейдем к следующему
            with open("error_log.txt", "a") as f:
                f.write(f"Error processing file {file}: {str(e)}\n")
            continue

        # запомним название обрабатываемого файла
        with open(filename_log, "w") as f:
            f.write(file)

        # сделаем что-то с первой строкой файла (например, выведем ее на экран)
        print(f"First line of {file}: {first_line}")

asyncio.run(main())



