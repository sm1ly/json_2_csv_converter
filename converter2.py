import json
import os
import asyncio
import aiohttp
import sys
from config import ZOHO_API_TOKEN, REFRESH_TOKEN, CLIENT_ID, CLIENT_SECRET


module_api_name = "Contacts"
GRANT_TYPE = 'refresh_token'
input_dir = 'cutted_files'
output_file = 'output.json'
current_file_name = 'current_file.txt'


async def update_token():
    refresh_token_url = "https://accounts.zoho.com/oauth/v2/token"
    refresh_token_data = {
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": GRANT_TYPE,
    }

    while True:
        async with aiohttp.ClientSession() as session:
            async with session.post(refresh_token_url, data=refresh_token_data) as resp:
                result = await resp.json()
                if "access_token" in result:
                    global ZOHO_API_TOKEN
                    ZOHO_API_TOKEN = result["access_token"]
                    print("Token updated successfully.")
                else:
                    print("Error updating token:", result)
        await asyncio.sleep(3000)  # Обновление токена каждые 50 минут (3000 секунд)


async def update_token_fast():
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

                if len(contacts) == 1000:  # Загружаем контакты пакетами по 1000
                    asyncio.ensure_future(upload_contacts_batch(contacts, session))
                    contacts = []

            if contacts:  # Загрузить оставшиеся контакты, если есть
                asyncio.ensure_future(upload_contacts_batch(contacts, session))


async def upload_contacts_batch(contacts_data, session):
    url = f'https://recruit.zoho.com/recruit/v2/{module_api_name}/upsert'
    headers = {
        'Authorization': f'Zoho-oauthtoken {ZOHO_API_TOKEN}',
        'Content-Type': 'application/json'
    }

    async with session.post(url, headers=headers, json={'data': contacts_data}) as response:
        try:
            response.raise_for_status()
            response_data = await response.json()  # Получить данные ответа в формате JSON
            print(f'Successfully added {len(contacts_data)} contacts.')
            print(response_data)  # Вывести данные ответа

        except Exception as e:
            print(f'Error occurred: {e}')
            sys.exit(1)  # Завершить программу с кодом ошибки 1


def read_current_file_name():
    if os.path.exists(current_file_name):
        with open(current_file_name, 'r') as file:
            return file.read().strip()
    return None


def write_current_file_name(file_name):
    with open(current_file_name, 'w') as file:
        file.write(file_name)


async def main():
    await update_token_fast()
    await asyncio.sleep(2)
    update_token_task = asyncio.create_task(update_token())

    current_processed_file = read_current_file_name()
    found_file = False
    upload_tasks = []

    for file_name in os.listdir(input_dir):
        if file_name.startswith("prepare_v02_"):
            if current_processed_file is None or found_file:
                file_path = os.path.join(input_dir, file_name)
                print(f"Processing file: {file_path}")
                write_current_file_name(file_name)
                task = asyncio.create_task(upload_contacts_from_file(file_path))
                upload_tasks.append(task)
            elif current_processed_file == file_name:
                found_file = True

    await asyncio.gather(update_token_task, *upload_tasks)

asyncio.run(main())

