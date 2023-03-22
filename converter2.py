import json
import os
import asyncio
import aiohttp
import requests
from concurrent.futures import ThreadPoolExecutor

COUNTRIES = ['united states', 'sweden']
ZOHO_API_TOKEN = '1000.c8d7ff6649539a54fa1b5046780d3f0e.432a72e3708c9c67e94c124950a57246'
module_api_name = "Contacts"

input_dir = 'cutted_files'
output_file = 'output.json'
current_file_name = 'current_file.txt'


async def update_token():
    refresh_token_url = "https://accounts.zoho.com/oauth/v2/token"
    refresh_token_data = {
        "refresh_token": "1000.b06043dd35041d8c55d58295d4c3dbf3.41f8e4dddd836c66a871814d12a3ca4c",
        "client_id": "1000.WZUONTXZWT47K71C5RFUCLT3TB34TW",
        "client_secret": "071c69a62b93d7dde45d8c46c4f64672c08142612e",
        "grant_type": "refresh_token",
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
        "refresh_token": "1000.b06043dd35041d8c55d58295d4c3dbf3.41f8e4dddd836c66a871814d12a3ca4c",
        "client_id": "1000.WZUONTXZWT47K71C5RFUCLT3TB34TW",
        "client_secret": "071c69a62b93d7dde45d8c46c4f64672c08142612e",
        "grant_type": "refresh_token",
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
    loop = asyncio.get_event_loop()
    url = f'https://recruit.zoho.com/recruit/v2/{module_api_name}/upsert'
    headers = {
        'Authorization': f'Zoho-oauthtoken {ZOHO_API_TOKEN}',
        'Content-Type': 'application/json'
    }

    with open(file_path, 'r') as infile:
        for line in infile:
            entry = json.loads(line)

            contact_data = {
                'data': [entry]
            }

            with ThreadPoolExecutor() as executor:
                try:
                    response = await loop.run_in_executor(executor, lambda: requests.post(url, headers=headers, json=contact_data))
                    response.raise_for_status()

                    print(f'Successfully added contact: {entry["First_Name"]} {entry["Last_Name"]}')
                    print(response)

                except Exception as e:
                    print(f'Error occurred: {e}')

                await asyncio.sleep(1)


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

    for file_name in os.listdir(input_dir):
        if file_name.startswith("prepare_v02_"):
            if current_processed_file is None or found_file:
                file_path = os.path.join(input_dir, file_name)
                print(f"Processing file: {file_path}")
                write_current_file_name(file_name)
                await upload_contacts_from_file(file_path)
            elif current_processed_file == file_name:
                found_file = True

    await update_token_task

asyncio.run(main())


