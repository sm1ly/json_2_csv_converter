import json
import requests
import tempfile
import asyncio
import aiohttp
import sys
from concurrent.futures import ThreadPoolExecutor

# Список разрешенных стран
COUNTRIES = ['argentina', 'armenia', 'australia', 'austria', 'azerbaijan', 'belarus', 'belgium', 'brazil',
             'bulgaria', 'canada', 'croatia', 'cyprus', 'czechia', 'denmark', 'estonia', 'finland', 'france',
             'georgia', 'germany', 'gibraltar', 'greece', 'hong kong', 'hungary', 'iceland', 'ireland', 'israel',
             'italy', 'kazakhstan', 'kyrgyzstan', 'latvia', 'liechtenstein', 'lithuania', 'luxembourg', 'malta',
             'moldova', 'monaco', 'montenegro', 'netherlands', 'new zealand', 'norway', 'poland', 'portugal',
             'russia', 'saudi arabia', 'serbia', 'singapore', 'slovakia', 'slovenia', 'spain', 'sweden',
             'switzerland', 'turkey', 'ukraine', 'united arab emirates', 'united kingdom', 'united states']

ZOHO_API_TOKEN = '1000.c8d7ff6649539a54fa1b5046780d3f0e.432a72e3708c9c67e94c124950a57246'
module_api_name = "Contacts"


def get_country(location):
    if location:
        country = location.split(', ')[-1]
        return country if country in COUNTRIES else ''
    return ''


async def json_to_zoho_contacts(input_file, output_file):
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as executor:
        while True:
            url = f'https://recruit.zoho.com/recruit/v2/{module_api_name}/upsert'
            headers = {
                'Authorization': f'Zoho-oauthtoken {ZOHO_API_TOKEN}',
                'Content-Type': 'application/json'
            }

            with open(input_file, 'r') as infile, tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file_new, tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file_processed:
                for line in infile:
                    entry = json.loads(line)
                    country = get_country(entry.get('a', ''))

                    if not country:
                        temp_file_new.write(line)
                        continue

                    names = entry.get('n', '').split(' ', 1)
                    first_name = names[0] if names else ''
                    last_name = names[1] if len(names) > 1 else ''
                    email, email2, email3 = (entry['e'][:3] + [''] * 3)[:3] if 'e' in entry else ('', '', '')
                    phone, phone2 = (entry['t'][:2] + [''] * 2)[:2] if 't' in entry else ('', '')
                    linkedin_url = entry.get('linkedin', '')

                    contact_data = {
                        'data': [{
                            'First_Name': first_name,
                            'Last_Name': last_name,
                            'Email': email,
                            'Secondary_Email': email2,
                            'Tertiary_Email': email3,
                            'LinkedIn_URL': linkedin_url,
                            'Mobile': phone,
                            'Work_Phone': phone2,
                            'Country': country
                        }]
                    }

                    try:
                        response = requests.post(url, headers=headers, json=contact_data)
                        response.raise_for_status()
                        print(f'Successfully added contact: {first_name} {last_name}')
                        print(response)

                        with open(output_file, 'a') as outfile:
                            outfile.write(line)  # Запись исходной строки

                        temp_file_processed.write(line)

                    except Exception as e:
                        print(f'Error occurred: {e}')
                        sys.exit(1)  # Завершить программу с кодом ошибки 1

                temp_file_new.seek(0)

                with open(input_file, 'w') as infile:
                    for line in temp_file_new:
                        infile.write(line)

            await asyncio.sleep(1)



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


async def main():
    await update_token_fast()  # Обновляем токен перед началом работы основной функции

    token_update_task = asyncio.create_task(update_token())
    json_to_zoho_contacts_task = asyncio.create_task(json_to_zoho_contacts(input_file, output_file))

    await asyncio.gather(token_update_task, json_to_zoho_contacts_task)

input_file = 'prepare_v01.json'
output_file = 'output.json'
asyncio.run(main())
