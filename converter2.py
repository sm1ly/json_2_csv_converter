import json
import requests
import tempfile

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


def json_to_zoho_contacts(input_file, output_file):
    url = f'https://recruit.zoho.com/recruit/v2/{module_api_name}/upsert'
    headers = {
        'Authorization': f'Zoho-oauthtoken {ZOHO_API_TOKEN}',
        'Content-Type': 'application/json'
    }

    with open(input_file, 'r+') as infile, open(output_file, 'a') as outfile:
        temp_file = tempfile.TemporaryFile(mode='w+')
        for line in infile:
            entry = json.loads(line)
            country = get_country(entry.get('a', ''))

            if not country:
                temp_file.write(line)
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
                outfile.write(json.dumps(entry) + '\n')  # Запись строки в файл output.json только при успешной отправке
            except requests.exceptions.HTTPError as error:
                print(f'Error adding contact: {first_name} {last_name}, stopping. Error: {error}')
                temp_file.write(line)  # Запись строки в temp_file только при ошибке
                break

        temp_file.seek(0)
        infile.seek(0)
        infile.truncate()
        for line in temp_file:
            infile.write(line)
        temp_file.close()


input_file = 'data.json'
output_file = 'output.json'
json_to_zoho_contacts(input_file, output_file)

