import json
import os
import sqlite3


def get_country(location):
    if location:
        country = location.split(', ')[-1]
        return country if country in COUNTRIES else ''
    return ''


def process_entry(entry):
    country = get_country(entry.get('a', ''))

    if not country:
        return None

    names = entry.get('n', '').split(' ', 1)
    first_name = names[0] if names else ''
    last_name = names[1] if len(names) > 1 else ''
    email, email2, email3 = (entry['e'][:3] + [''] * 3)[:3] if 'e' in entry else ('', '', '')
    phone, phone2 = (entry['t'][:2] + [''] * 2)[:2] if 't' in entry else ('', '')
    linkedin_url = entry.get('linkedin', '')

    contact_data = {
        'First_Name': first_name,
        'Last_Name': last_name,
        'Email': email,
        'Secondary_Email': email2,
        'Tertiary_Email': email3,
        'LinkedIn_URL': linkedin_url,
        'Mobile': phone,
        'Work_Phone': phone2,
        'Country': country
    }

    return contact_data


def drop_table(cursor):
    cursor.execute("DROP TABLE IF EXISTS contacts")


input_file = 'prepare_v01.json'
sqlite_db = 'contacts.db'
COUNTRIES = ['argentina', 'armenia', 'australia', 'austria', 'azerbaijan', 'belarus', 'belgium', 'brazil',
             'bulgaria', 'canada', 'croatia', 'cyprus', 'czechia', 'denmark', 'estonia', 'finland', 'france',
             'georgia', 'germany', 'gibraltar', 'greece', 'hong kong', 'hungary', 'iceland', 'ireland', 'israel',
             'italy', 'kazakhstan', 'kyrgyzstan', 'latvia', 'liechtenstein', 'lithuania', 'luxembourg', 'malta',
             'moldova', 'monaco', 'montenegro', 'netherlands', 'new zealand', 'norway', 'poland', 'portugal',
             'russia', 'saudi arabia', 'serbia', 'singapore', 'slovakia', 'slovenia', 'spain', 'sweden',
             'switzerland', 'turkey', 'ukraine', 'united arab emirates', 'united kingdom', 'united states']

conn = sqlite3.connect(sqlite_db)
c = conn.cursor()

# drop_table(c)

c.execute('''CREATE TABLE IF NOT EXISTS contacts
             (First_Name TEXT, Last_Name TEXT, Email TEXT, Secondary_Email TEXT,
             Tertiary_Email TEXT, LinkedIn_URL TEXT, Mobile TEXT, Work_Phone TEXT, Country TEXT)''')
conn.commit()

record_counter = 0

with open(input_file, 'r') as infile:
    for line in infile:
        entry = json.loads(line)
        contact_data = process_entry(entry)

        if contact_data:
            c.execute('''INSERT INTO contacts (First_Name, Last_Name, Email, Secondary_Email,
                         Tertiary_Email, LinkedIn_URL, Mobile, Work_Phone, Country)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                         (contact_data['First_Name'], contact_data['Last_Name'], contact_data['Email'],
                          contact_data['Secondary_Email'], contact_data['Tertiary_Email'], contact_data['LinkedIn_URL'],
                          contact_data['Mobile'], contact_data['Work_Phone'], contact_data['Country']))
            conn.commit()
            record_counter += 1

            if record_counter % 1000 == 0:
                print(f"{record_counter} записей добавлено в базу данных.")

conn.close()
