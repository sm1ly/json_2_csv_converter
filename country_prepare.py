import json
import os

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

input_file = 'prepare_v01.json'
output_dir = 'cutted_files'
output_file_prefix = 'prepare_v02_'
max_lines_per_file = 500000
file_counter = 1

COUNTRIES = ['united states', 'sweden']

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

with open(input_file, 'r') as infile:
    outfile = open(f"{output_dir}/{output_file_prefix}{file_counter}.json", 'w')
    line_counter = 0

    for line in infile:
        entry = json.loads(line)
        contact_data = process_entry(entry)

        if contact_data:
            outfile.write(json.dumps(contact_data) + '\n')
            line_counter += 1

            if line_counter >= max_lines_per_file:
                outfile.close()
                file_counter += 1
                outfile = open(f"{output_dir}/{output_file_prefix}{file_counter}.json", 'w')
                line_counter = 0

    outfile.close()
