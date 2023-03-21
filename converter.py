#!/usr/bin/python3

import json
import csv
import asyncio
import os

# Список разрешенных стран
COUNTRIES = ['argentina', 'armenia', 'australia', 'austria', 'azerbaijan', 'belarus', 'belgium', 'brazil',
             'bulgaria', 'canada', 'croatia', 'cyprus', 'czechia', 'denmark', 'estonia', 'finland', 'france',
             'georgia', 'germany', 'gibraltar', 'greece', 'hong kong', 'hungary', 'iceland', 'ireland', 'israel',
             'italy', 'kazakhstan', 'kyrgyzstan', 'latvia', 'liechtenstein', 'lithuania', 'luxembourg', 'malta',
             'moldova', 'monaco', 'montenegro', 'netherlands', 'new zealand', 'norway', 'poland', 'portugal',
             'russia', 'saudi arabia', 'serbia', 'singapore', 'slovakia', 'slovenia', 'spain', 'sweden',
             'switzerland', 'turkey', 'ukraine', 'united arab emirates', 'united kingdom', 'united states']

# Размер блока записи для каждого файла
CHUNK_SIZE = 19999


# Функция для извлечения страны из поля "a" JSON-объекта
def get_country(location):
    if location:
        country = location.split(', ')[-1]
        return country if country in COUNTRIES else ''
    return ''

# Асинхронная функция для конвертации JSON в CSV
async def json_to_csv(input_file, output_directory, output_file_prefix):
    # Открываем входной файл на чтение
    with open(input_file, 'r') as infile:
        # Заголовки полей для CSV-файла
        fieldnames = ['first_name', 'second_name', 'email', 'email2', 'linkedin_url', 'phone', 'phone2', 'country']

        chunk_count = 0
        valid_rows_written = 0
        outfile = None
        writer = None

        # Создаем выходную директорию, если ее не существует
        os.makedirs(output_directory, exist_ok=True)

        # Читаем строки из входного файла
        for line in infile:
            # Разбираем строку JSON
            entry = json.loads(line)
            data = {}
            # Получаем страну из поля "a"
            data['country'] = get_country(entry.get('a', ''))

            # Если страна не разрешена, пропускаем строку
            if not data['country']:
                continue

            # Если количество записанных строк кратно размеру блока, начинаем новый файл
            if valid_rows_written % CHUNK_SIZE == 0:
                if outfile:
                    outfile.close()
                chunk_count += 1
                padded_chunk_count = str(chunk_count).zfill(5)
                outfile_path = os.path.join(output_directory, f"{output_file_prefix}_{padded_chunk_count}.csv")
                outfile = open(outfile_path, 'w', newline='')
                writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter=';')
                writer.writeheader()

            # Заполняем данные для CSV-файла
            data['phone'], data['phone2'] = (entry['t'][:2] + [''] * 2)[:2] if 't' in entry else ('', '')
            data['email'], data['email2'] = (entry['e'][:2] + [''] * 2)[:2] if 'e' in entry else ('', '')
            data['linkedin_url'] = entry.get('linkedin', '')
            names = entry.get('n', '').split(' ', 1)
            data['first_name'] = names[0] if names else ''
            data['second_name'] = names[1] if len(names) > 1 else ''

            # Записываем данные в CSV-файл
            writer.writerow(data)
            valid_rows_written += 1

        # Закрываем последний выходной файл
        if outfile:
            outfile.close()

# Имя входного файла и выходной директории
input_file = 'data.json'
output_directory = 'output_directory'
output_file_prefix = 'output'

# Запускаем асинхронную функцию для конвертации JSON в CSV
loop = asyncio.get_event_loop()
loop.run_until_complete(json_to_csv(input_file, output_directory, output_file_prefix))
