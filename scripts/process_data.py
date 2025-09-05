import pandas as pd
import numpy as np
from datetime import datetime
import json
import os
import requests
from io import BytesIO

def main():
    try:
        print("Начинаем обработку данных...")
        
        # ===== ЗАГРУЗКА ФАЙЛОВ ИЗ ВНЕШНИХ ИСТОЧНИКОВ =====
        print("Загружаем файлы с внешних URL...")
        
        # URL ваших файлов
        PRICE_URL = "https://b24.engpx.ru/~1PZm1"  # Прайс
        STOCK_URL = "https://b24.engpx.ru/~8G8gh"  # Наличие
        
        # Загрузка прайса
        print("Загружаем прайс-лист...")
        price_response = requests.get(PRICE_URL)
        price_response.raise_for_status()  # Проверка ошибок
        price_file = BytesIO(price_response.content)
        
        # Загрузка остатков
        print("Загружаем данные остатков...")
        stock_response = requests.get(STOCK_URL)
        stock_response.raise_for_status()  # Проверка ошибок
        stock_file = BytesIO(stock_response.content)
        
        # ===== ОБРАБОТКА ДАННЫХ =====
        print("Обрабатываем данные...")
        
        # Чтение Excel файлов с явным указанием движка
        price_df = pd.read_excel(price_file, sheet_name='Прайс-лист', engine='openpyxl')
        stock_df = pd.read_excel(stock_file, sheet_name='Лист_1', engine='openpyxl')

        # ===== ОБРАБОТКА ПРАЙС-ЛИСТА =====
        print("Обрабатываем прайс-лист...")
        price_df = price_df[['Артикул', 'Товар', 'Розничная']].copy()
        price_df.columns = ['Артикул', 'Модель', 'Цена']
        
        # Очистка данных
        price_df = price_df.iloc[1:].reset_index(drop=True)
        price_df = price_df.dropna(subset=['Артикул'])
        price_df['Артикул'] = price_df['Артикул'].astype(str).str.strip()
        price_df = price_df.drop_duplicates('Артикул')
        
        # Преобразуем цену в число
        price_df['Цена'] = pd.to_numeric(price_df['Цена'], errors='coerce').fillna(0)

        # ===== ОБРАБОТКА ОСТАТКОВ =====
        print("Обрабатываем остатки...")
        # Фильтруем только строки с артикулами
        stock_df = stock_df[stock_df.iloc[:, 0].astype(str).str.contains(r'^\d', na=False)]
        stock_df['Артикул'] = stock_df.iloc[:, 0].astype(str).str.strip()
        
        # Берем колонку H (индекс 7) - "В наличии"
        stock_df = stock_df.rename(columns={stock_df.columns[7]: 'В_наличии'})
        stock_df = stock_df[['Артикул', 'В_наличии']]
        
        # Очистка и обработка отрицательных значений
        stock_df['В_наличии'] = pd.to_numeric(stock_df['В_наличии'], errors='coerce').fillna(0)
        stock_df['В_наличии'] = stock_df['В_наличии'].apply(lambda x: max(0, x) if pd.notnull(x) else 0)
        stock_df['В_наличии'] = stock_df['В_наличии'].astype(int)

        # ===== ОБЪЕДИНЕНИЕ ДАННЫХ =====
        print("Объединяем данные...")
        merged_df = pd.merge(price_df, stock_df, on='Артикул', how='left')
        merged_df['В_наличии'] = merged_df['В_наличии'].fillna(0).astype(int)

        # Добавляем колонку "Статус наличия"
        def get_stock_status(row):
            if row['В_наличии'] > 0:
                return 'В наличии'
            else:
                return 'Нет в наличии'

        merged_df['Статус'] = merged_df.apply(get_stock_status, axis=1)

        # ===== ПОДГОТОВКА ДАННЫХ ДЛЯ JSON =====
        print("Подготавливаем данные для JSON...")
        # Убираем .0 из артикулов
        merged_df['Артикул'] = merged_df['Артикул'].astype(str).str.replace(r'\.0$', '', regex=True)
        
        # Преобразуем в список словарей
        data_for_json = merged_df.to_dict('records')
        
        # Убираем NaN значения и преобразуем типы
        for item in data_for_json:
            item['Цена'] = float(item['Цена']) if pd.notnull(item['Цена']) else 0.0
            item['В_наличии'] = int(item['В_наличии'])

        # ===== СОХРАНЕНИЕ В JSON =====
        print("Сохраняем в JSON...")
        with open('data.json', 'w', encoding='utf-8') as f:
            json.dump(data_for_json, f, ensure_ascii=False, indent=2)
        
        print("✅ data.json успешно создан!")
        print(f"Обработано позиций: {len(data_for_json)}")
        
        # Также сохраняем Excel для отладки
        current_date = datetime.now().strftime('%Y-%m-%d')
        output_filename = f'Сводная_таблица_котлы_{current_date}.xlsx'
        
        with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
            merged_df.to_excel(writer, sheet_name='Сводная таблица', index=False)
            
            summary_df = pd.DataFrame({
                'Показатель': ['Всего позиций', 'В наличии', 'Нет в наличии'],
                'Количество': [
                    len(merged_df),
                    len(merged_df[merged_df['Статус'] == 'В наличии']),
                    len(merged_df[merged_df['Статус'] == 'Нет в наличии'])
                ]
            })
            summary_df.to_excel(writer, sheet_name='Итоги', index=False)
        
        print(f"✅ {output_filename} также создан для отладки")
        
        # Выводим пример данных для проверки
        print("\nПример обработанных данных (первые 3 записи):")
        for i, item in enumerate(data_for_json[:3]):
            print(f"{i+1}. {item['Модель']} - {item['Цена']} руб. - {item['В_наличии']} шт.")
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Ошибка загрузки файлов: {e}")
        print("Проверьте URL адреса и доступность файлов")
        raise
        
    except pd.errors.EmptyDataError as e:
        print(f"❌ Ошибка: файлы пустые или не содержат данных: {e}")
        raise
        
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {str(e)}")
        print("Тип ошибки:", type(e).__name__)
        raise

if __name__ == "__main__":
    main()
