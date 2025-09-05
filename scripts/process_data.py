import pandas as pd
import numpy as np
from datetime import datetime
import json
import requests

def main():
    try:
        print("Начинаем обработку данных...")
        
        # ===== НАСТРОЙКА GOOGLE SHEETS =====
        # ЗАМЕНИТЕ ЭТИ ID НА ВАШИ!
        PRICE_SHEET_ID = "19PRNpA6F_HMI6iHSCg2iJF52PnN203ckY1WnqY_t5fc"
        STOCK_SHEET_ID = "1o0e3-E20mQsWToYVQpCHZgLcbizCafLRpoPdxr8Rqfw"
        
        # Формируем URL для экспорта в CSV
        price_csv_url = f"https://docs.google.com/spreadsheets/d/{PRICE_SHEET_ID}/export?format=csv"
        stock_csv_url = f"https://docs.google.com/spreadsheets/d/{STOCK_SHEET_ID}/export?format=csv"
        
        print("Загружаем данные из Google Таблиц...")
        
        # ===== ЗАГРУЗКА ДАННЫХ =====
        print("Загружаем прайс-лист...")
        price_df = pd.read_csv(price_csv_url)
        
        print("Загружаем данные остатков...")
        stock_df = pd.read_csv(stock_csv_url)
        
        # ===== ОБРАБОТКА ПРАЙС-ЛИСТА =====
        print("Обрабатываем прайс-лист...")
        
        # Автоматически находим нужные колонки
        def find_column(df, possible_names):
            for col in df.columns:
                col_lower = str(col).lower()
                if any(name.lower() in col_lower for name in possible_names):
                    return col
            return df.columns[1]  # Вторая колонка если не нашли
        
        article_col = find_column(price_df, ['артикул', 'article', 'код'])
        name_col = find_column(price_df, ['товар', 'наименование', 'модель', 'name', 'product'])
        price_col = find_column(price_df, ['розничная', 'цена', 'price', 'retail'])
        
        print(f"Найдены колонки в прайсе: Артикул={article_col}, Товар={name_col}, Цена={price_col}")
        
        price_df = price_df[[article_col, name_col, price_col]].copy()
        price_df.columns = ['Артикул', 'Модель', 'Цена']
        
        # Очистка данных
        price_df = price_df.dropna(subset=['Артикул'])
        price_df['Артикул'] = price_df['Артикул'].astype(str).str.strip()
        price_df = price_df.drop_duplicates('Артикул')
        
        # Преобразуем цену в число
        price_df['Цена'] = pd.to_numeric(price_df['Цена'], errors='coerce').fillna(0)

        # ===== ОБРАБОТКА ОСТАТКОВ =====
        print("Обрабатываем остатки...")
        
        # Находим колонки в остатках
        stock_article_col = find_column(stock_df, ['артикул', 'article', 'код'])
        stock_qty_col = find_column(stock_df, ['в наличии', 'остаток', 'количество', 'quantity', 'stock'])
        
        print(f"Найдены колонки в остатках: Артикул={stock_article_col}, Наличие={stock_qty_col}")
        
        # Фильтруем только строки с артикулами
        stock_df = stock_df[stock_df[stock_article_col].astype(str).str.contains(r'^\d', na=False)]
        stock_df['Артикул'] = stock_df[stock_article_col].astype(str).str.strip()
        
        stock_df = stock_df.rename(columns={stock_qty_col: 'В_наличии'})
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
            print(f"{i+1}. {item.get('Модель', 'Нет названия')} - {item.get('Цена', 0)} руб. - {item.get('В_наличии', 0)} шт. - {item.get('Статус', 'Неизвестно')}")
            
    except Exception as e:
        print(f"❌ Ошибка: {str(e)}")
        print("Тип ошибки:", type(e).__name__)
        import traceback
        traceback.print_exc()
        
        # Создаем минимальные тестовые данные даже при ошибке
        create_fallback_data()

def create_fallback_data():
    """Создает временные данные чтобы сайт работал"""
    fallback_data = [
        {
            "Артикул": "10680202001",
            "Модель": "Котел настенный Meteor B20 18 C", 
            "Цена": 37848,
            "В_наличии": 3,
            "Статус": "В наличии"
        }
    ]
    
    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(fallback_data, f, ensure_ascii=False, indent=2)
    print("✅ Создан временный data.json")

if __name__ == "__main__":
    main()
