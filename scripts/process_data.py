import pandas as pd
import numpy as np
from datetime import datetime
import json
import requests
from io import BytesIO
import re

def main():
    try:
        print("Начинаем обработку данных из Google Sheets...")
        
        # ===== НАСТРОЙКА GOOGLE SHEETS =====
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
        
        # ===== ОТЛАДОЧНАЯ ИНФОРМАЦИЯ =====
        print("\n=== ИНФОРМАЦИЯ О КОЛОНКАХ ===")
        print("Колонки в прайсе:", price_df.columns.tolist())
        print("Колонки в остатках:", stock_df.columns.tolist())
        print("Первые 3 строки прайса:")
        print(price_df.head(3))
        print("\nПервые 3 строки остатков:")
        print(stock_df.head(3))
        print("==============================\n")
        
        # ===== ОБРАБОТКА ПРАЙС-ЛИСТА =====
        print("Обрабатываем прайс-лист...")
        
        # Автоматически находим нужные колонки
        def find_column(df, possible_names):
            for col in df.columns:
                col_lower = str(col).lower()
                if any(name.lower() in col_lower for name in possible_names):
                    return col
            return None
        
        # Для прайса
        article_col = find_column(price_df, ['артикул', 'article', 'код', 'articul', 'sku'])
        name_col = find_column(price_df, ['товар', 'наименование', 'модель', 'name', 'product', 'название'])
        price_col = find_column(price_df, ['розничная', 'цена', 'price', 'retail', 'стоимость', 'руб'])
        
        print(f"Найдены колонки в прайсе: Артикул='{article_col}', Товар='{name_col}', Цена='{price_col}'")
        
        if not all([article_col, name_col, price_col]):
            raise ValueError("Не найдены все необходимые колонки в прайсе")
        
        # Создаем копию только с нужными колонками
        price_df = price_df[[article_col, name_col, price_col]].copy()
        price_df.columns = ['Артикул', 'Модель', 'Цена']
        
        # Очистка данных прайса
        price_df = price_df.dropna(subset=['Артикул'])
        price_df['Артикул'] = price_df['Артикул'].astype(str).str.strip()
        price_df = price_df.drop_duplicates('Артикул')
        
        # Преобразуем цену в число
        def parse_price(price_str):
            if pd.isna(price_str):
                return 0.0
            try:
                # Убираем пробелы (тысячные разделители) и запятые (десятичные)
                price_str = str(price_str).replace(' ', '').replace(',', '.')
                # Убираем все нечисловые символы кроме точки
                price_str = re.sub(r'[^\d\.]', '', price_str)
                return float(price_str)
            except:
                return 0.0
        
        price_df['Цена'] = price_df['Цена'].apply(parse_price)

        # ===== ОБРАБОТКА ОСТАТКОВ =====
        print("Обрабатываем остатки...")
        
        # Находим колонки в остатках
        stock_article_col = find_column(stock_df, ['артикул', 'article', 'код', 'articul', 'sku'])
        stock_qty_col = find_column(stock_df, ['в наличии', 'остаток', 'количество', 'quantity', 'stock', 'наличие', 'кол-во'])
        
        print(f"Найдены колонки в остатках: Артикул='{stock_article_col}', Наличие='{stock_qty_col}'")
        
        if not all([stock_article_col, stock_qty_col]):
            raise ValueError("Не найдены все необходимые колонки в остатках")
        
        # Создаем копию только с нужными колонками
        stock_df = stock_df[[stock_article_col, stock_qty_col]].copy()
        stock_df.columns = ['Артикул', 'В_наличии']
        
        # Очистка данных остатков
        stock_df = stock_df.dropna(subset=['Артикул'])
        stock_df['Артикул'] = stock_df['Артикул'].astype(str).str.strip()
        stock_df = stock_df.drop_duplicates('Артикул')
        
        # Обработка количества - исправляем отрицательные значения
        def parse_quantity(qty_str):
            if pd.isna(qty_str):
                return 0
            try:
                qty = float(str(qty_str).replace(' ', '').replace(',', '.'))
                # Отрицательные значения становятся 0
                return max(0, int(qty))
            except:
                return 0
        
        stock_df['В_наличии'] = stock_df['В_наличии'].apply(parse_quantity)

        # ===== ОЧИСТКА И НОРМАЛИЗАЦИЯ АРТИКУЛОВ =====
        print("Очищаем и нормализуем артикулы...")
        
        def clean_article(article):
            """Очистка и нормализация артикула"""
            if pd.isna(article):
                return None
            article = str(article).strip()
            # Убираем .0 в конце (преобразование из float)
            if article.endswith('.0'):
                article = article[:-2]
            # Убираем лишние пробелы и непечатаемые символы
            article = re.sub(r'\s+', '', article)
            return article
        
        # Применяем очистку к артикулам
        price_df['Артикул_чистый'] = price_df['Артикул'].apply(clean_article)
        stock_df['Артикул_чистый'] = stock_df['Артикул'].apply(clean_article)
        
        # ===== ОБЪЕДИНЕНИЕ ДАННЫХ =====
        print("Объединяем данные...")
        
        # Объединяем по очищенным артикулам (LEFT JOIN чтобы сохранить все товары из прайса)
        merged_df = pd.merge(
            price_df, 
            stock_df, 
            left_on='Артикул_чистый', 
            right_on='Артикул_чистый', 
            how='left',
            suffixes=('_price', '_stock')
        )
        
        # Заполняем пропущенные значения
        merged_df['В_наличии'] = merged_df['В_наличии'].fillna(0).astype(int)
        
        # Используем оригинальный артикул из прайса
        merged_df['Артикул'] = merged_df['Артикул_price']
        merged_df['Модель'] = merged_df['Модель'].fillna('Неизвестная модель')
        
        # Убираем временные колонки
        final_columns = ['Артикул', 'Модель', 'Цена', 'В_наличии']
        merged_df = merged_df[final_columns]
        
        print(f"После объединения: {len(merged_df)} записей")
        matched_count = (merged_df['В_наличии'] > 0).sum()
        print(f"Товаров в наличии: {matched_count}")

        # Добавляем колонку "Статус наличия"
        def get_stock_status(row):
            if row['В_наличии'] > 0:
                return 'В наличии'
            else:
                return 'Нет в наличии'

        merged_df['Статус'] = merged_df.apply(get_stock_status, axis=1)

        # ===== ПОДГОТОВКА ДАННЫХ ДЛЯ JSON =====
        print("Подготавливаем данные для JSON...")
        
        # Преобразуем в список словарей
        data_for_json = merged_df.to_dict('records')
        
        # Убираем .0 из артикулов и преобразуем типы
        for item in data_for_json:
            # Исправляем артикулы (убираем .0)
            if 'Артикул' in item and str(item['Артикул']).endswith('.0'):
                item['Артикул'] = str(item['Артикул'])[:-2]
            
            item['Цена'] = float(item['Цена']) if pd.notnull(item['Цена']) else 0.0
            item['В_наличии'] = int(item['В_наличии'])

        # ===== СОХРАНЕНИЕ В JSON =====
        print("Сохраняем в JSON...")
        with open('data.json', 'w', encoding='utf-8') as f:
            json.dump(data_for_json, f, ensure_ascii=False, indent=2)
        
        print("✅ data.json успешно создан!")
        print(f"Обработано позиций: {len(data_for_json)}")
        print(f"В наличии: {matched_count} позиций")
        print(f"Нет в наличии: {len(data_for_json) - matched_count} позиций")
        
        # Выводим пример данных для проверки
        print("\nПример обработанных данных (первые 10 записей):")
        for i, item in enumerate(data_for_json[:10]):
            status = "✅" if item.get('В_наличии', 0) > 0 else "❌"
            print(f"{status} {i+1:2d}. {item.get('Артикул')} - {item.get('Модель', 'Нет названия')[:30]}... - {item.get('Цена', 0):.2f} руб. - {item.get('В_наличии', 0)} шт.")
        
        # Также сохраняем Excel для отладки
        current_date = datetime.now().strftime('%Y-%m-%d')
        output_filename = f'Сводная_таблица_котлы_{current_date}.xlsx'
        
        with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
            merged_df.to_excel(writer, sheet_name='Сводная таблица', index=False)
            
            summary_df = pd.DataFrame({
                'Показатель': ['Всего позиций', 'В наличии', 'Нет в наличии'],
                'Количество': [
                    len(merged_df),
                    matched_count,
                    len(merged_df) - matched_count
                ]
            })
            summary_df.to_excel(writer, sheet_name='Итоги', index=False)
        
        print(f"✅ {output_filename} также создан для отладки")
            
    except Exception as e:
        print(f"❌ Ошибка: {str(e)}")
        print("Тип ошибки:", type(e).__name__)
        import traceback
        traceback.print_exc()
        
        # В случае ошибки создаем пустой файл
        print("Создаем пустой data.json для избежания ошибок на сайте...")
        with open('data.json', 'w', encoding='utf-8') as f:
            json.dump([], f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
