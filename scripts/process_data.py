import pandas as pd
import json
import re

def main():
    try:
        print("Загрузка данных из Google Sheets...")
        
        # Прямые ссылки на CSV экспорт Google Sheets
        price_url = "https://docs.google.com/spreadsheets/d/19PRNpA6F_HMI6iHSCg2iJF52PnN203ckY1WnqY_t5fc/export?format=csv"
        stock_url = "https://docs.google.com/spreadsheets/d/1o0e3-E20mQsWToYVQpCHZgLcbizCafLRpoPdxr8Rqfw/export?format=csv"
        
        # Загружаем данные
        print("Загружаем прайс...")
        price_df = pd.read_csv(price_url)
        
        print("Загружаем остатки...")
        stock_df = pd.read_csv(stock_url)
        
        print("Обработка данных...")
        
        # Автоматически находим нужные колонки
        def find_column(df, possible_names):
            for col in df.columns:
                col_lower = str(col).lower()
                if any(name.lower() in col_lower for name in possible_names):
                    return col
            return None
        
        # Для прайса
        article_col_price = find_column(price_df, ['артикул', 'article', 'код', 'articul', 'sku'])
        name_col = find_column(price_df, ['товар', 'наименование', 'модель', 'name', 'product', 'название'])
        price_col = find_column(price_df, ['розничная', 'цена', 'price', 'retail', 'стоимость', 'руб'])
        
        print(f"Колонки в прайсе: Артикул='{article_col_price}', Модель='{name_col}', Цена='{price_col}'")
        
        # Для остатков
        article_col_stock = find_column(stock_df, ['артикул', 'article', 'код', 'articul', 'sku'])
        stock_col = find_column(stock_df, ['в наличии', 'остаток', 'количество', 'quantity', 'stock', 'наличие', 'кол-во'])
        
        print(f"Колонки в остатках: Артикул='{article_col_stock}', Наличие='{stock_col}'")
        
        # Создаем копии с нужными колонками
        price_clean = price_df[[article_col_price, name_col, price_col]].copy()
        price_clean.columns = ['Артикул', 'Модель', 'Цена']
        
        stock_clean = stock_df[[article_col_stock, stock_col]].copy()
        stock_clean.columns = ['Артикул', 'В_наличии']
        
        # Очистка данных
        price_clean = price_clean.dropna(subset=['Артикул'])
        price_clean['Артикул'] = price_clean['Артикул'].astype(str).str.strip()
        
        stock_clean = stock_clean.dropna(subset=['Артикул'])
        stock_clean['Артикул'] = stock_clean['Артикул'].astype(str).str.strip()
        
        # Обработка количества
        def parse_quantity(qty):
            try:
                qty = float(str(qty).replace(' ', '').replace(',', '.'))
                return max(0, int(qty))
            except:
                return 0
        
        stock_clean['В_наличии'] = stock_clean['В_наличии'].apply(parse_quantity)
        
        # Обработка цены
        def parse_price(price):
            try:
                price = str(price).replace(' ', '').replace(',', '.')
                price = re.sub(r'[^\d\.]', '', price)
                return float(price)
            except:
                return 0.0
        
        price_clean['Цена'] = price_clean['Цена'].apply(parse_price)
        
        # Объединяем данные
        merged_df = pd.merge(price_clean, stock_clean, on='Артикул', how='left')
        merged_df['В_наличии'] = merged_df['В_наличии'].fillna(0).astype(int)
        
        # Добавляем дополнительные поля
        def extract_info(model):
            model_str = str(model).upper()
            
            # Мощность (ищем числа)
            power_match = re.search(r'(\d+)\s*(кВт|KW|C|H|С|Х)', model_str)
            power = power_match.group(1) if power_match else "Не указана"
            
            # Контуры
            contours = "Двухконтурный" if ' C' in model_str or 'С ' in model_str else "Одноконтурный"
            
            # Wi-Fi
            wifi = "Да" if any(x in model_str for x in ['WI-FI', 'WIFI', 'ВАЙ-ФАЙ', 'WI FI']) else "Нет"
            
            return power, contours, wifi
        
        # Применяем функцию
        merged_df[['Мощность', 'Контуры', 'WiFi']] = merged_df['Модель'].apply(
            lambda x: pd.Series(extract_info(x))
        )
        
        # Добавляем статус
        merged_df['Статус'] = merged_df['В_наличии'].apply(lambda x: 'В наличии' if x > 0 else 'Нет в наличии')
        
        # Конвертируем в JSON
        result = merged_df.to_dict('records')
        
        # Сохраняем
        with open('data.json', 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Готово! Обработано {len(result)} товаров")
        print(f"📊 В наличии: {sum(1 for x in result if x['В_наличии'] > 0)} товаров")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        
        # Создаем пустой файл чтобы сайт не сломался
        with open('data.json', 'w', encoding='utf-8') as f:
            json.dump([], f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
