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
        print("Колонки в прайсе:", price_df.columns.tolist())
        
        print("Загружаем остатки...")
        stock_df = pd.read_csv(stock_url)
        print("Колонки в остатках:", stock_df.columns.tolist())
        
        print("Обработка данных...")
        
        # Автоматически находим нужные колонки
        def find_column(df, possible_names):
            for col in df.columns:
                col_lower = str(col).lower()
                if any(name.lower() in col_lower for name in possible_names):
                    return col
            return None
        
        # Находим колонки в прайсе
        article_col_price = find_column(price_df, ['артикул', 'article', 'код', 'articul', 'sku'])
        name_col = find_column(price_df, ['товар', 'наименование', 'модель', 'name', 'product', 'название'])
        price_col = find_column(price_df, ['розничная', 'цена', 'price', 'retail', 'стоимость', 'руб'])
        
        print(f"Найдены колонки в прайсе: Артикул='{article_col_price}', Название='{name_col}', Цена='{price_col}'")
        
        # Находим колонки в остатках
        article_col_stock = find_column(stock_df, ['артикул', 'article', 'код', 'articul', 'sku'])
        stock_col = find_column(stock_df, ['в наличии', 'остаток', 'количество', 'quantity', 'stock', 'наличие', 'кол-во'])
        
        print(f"Найдены колонки в остатках: Артикул='{article_col_stock}', Наличие='{stock_col}'")
        
        if not all([article_col_price, name_col, price_col, article_col_stock, stock_col]):
            raise ValueError("Не найдены все необходимые колонки в таблицах")
        
        # Создаем чистые датафреймы
        price_clean = price_df[[article_col_price, name_col, price_col]].copy()
        price_clean.columns = ['Артикул', 'Модель', 'Цена']
        
        stock_clean = stock_df[[article_col_stock, stock_col]].copy()
        stock_clean.columns = ['Артикул', 'В_наличии']
        
        # Очистка данных
        price_clean = price_clean.dropna(subset=['Артикул'])
        price_clean['Артикул'] = price_clean['Артикул'].astype(str).str.strip()
        
        stock_clean = stock_clean.dropna(subset=['Артикул'])
        stock_clean['Артикул'] = stock_clean['Артикул'].astype(str).str.strip()
        
        # Обработка цены
        def parse_price(price):
            try:
                if pd.isna(price):
                    return 0.0
                price_str = str(price).replace(' ', '').replace(',', '.')
                price_str = re.sub(r'[^\d\.]', '', price_str)
                return float(price_str)
            except:
                return 0.0
        
        price_clean['Цена'] = price_clean['Цена'].apply(parse_price)
        
        # Обработка количества
        def parse_quantity(qty):
            try:
                if pd.isna(qty):
                    return 0
                qty_str = str(qty).replace(' ', '').replace(',', '.')
                qty_val = float(qty_str)
                return max(0, int(qty_val))
            except:
                return 0
        
        stock_clean['В_наличии'] = stock_clean['В_наличии'].apply(parse_quantity)
        
        # Объединяем данные
        merged_df = pd.merge(price_clean, stock_clean, on='Артикул', how='left')
        merged_df['В_наличии'] = merged_df['В_наличии'].fillna(0).astype(int)
        
        # Функция для определения фото по модели
        def get_image_for_model(model_name):
            """Определяет какое фото использовать для модели"""
            model = str(model_name).upper()
            
            # Правила сопоставления моделей с фото
            if 'METEOR T2' in model:
                return 'images/METEOR_T2.jpg'
            elif 'METEOR C30' in model:
                return 'images/METEOR_C30.jpg'
            elif 'METEOR B30' in model:
                return 'images/METEOR_B30.jpg'
            elif 'METEOR B20' in model:
                return 'images/METEOR_B20.jpg'
            elif 'METEOR C11' in model:
                return 'images/METEOR_C11.jpg'
            elif 'METEOR Q3' in model:
                return 'images/METEOR_Q3.jpg'
            elif 'METEOR M30' in model:
                return 'images/METEOR_M30.jpg'
            elif 'METEOR M6' in model:
                return 'images/METEOR_M6.jpg'
            elif 'LAGGARTT' in model or 'ГАЗ 6000' in model:
                return 'images/LaggarTT.jpg'
            elif 'DEVOTION' in model:
                return 'images/Devotion.jpg'
            elif 'MK' in model:
                return 'images/MK.jpg'
            else:
                return 'images/default.jpg'
        
        # Добавляем дополнительные поля
        def extract_info(model):
            model_str = str(model).upper()
            
            # Мощность (ищем числа)
            power_match = re.search(r'(\d+)\s*(кВт|KW|C|H|С|Х)', model_str)
            power = power_match.group(1) if power_match else "Не указана"
            
            # Контуры - ИСПРАВЛЕНО: "C" - двухконтурный, "H" - одноконтурный
            if ' C' in model_str or 'С ' in model_str:
                contours = "Двухконтурный"
            elif ' H' in model_str or 'Н ' in model_str:
                contours = "Одноконтурный"
            else:
                # Для напольных котлов и других определяем по контексту
                if 'НАПОЛЬНЫЙ' in model_str or 'MK' in model_str:
                    contours = "Одноконтурный"
                else:
                    contours = "Двухконтурный"  # по умолчанию для настенных
            
            # Wi-Fi
            wifi = "Да" if any(x in model_str for x in ['WI-FI', 'WIFI', 'ВАЙ-ФАЙ', 'WI FI']) else "Нет"
            
            return power, contours, wifi
        
        # Применяем функции к каждой модели
        merged_df[['Мощность', 'Контуры', 'WiFi']] = merged_df['Модель'].apply(
            lambda x: pd.Series(extract_info(x))
        )
        
        # Добавляем фото
        merged_df['Фото'] = merged_df['Модель'].apply(get_image_for_model)
        
        # Добавляем статус
        merged_df['Статус'] = merged_df['В_наличии'].apply(lambda x: 'В наличии' if x > 0 else 'Нет в наличии')
        
        # Конвертируем в JSON
        result = merged_df.to_dict('records')
        
        # Сохраняем
        with open('data.json', 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Готово! Обработано {len(result)} товаров")
        print(f"📊 В наличии: {sum(1 for x in result if x['В_наличии'] > 0)} товаров")
        
        # Покажем пример данных
        print("\nПример данных (первые 3 товара):")
        for i, item in enumerate(result[:3]):
            print(f"{i+1}. {item['Артикул']} - {item['Модель'][:30]}...")
            print(f"   Цена: {item['Цена']} руб., Наличие: {item['В_наличии']} шт.")
            print(f"   Контуры: {item['Контуры']}, Мощность: {item['Мощность']} кВт, Wi-Fi: {item['WiFi']}")
            print(f"   Фото: {item['Фото']}")
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        
        # Создаем пустой файл чтобы сайт не сломался
        with open('data.json', 'w', encoding='utf-8') as f:
            json.dump([], f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
