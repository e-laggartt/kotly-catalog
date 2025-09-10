import pandas as pd
import json
import re
import os

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
        
        # Функция для определения типа товара
        def determine_type(model_name):
            """Определяет тип товара на основе названия"""
            model = str(model_name).upper()
            
            if "КОТЕЛ" in model:
                return "boiler"
            elif "БОЙЛЕР" in model:
                return "water_heater"
            elif "ДЫМОХОД" in model or "АДАПТЕР" in model:
                return "chimney"
            elif "ДАТЧИК" in model or "КОМПЛЕКТ" in model:
                return "accessory"
            else:
                return "boiler"  # По умолчанию, если не определили
        
        # Добавляем тип товара
        merged_df['type'] = merged_df['Модель'].apply(determine_type)
        
        # Функция для извлечения характеристик в зависимости от типа
        def extract_info(row):
            model = str(row['Модель']).upper()
            product_type = row['type']
            
            # Инициализируем все возможные характеристики
            power = ""
            contours = ""
            wifi = ""
            volume = ""
            diameter = ""
            chimney_type = ""
            
            if product_type == "boiler":
                # Мощность - ищем число перед C/H/С/Х или в артикуле для Devotion/MK
                power_match = re.search(r'(\d+)\s*(кВт|KW|C|H|С|Х)', model)
                if power_match:
                    power = power_match.group(1)
                else:
                    # Для Devotion (LL1GBQ30-M6) и MK (MK 500)
                    if "DEVOTION" in model or "MK" in model:
                        # Ищем число в артикуле или в названии
                        digits = re.search(r'(\d+)', row['Артикул'] if row['Артикул'] else model)
                        if digits:
                            power = digits.group(1)
                    else:
                        power = "Не указана"
                
                # Контуры - явное исключение для LN1GBQ60 и логика для MK/Devotion
                if "LN1GBQ60" in model or "T2" in model or "MK" in model:
                    contours = "Одноконтурный"
                elif " C " in model or "С " in model or "M6" in model:
                    contours = "Двухконтурный"
                elif " H " in model or "Н " in model:
                    contours = "Одноконтурный"
                else:
                    contours = "Двухконтурный"  # По умолчанию для настенных
                
                # Wi-Fi
                if any(x in model for x in ['WI-FI', 'WIFI', 'ВАЙ-ФАЙ', 'WI FI']):
                    wifi = "Да"
                else:
                    wifi = "Нет"
            
            elif product_type == "water_heater":
                # Объем - извлекаем число после G
                vol_match = re.search(r'G\s*(\d+)', model)
                if vol_match:
                    volume = vol_match.group(1)
                else:
                    volume = ""
            
            elif product_type == "chimney":
                # Диаметр - DN60/100, DN80/125
                diam_match = re.search(r'(DN\d+/\d+)', model)
                if diam_match:
                    diameter = diam_match.group(1)
                else:
                    diameter = ""
                
                # Тип дымохода
                if "PP" in model:
                    chimney_type = "Конденсационный"
                else:
                    chimney_type = "Обычный"
            
            # Для accessory ничего не делаем, все поля остаются пустыми
            
            return pd.Series([power, contours, wifi, volume, diameter, chimney_type])
        
        # Применяем функцию извлечения характеристик
        merged_df[['Мощность', 'Контуры', 'WiFi', 'Объем', 'Диаметр', 'Тип_дымохода']] = merged_df.apply(extract_info, axis=1)
        
        # Функция для определения фото по модели и типу
        def get_image_for_model(row):
            """Определяет какое фото использовать для модели"""
            model = str(row['Модель']).upper()
            product_type = row['type']
            
            # Правила для котлов (boiler)
            if product_type == "boiler":
                if 'METEOR T2' in model:
                    return 'images/meteor-t2.jpg'
                elif 'METEOR C30' in model:
                    return 'images/meteor-c30.jpg'
                elif 'METEOR B30' in model:
                    return 'images/meteor-b30.jpg'
                elif 'METEOR B20' in model:
                    return 'images/meteor-b20.jpg'
                elif 'METEOR C11' in model:
                    return 'images/meteor-c11.jpg'
                elif 'METEOR Q3' in model:
                    return 'images/meteor-q3.jpg'
                elif 'METEOR M30' in model:
                    return 'images/meteor-m30.jpg'
                elif 'METEOR M6' in model:
                    return 'images/meteor-m6.jpg'
                elif 'LAGGARTT' in model or 'ГАЗ 6000' in model:
                    return 'images/laggartt.jpg'
                elif 'DEVOTION' in model:
                    return 'images/devotion.jpg'
                elif 'MK' in model:
                    return 'images/mk.jpg'
                else:
                    return 'images/default.jpg'
            
            # Правила для бойлеров
            elif product_type == "water_heater":
                return 'images/water_heater.jpg'
            
            # Правила для дымоходов
            elif product_type == "chimney":
                if "АДАПТЕР" in model:
                    return 'images/adapter.jpg'
                elif "PP" in model:
                    return 'images/chimney-pp.jpg'
                else:
                    return 'images/chimney-normal.jpg'
            
            # Правила для комплектующих
            elif product_type == "accessory":
                article = str(row['Артикул']).upper()
                if article == "BB99000142":
                    return 'images/sensor.jpg'
                elif article in ["30100000001", "30100000002"]:
                    return 'images/gas-kit.jpg'
                else:
                    return 'images/accessory.jpg'
            
            else:
                return 'images/default.jpg'
        
        # Добавляем фото
        merged_df['Фото'] = merged_df.apply(get_image_for_model, axis=1)
        
        # Добавляем статус
        merged_df['Статус'] = merged_df['В_наличии'].apply(lambda x: 'В наличии' if x > 0 else 'Нет в наличии')
        
        # Подготавливаем итоговый список для JSON
        result = []
        for _, row in merged_df.iterrows():
            item = {
                "Артикул": row["Артикул"],
                "Модель": row["Модель"],
                "Цена": row["Цена"],
                "В_наличии": row["В_наличии"],
                "Статус": row["Статус"],
                "Фото": row["Фото"],
                "type": row["type"]
            }
            
            # Добавляем характеристики в зависимости от типа
            if row["type"] == "boiler":
                item["Мощность"] = row["Мощность"]
                item["Контуры"] = row["Контуры"]
                item["WiFi"] = row["WiFi"]
            elif row["type"] == "water_heater":
                item["Объем"] = row["Объем"]
            elif row["type"] == "chimney":
                item["Диаметр"] = row["Диаметр"]
                item["Тип_дымохода"] = row["Тип_дымохода"]
            # Для accessory не добавляем специфических характеристик
            
            result.append(item)
        
        # Сохраняем
        with open('data.json', 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Готово! Обработано {len(result)} товаров")
        print(f"📊 В наличии: {sum(1 for x in result if x['В_наличии'] > 0)} товаров")
        
        # Покажем пример данных
        print("\nПример данных (первые 3 товара):")
        for i, item in enumerate(result[:3]):
            print(f"{i+1}. {item['Артикул']} - {item['Модель'][:50]}...")
            print(f"   Тип: {item['type']}, Цена: {item['Цена']} руб., Наличие: {item['В_наличии']} шт.")
            if item['type'] == 'boiler':
                print(f"   Контуры: {item.get('Контуры', '')}, Мощность: {item.get('Мощность', '')} кВт, Wi-Fi: {item.get('WiFi', '')}")
            elif item['type'] == 'water_heater':
                print(f"   Объем: {item.get('Объем', '')} л")
            elif item['type'] == 'chimney':
                print(f"   Диаметр: {item.get('Диаметр', '')}, Тип: {item.get('Тип_дымохода', '')}")
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