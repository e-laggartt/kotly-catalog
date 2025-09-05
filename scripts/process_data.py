import pandas as pd
import json
import re

def main():
    try:
        print("Загрузка данных из Google Sheets...")
        
        # Прямые ссылки на CSV экспорт
        price_url = "https://docs.google.com/spreadsheets/d/19PRNpA6F_HMI6iHSCg2iJF52PnN203ckY1WnqY_t5fc/export?format=csv"
        stock_url = "https://docs.google.com/spreadsheets/d/1o0e3-E20mQsWToYVQpCHZgLcbizCafLRpoPdxr8Rqfw/export?format=csv"
        
        # Загружаем данные
        print("Загружаем прайс...")
        price_df = pd.read_csv(price_url)
        
        print("Загружаем остатки...")
        stock_df = pd.read_csv(stock_url)
        
        print("Обработка данных...")
        
        # Переименовываем колонки для единообразия
        price_df = price_df.rename(columns={
            'Артикул': 'Артикул',
            'Модель': 'Модель', 
            'Цена, руб': 'Цена'
        })
        
        stock_df = stock_df.rename(columns={
            'Артикул': 'Артикул',
            'В наличии': 'В_наличии'
        })
        
        # Очистка данных
        price_df = price_df.dropna(subset=['Артикул'])
        price_df['Артикул'] = price_df['Артикул'].astype(str).str.strip()
        
        stock_df = stock_df.dropna(subset=['Артикул'])
        stock_df['Артикул'] = stock_df['Артикул'].astype(str).str.strip()
        
        # Обработка цены (убираем пробелы и запятые)
        def parse_price(price):
            try:
                if pd.isna(price):
                    return 0.0
                price_str = str(price).replace(' ', '').replace(',', '.')
                # Убираем все нечисловые символы кроме точки
                price_str = re.sub(r'[^\d\.]', '', price_str)
                return float(price_str)
            except:
                return 0.0
        
        price_df['Цена'] = price_df['Цена'].apply(parse_price)
        
        # Обработка количества
        def parse_quantity(qty):
            try:
                if pd.isna(qty):
                    return 0
                qty_str = str(qty).replace(' ', '').replace(',', '.')
                qty_val = float(qty_str)
                return max(0, int(qty_val))  # Отрицательные -> 0
            except:
                return 0
        
        stock_df['В_наличии'] = stock_df['В_наличии'].apply(parse_quantity)
        
        # Объединяем данные
        merged_df = pd.merge(price_df, stock_df, on='Артикул', how='left')
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
        
        # Применяем функцию к каждой модели
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
        
        # Покажем пример данных
        print("\nПример данных (первые 3 товара):")
        for i, item in enumerate(result[:3]):
            print(f"{i+1}. {item['Артикул']} - {item['Модель'][:30]}... - {item['Цена']} руб. - {item['В_наличии']} шт.")
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        
        # Создаем пустой файл чтобы сайт не сломался
        with open('data.json', 'w', encoding='utf-8') as f:
            json.dump([], f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
