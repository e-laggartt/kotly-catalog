import pandas as pd
import json

def main():
    try:
        print("Загрузка данных из Google Sheets...")
        
        # Прямые ссылки на CSV экспорт
        price_url = "https://docs.google.com/spreadsheets/d/19PRNpA6F_HMI6iHSCg2iJF52PnN203ckY1WnqY_t5fc/export?format=csv"
        stock_url = "https://docs.google.com/spreadsheets/d/1o0e3-E20mQsWToYVQpCHZgLcbizCafLRpoPdxr8Rqfw/export?format=csv"
        
        # Загружаем данные
        price_df = pd.read_csv(price_url)
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
        stock_df['В_наличии'] = stock_df['В_наличии'].fillna(0).astype(int)
        
        # Объединяем данные
        merged_df = pd.merge(price_df, stock_df, on='Артикул', how='left')
        merged_df['В_наличии'] = merged_df['В_наличии'].fillna(0).astype(int)
        
        # Добавляем дополнительные поля
        def extract_info(model):
            model_str = str(model).upper()
            
            # Мощность (ищем числа перед C/H)
            power_match = None
            if 'C' in model_str:
                power_match = model_str.split('C')[0].strip().split()[-1]
            elif 'H' in model_str:
                power_match = model_str.split('H')[0].strip().split()[-1]
            
            # Контуры
            contours = "Двухконтурный" if ' C' in model_str else "Одноконтурный"
            
            # Wi-Fi
            wifi = "Да" if any(x in model_str for x in ['WI-FI', 'WIFI', 'ВАЙ-ФАЙ']) else "Нет"
            
            return {
                'Мощность': power_match if power_match and power_match.isdigit() else "Не указана",
                'Контуры': contours,
                'WiFi': wifi
            }
        
        # Применяем функцию к каждой модели
        info_df = merged_df['Модель'].apply(lambda x: pd.Series(extract_info(x)))
        merged_df = pd.concat([merged_df, info_df], axis=1)
        
        # Добавляем статус
        merged_df['Статус'] = merged_df['В_наличии'].apply(lambda x: 'В наличии' if x > 0 else 'Нет в наличии')
        
        # Конвертируем в JSON
        result = merged_df.to_dict('records')
        
        # Сохраняем
        with open('data.json', 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Готово! Обработано {len(result)} товаров")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        # Создаем пустой файл чтобы сайт не сломался
        with open('data.json', 'w', encoding='utf-8') as f:
            json.dump([], f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
