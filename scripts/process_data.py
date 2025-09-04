import pandas as pd
import json
from datetime import datetime

def main():
    try:
        print("Начинаем обработку данных...")

        # === Загрузка данных ===
        price_df = pd.read_excel('Прайс.xlsx', sheet_name='Прайс-лист')
        stock_df = pd.read_excel('наличие котлы.xlsx', sheet_name='Лист_1')

        # === Обработка прайс-листа ===
        print("Обрабатываем прайс-лист...")
        price_df = price_df[['Артикул', 'Товар', 'Розничная']].copy()
        price_df.columns = ['Артикул', 'Модель', 'Цена']

        # Пропускаем первую строку (если нужно)
        if price_df.iloc[0].isnull().sum() < 2:
            price_df = price_df.iloc[1:].reset_index(drop=True)

        price_df = price_df.dropna(subset=['Артикул'])
        price_df['Артикул'] = (price_df['Артикул']
                               .astype(str)
                               .str.replace(r'\.0$', '', regex=True)
                               .str.strip())
        price_df['Цена'] = pd.to_numeric(price_df['Цена'], errors='coerce').fillna(0)

        price_df = price_df.drop_duplicates('Артикул')

        # === Обработка остатков ===
        print("Обрабатываем остатки...")
        # Убираем строки без артикула
        stock_df = stock_df.dropna(subset=[stock_df.columns[0]]).copy()

        # Фильтруем только строки с числовым артикулом (6+ цифр)
        mask = stock_df.iloc[:, 0].astype(str).str.match(r'^\d{6,}$', na=False)
        stock_df = stock_df[mask].copy()

        stock_df['Артикул'] = (stock_df.iloc[:, 0]
                               .astype(str)
                               .str.replace(r'\.0$', '', regex=True)
                               .str.strip())
        stock_df['В_наличии'] = pd.to_numeric(stock_df.iloc[:, 7], errors='coerce')  # Колонка H
        stock_df['В_наличии'] = stock_df['В_наличии'].fillna(0).apply(lambda x: max(0, x)).astype(int)

        stock_df = stock_df[['Артикул', 'В_наличии']]

        # === Объединение ===
        print("Объединяем данные...")
        merged_df = pd.merge(price_df, stock_df, on='Артикул', how='left')
        merged_df['В_наличии'] = merged_df['В_наличии'].fillna(0).astype(int)

        merged_df['Статус'] = merged_df['В_наличии'].apply(
            lambda x: 'В наличии' if x > 0 else 'Нет в наличии'
        )

        # === Сохранение JSON ===
        print("Сохраняем в JSON...")
        data_for_json = []
        for _, row in merged_df.iterrows():
            data_for_json.append({
                'Артикул': row['Артикул'],
                'Модель': row['Модель'],
                'Цена': float(row['Цена']),
                'В_наличии': int(row['В_наличии']),
                'Статус': row['Статус']
            })

        with open('data.json', 'w', encoding='utf-8') as f:
            json.dump(data_for_json, f, ensure_ascii=False, indent=2)

        print("✅ data.json создан")

        # === Сохранение Excel ===
        current_date = datetime.now().strftime('%Y-%m-%d')
        output_filename = f'Сводная_таблица_котлы_{current_date}.xlsx'

        with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
            merged_df.to_excel(writer, sheet_name='Сводная таблица', index=False)

            summary_df = pd.DataFrame({
                'Показатель': ['Всего', 'В наличии', 'Нет в наличии'],
                'Количество': [
                    len(merged_df),
                    len(merged_df[merged_df['В_наличии'] > 0]),
                    len(merged_df[merged_df['В_наличии'] == 0])
                ]
            })
            summary_df.to_excel(writer, sheet_name='Итоги', index=False)

        print(f"✅ {output_filename} создан")
        print(f"📊 В наличии: {len(merged_df[merged_df['В_наличии'] > 0])} позиций")

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        raise

if __name__ == "__main__":
    main()
