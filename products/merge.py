import pandas as pd

def merge_market_data():
    try:
        # Market verilerini Excel dosyalarından okuma
        a101_df = pd.read_excel('a101_products.xlsx')
        sok_df = pd.read_excel('sok_products.xlsx')
        migros_df = pd.read_excel('migros_products.xlsx')

        # Her market için kaynak bilgisi ekleme
        a101_df['market'] = 'A101'
        sok_df['market'] = 'ŞOK'
        migros_df['market'] = 'Migros'

        # Tüm dataframe'leri birleştirme
        merged_df = pd.concat([a101_df, sok_df, migros_df], 
                            ignore_index=True, 
                            sort=False)
        
        # NaN değerleri koruma
        merged_df = merged_df.fillna('')
        
        # Birleştirilmiş veriyi Excel olarak kaydetme
        merged_df.to_excel('merged_products.xlsx', index=False)
        print("Veriler başarıyla birleştirildi!")
        
        # Opsiyonel: Her marketten kaç ürün eklendiğini gösterme
        print("\nMağaza başına ürün sayıları:")
        print(merged_df['market'].value_counts())
        
        return merged_df

    except FileNotFoundError as e:
        print(f"Hata: Excel dosyası bulunamadı - {str(e)}")
    except Exception as e:
        print(f"Beklenmeyen bir hata oluştu: {str(e)}")

# Fonksiyonu çağırma
if __name__ == "__main__":
    merged_data = merge_market_data()