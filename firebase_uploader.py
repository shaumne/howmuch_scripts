import os
import pandas as pd
from firebase_admin import credentials, initialize_app, firestore
import json
from datetime import datetime
import numpy as np

class FirebaseUploader:
    def __init__(self, excel_path, credentials_path):
        """
        Firebase yükleyiciyi başlat
        """
        try:
            # Önceki Firebase uygulamasını temizle
            try:
                from firebase_admin import delete_app, get_app
                delete_app(get_app())
            except:
                pass
            
            # Firebase'i başlat
            print("Firebase başlatılıyor...")
            cred = credentials.Certificate(credentials_path)
            initialize_app(cred)
            self.db = firestore.client()
            print("Firebase bağlantısı başarılı!")
            
            # Excel dosyasını oku
            print("Excel dosyası okunuyor...")
            self.df = pd.read_excel(excel_path)
            print(f"Excel dosyası okundu: {len(self.df)} satır")
            
        except Exception as e:
            print(f"Başlatma hatası: {str(e)}")
            raise
        
    def upload_data(self):
        """Verileri Firebase'e yükle"""
        try:
            # Koleksiyon referansları
            products_ref = self.db.collection('products')
            markets_ref = self.db.collection('markets')
            categories_ref = self.db.collection('categories')
            
            # Benzersiz market ve kategorileri topla
            unique_markets = self.df['market'].unique()
            unique_categories = self.df['category'].unique()
            
            print("Marketler yükleniyor...")
            # Marketleri kaydet
            for market in unique_markets:
                markets_ref.document(market).set({
                    'name': market,
                    'last_updated': firestore.SERVER_TIMESTAMP
                }, merge=True)
            
            print("Kategoriler yükleniyor...")
            # Kategorileri kaydet
            for category in unique_categories:
                categories_ref.document(category).set({
                    'name': category,
                    'last_updated': firestore.SERVER_TIMESTAMP
                }, merge=True)
            
            print("Ürünler yükleniyor...")
            # Her ürün için
            for idx, row in self.df.iterrows():
                # Benzersiz ürün ID oluştur
                product_id = f"{row['market']}_{row['normalized_name']}"
                
                # String'e çevrilmesi gereken array'leri düzelt
                volume = eval(row['volume']) if isinstance(row['volume'], str) else row['volume']
                units = eval(row['units']) if isinstance(row['units'], str) else row['units']
                search_tags = eval(row['search_tags']) if isinstance(row['search_tags'], str) else row['search_tags']
                
                # Fiyat geçmişi için alt koleksiyon verisi
                price_history = {
                    'date': firestore.SERVER_TIMESTAMP,
                    'current_price': row['current_price'],
                    'original_price': row['original_price'],
                    'discount_percentage': row['discount_percentage'],
                    'money_price': row['money_price'],
                    'basket_price': row['basket_price'],
                    'money_discount_percentage': row['money_discount_percentage'],
                    'basket_discount_percentage': row['basket_discount_percentage']
                }
                
                # Ana ürün verisi
                product_data = {
                    'normalized_name': row['normalized_name'],
                    'original_name': row['original_name'],
                    'market': row['market'],
                    'category': row['category'],
                    'volume': volume,
                    'units': units,
                    'current_price': row['current_price'],
                    'original_price': row['original_price'],
                    'discount_percentage': row['discount_percentage'],
                    'money_price': row['money_price'],
                    'basket_price': row['basket_price'],
                    'money_discount_percentage': row['money_discount_percentage'],
                    'basket_discount_percentage': row['basket_discount_percentage'],
                    'image_url': row['image_url'],
                    'url': row['url'],
                    'search_tags': search_tags,
                    'last_updated': firestore.SERVER_TIMESTAMP
                }
                
                # None değerleri temizle (array'leri kontrol ederek)
                def is_valid_value(v):
                    if isinstance(v, (list, np.ndarray)):
                        return len(v) > 0
                    return pd.notna(v)
                
                product_data = {k: v for k, v in product_data.items() if is_valid_value(v)}
                price_history = {k: v for k, v in price_history.items() if is_valid_value(v)}
                
                # Ürünü kaydet
                product_ref = products_ref.document(product_id)
                product_ref.set(product_data, merge=True)
                
                # Fiyat geçmişini kaydet
                product_ref.collection('price_history').add(price_history)
                
                if idx % 100 == 0:
                    print(f"{idx} ürün yüklendi...")
            
            print("\nTüm veriler başarıyla Firebase'e yüklendi!")
            
        except Exception as e:
            print(f"Hata oluştu: {str(e)}")
            raise

def main():
    # Dosya yollarını belirt
    excel_path = 'normalized_products.xlsx'
    credentials_path = 'C:/Users/Shaumne/Desktop/script/serviceAccoutKey.json'
    
    try:
        print("Firebase yükleme işlemi başlıyor...")
        uploader = FirebaseUploader(excel_path, credentials_path)
        uploader.upload_data()
        
    except Exception as e:
        print(f"\nHata oluştu: {str(e)}")
        raise

if __name__ == "__main__":
    main() 