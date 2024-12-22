import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from queue import Queue
import time
from datetime import datetime
import psutil
import os
import random

class MigrosScraper:
    def __init__(self, input_file):
        self.input_file = input_file
        self.max_threads = 20
        self.results_queue = Queue()
        self.thread_local = threading.local()
        self.drivers = []
        self.processed_urls = set()
        self.url_lock = threading.Lock()
        
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]

    def scrape_product_page(self, url, category):
        driver = self.get_driver()
        all_products = []
        page_number = 1
        processed_pages = set()
        
        # Sayfa yüklendikten sonra tüm ürünleri görmek için aşamalı scroll
        def scroll_to_load_images():
            try:
                # Sayfanın toplam yüksekliğini al
                total_height = driver.execute_script("return document.body.scrollHeight")
                
                # Sayfayı yukarıdan aşağıya doğru kademeli olarak scroll et
                scroll_step = 800  # Her adımda 800px scroll
                current_position = 0
                
                while current_position < total_height:
                    current_position += scroll_step
                    driver.execute_script(f"window.scrollTo(0, {current_position});")
                    time.sleep(0.5)  # Her scroll sonrası yarım saniye bekle
                
                # Son olarak sayfanın en altına git
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)  # Son scroll sonrası 2 saniye bekle
                
            except Exception as e:
                print(f"Scroll işlemi sırasında hata: {str(e)}")
        
        try:
            print(f"\n{'='*50}")
            print(f"URL: {url}")
            print(f"Kategori: {category}")
            
            # İlk sayfa yüklemesi ve cookie kontrolü
            base_url = url.split('?')[0]
            driver.get(base_url)
            time.sleep(2)
            
            # Cookie popup'ını kabul et
            try:
                cookie_button = WebDriverWait(driver, 3).until(
                    EC.element_to_be_clickable((By.XPATH, 
                        "/html/body/sm-root/div/fe-product-cookie-indicator/div/div/button[2]"))
                )
                cookie_button.click()
                time.sleep(1)
                print("✓ Cookie popup'ı kabul edildi")
            except:
                print("! Cookie popup'ı bulunamadı veya zaten kabul edilmiş")
                pass
            
            # Tüm resimleri yüklemek için scroll işlemi
            scroll_to_load_images()
            
            # Resimlerin yüklenmesini bekle
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "img.product-image.loaded"))
            )
            
            while True:
                current_url = f"{base_url}?sayfa={page_number}&sirala=onerilenler"
                
                if page_number in processed_pages:
                    print("\nTüm sayfalar tarandı!")
                    break
                    
                driver.get(current_url)
                time.sleep(2)
                processed_pages.add(page_number)
                
                # Her yeni sayfada scroll işlemini tekrarla
                scroll_to_load_images()
                
                # Ürünleri topla
                product_cards = driver.find_elements(By.CLASS_NAME, "mdc-card")
                
                for card in product_cards:
                    try:
                        # JavaScript ile resim kontrolü ekle
                        product_data = driver.execute_script("""
                            var card = arguments[0];
                            
                            // Resim URL'sini kontrol et ve sadece tam yüklenmiş resimleri al
                            var imageUrl = '';
                            var imgElement = card.querySelector('#product-image-link > img');
                            if (imgElement && imgElement.complete && 
                                imgElement.src && 
                                !imgElement.src.startsWith('data:image')) {
                                imageUrl = imgElement.src;
                            }
                            
                            return {
                                name: card.querySelector('#product-name')?.textContent?.trim() || '',
                                url: card.querySelector('#product-image-link')?.href || '',
                                main_price: card.querySelector('.price span')?.textContent?.trim() || '',
                                money_price: card.querySelector('.money-discount #sale-price')?.textContent?.trim() || '',
                                basket_discount: card.querySelector('.crm-badge span')?.textContent?.trim() || '',
                                image: imageUrl
                            }
                        """, card)
                        
                        # Eğer resim base64 ise veya boşsa, tekrar deneme yap
                        if not product_data['image'] or product_data['image'].startswith('data:image'):
                            # Karta scroll yap ve bekle
                            driver.execute_script("arguments[0].scrollIntoView(true);", card)
                            time.sleep(1)
                            
                            # Resmi tekrar kontrol et
                            img = card.find_element(By.CSS_SELECTOR, "#product-image-link > img")
                            if img.is_displayed() and img.get_attribute("src") and not img.get_attribute("src").startswith('data:image'):
                                product_data['image'] = img.get_attribute("src")
                        
                        # Debug bilgisi
                        print(f"\nÜrün: {product_data['name']}")
                        print(f"Resim URL: {product_data['image']}")
                        
                        # Ürün ID'sini URL'den al ve formatla
                        product_id = ''
                        if product_data['url']:
                            try:
                                # URL'den ID'yi al
                                raw_id = product_data['url'].split('-p-')[1].replace('p-', '')
                                
                                # Eğer ID hex formatındaysa (1ac7780 gibi), decimal'e çevir
                                if any(c.isalpha() for c in raw_id):
                                    decimal_id = str(int(raw_id, 16))
                                    # 8 haneye tamamla
                                    product_id = decimal_id.zfill(8)
                                else:
                                    # Direkt olarak 8 haneye tamamla
                                    product_id = raw_id.zfill(8)
                                    
                            except:
                                print(f"! ID çıkarılamadı: {product_data['url']}")
                                pass
                        
                        # Resim URL'si seçme stratejisi
                        if product_data['image'] and not product_data['image'].startswith('data:image'):
                            image_url = product_data['image']
                        else:
                            image_url = ''
                        # Ürün verisini güncelle
                        product_data['image'] = image_url
                        
                        name = product_data['name']
                        if name in [p['name'] for p in all_products]:  # Mükerrer ürünleri atla
                            continue
                            
                        product_url = product_data['url']
                        
                        # Fiyatları temizle ve düzelt
                        def clean_price(price):
                            if not price:
                                return ''
                            # Eğer birden fazla fiyat varsa son fiyatı al
                            prices = price.split('TL')
                            last_price = prices[-2] if len(prices) > 1 else prices[0]
                            return last_price.replace('TL','').replace('₺','').strip()
                        
                        main_price = clean_price(product_data['main_price'])
                        money_price = clean_price(product_data['money_price'])
                        
                        # Sepette indirimi temizle
                        basket_discount = product_data['basket_discount']
                        if basket_discount:
                            # Sadece sayıyı al
                            import re
                            numbers = re.findall(r'\d+', basket_discount)
                            basket_discount = numbers[0] if numbers else ''
                        
                        # Sepette indirim hesapla
                        try:
                            if basket_discount and main_price:
                                discount_percent = float(basket_discount)
                                main_price_float = float(main_price.replace(',','.'))
                                basket_price = main_price_float * (1 - discount_percent/100)
                                basket_price = round(basket_price, 2)
                            else:
                                basket_price = ''
                        except:
                            basket_price = ''
                            print(f"Fiyat hesaplama hatası: {main_price} - {basket_discount}")
                        
                        product = {
                            'name': name,
                            'image_url': image_url,
                            'main_price': main_price,
                            'money_price': money_price,
                            'basket_discount_percent': basket_discount,
                            'basket_price': str(basket_price) if basket_price else '',
                            'url': product_url,
                            'category': category
                        }
                        all_products.append(product)
                        
                    except Exception as e:
                        print(f"Ürün işlenirken hata: {str(e)}")
                        continue
                
                # Sonraki sayfa kontrolü
                try:
                    # Sonraki sayfa butonunu bul
                    next_button = WebDriverWait(driver, 3).until(
                        EC.presence_of_element_located((By.ID, "pagination-button-next"))
                    )
                    
                    # Butonun durumunu kontrol et
                    if "button--nonselected" in next_button.get_attribute("class") and not next_button.get_attribute("disabled"):
                        page_number += 1
                        print(f"Sayfa {page_number-1} tamamlandı, sonraki sayfaya geçiliyor...")
                        time.sleep(1)
                    else:
                        print("\nSon sayfaya ulaşıldı!")
                        break
                        
                except Exception as e:
                    print(f"\nSonraki sayfa bulunamadı veya son sayfaya ulaşıldı: {str(e)}")
                    break
            
            if all_products:
                self.results_queue.put(all_products)
                print(f"\n✓ Toplam {len(all_products)} ürün başarıyla işlendi")
                print(f"✓ Toplam {len(processed_pages)} sayfa tarandı")
            
        except Exception as e:
            print(f"HATA: {str(e)}")
        finally:
            print(f"{'='*50}\n")

    def save_results(self, total_urls):
        """Sonuçları toplama ve kaydetme işlemi"""
        all_products = []
        while not self.results_queue.empty():
            all_products.extend(self.results_queue.get())
        
        if all_products:
            output_df = pd.DataFrame(all_products)
            # Sütunları düzenle
            columns = [
                'name', 
                'main_price', 
                'money_price',
                'basket_discount_percent',
                'basket_price',
                'image_url', 
                'url', 
                'category'
            ]
            output_df = output_df[columns]
            
            output_file = f'migros_products_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx'
            output_df.to_excel(output_file, index=False)
            print(f"\n{'='*50}")
            print(f"✓ Sonuçlar {output_file} dosyasına kaydedildi")
            print(f"✓ Toplam {len(all_products)} ürün işlendi")
            print(f"✓ Toplam {total_urls} URL tarandı")
            print(f"✓ Toplam {len(self.processed_urls)} benzersiz URL işlendi")
            print(f"{'='*50}")
        else:
            print("\n❌ İşlenecek ürün bulunamadı!")

    def test_single_url(self, test_url, test_category):
        """Test amaçlı tek URL'yi işlemek için fonksiyon"""
        try:
            print("\n=== TEST MODU ===")
            print(f"Test URL: {test_url}")
            print(f"Test Kategori: {test_category}")
            print("================\n")
            
            # URL'yi işle
            self.scrape_product_page(test_url, test_category)
            
            # Sonuçları kaydet
            self.save_results(1)
            
        except Exception as e:
            print(f"Test sırasında hata: {str(e)}")
        finally:
            self.cleanup()

    def get_driver(self):
        """Her thread için ayrı bir driver oluştur"""
        if not hasattr(self.thread_local, "driver"):
            chrome_options = webdriver.ChromeOptions()
            
            # Temel ayarlar
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-logging')
            chrome_options.add_argument('--log-level=3')
            chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
            
            # Bildirimleri devre dışı bırak
            chrome_options.add_argument('--disable-notifications')
            
            # Bot tespitini engelleme
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # Random User-Agent seç
            user_agent = random.choice(self.user_agents)
            chrome_options.add_argument(f'user-agent={user_agent}')
            
            driver = webdriver.Chrome(options=chrome_options)
            driver.implicitly_wait(10)
            
            self.thread_local.driver = driver
            self.drivers.append(driver)
            
        return self.thread_local.driver

    def cleanup(self):
        """Tüm Chrome driver'ları temizle"""
        print("Chrome driver'lar temizleniyor...")
        for driver in self.drivers:
            try:
                driver.quit()
            except:
                pass
        
        # Kalan Chrome process'lerini kontrol et ve kapat
        for proc in psutil.process_iter(['pid', 'name']):
            try:
                if 'chromedriver' in proc.info['name'].lower():
                    os.kill(proc.info['pid'], 9)
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass

    def process_urls(self):
        """Excel'den URL'leri oku ve işle"""
        try:
            # Excel dosyasını oku
            df = pd.read_excel(self.input_file)
            
            # Gerekli sütunları kontrol et
            required_columns = ['url', 'category', 'market']
            if not all(col in df.columns for col in required_columns):
                raise Exception("Excel dosyasında 'url', 'category' ve 'market' sütunları bulunamadı!")
            
            # Sadece Migros market verilerini filtrele
            df = df[df['market'].str.lower() == 'migros'].copy()
            
            # Boş olmayan URL'leri al
            urls_and_categories = df[['url', 'category']].dropna().values.tolist()
            total_urls = len(urls_and_categories)
            
            if total_urls == 0:
                print("İşlenecek Migros URL'si bulunamadı!")
                return
            
            print(f"\nToplam {total_urls} Migros URL'si işlenecek")
            
            # Tek bir thread havuzu oluştur
            with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
                futures = []
                
                # Tüm URL'ler için future'ları oluştur
                for url, category in urls_and_categories:
                    if url not in self.processed_urls:
                        with self.url_lock:
                            self.processed_urls.add(url)
                        future = executor.submit(self.scrape_product_page, url, category)
                        futures.append(future)
                
                # Future'ları tamamlanma sırasına göre işle
                for i, future in enumerate(as_completed(futures), 1):
                    try:
                        future.result()
                        print(f"\nİşlenen URL: {i}/{len(futures)}")
                    except Exception as e:
                        print(f"URL işlenirken hata: {str(e)}")
            
            # Tüm sonuçları topla ve Excel'e kaydet
            self.save_results(total_urls)
            
        except Exception as e:
            print(f"URL'ler işlenirken hata: {str(e)}")
        
        finally:
            self.cleanup()

    def generate_search_tags(self, product_data):
        """Ürün için arama etiketleri oluşturur"""
        tags = set()  # Tekrar eden tag'leri önlemek için set kullanıyoruz
        
        def generate_turkish_variants(word):
            """Türkçe karakter varyasyonlarını oluşturur"""
            replacements = {
                'ı': 'i',
                'i': 'ı',
                'ğ': 'g',
                'ü': 'u',
                'ş': 's',
                'ö': 'o',
                'ç': 'c',
                'İ': 'I',
                'Ğ': 'G',
                'Ü': 'U',
                'Ş': 'S',
                'Ö': 'O',
                'Ç': 'C'
            }
            
            variants = {word.lower()}
            
            # Orijinal kelimeyi de ekle
            variants.add(word)
            
            # Her bir karakteri değiştirerek varyasyonlar oluştur
            for old, new in replacements.items():
                if old in word.lower():
                    variants.add(word.lower().replace(old, new))
            
            return variants

        # Normalized name'i kelimelere ayır
        name_words = product_data['normalized_name'].split()
        
        # Tek kelimelik tag'ler ve varyasyonları
        for word in name_words:
            if len(word) >= 2:  # 2 karakterden uzun kelimeleri al
                tags.update(generate_turkish_variants(word))
        
        # İki kelimelik kombinasyonlar ve varyasyonları
        for i in range(len(name_words) - 1):
            if len(name_words[i]) >= 2 and len(name_words[i+1]) >= 2:
                two_word = f"{name_words[i]} {name_words[i+1]}"
                tags.update(generate_turkish_variants(two_word))
        
        # Üç kelimelik kombinasyonlar ve varyasyonları
        for i in range(len(name_words) - 2):
            if len(name_words[i]) >= 2 and len(name_words[i+1]) >= 2 and len(name_words[i+2]) >= 2:
                three_word = f"{name_words[i]} {name_words[i+1]} {name_words[i+2]}"
                tags.update(generate_turkish_variants(three_word))
        
        # Kategori bazlı tag'ler ve varyasyonları
        category_words = product_data['category'].lower().split()
        for word in category_words:
            if len(word) >= 2:
                tags.update(generate_turkish_variants(word))
        
        # Özel durumlar için tag'ler
        if product_data.get('volume') and product_data.get('units'):
            for vol, unit in zip(product_data['volume'], product_data['units']):
                volume_tag = f"{vol} {unit}"
                tags.add(volume_tag)
                # Birim varyasyonları
                unit_variants = {
                    'litre': ['lt', 'l', 'liter'],
                    'gram': ['gr', 'g'],
                    'kilogram': ['kg', 'kilo'],
                    'mililitre': ['ml', 'mlt']
                }
                for main_unit, variants in unit_variants.items():
                    if unit.lower() == main_unit:
                        for variant in variants:
                            tags.add(f"{vol} {variant}")
        
        # İndirim durumu için tag'ler
        if product_data.get('discount_percentage'):
            tags.update(['indirim', 'indirimli', 'kampanya', 'firsat', 'fırsat'])
        
        if product_data.get('money_discount_percentage'):
            tags.update([
                'money', 'money kart', 'money indirim', 
                'money kart indirimi', 'money club', 
                'moneyclub', 'money club indirim'
            ])
        
        if product_data.get('basket_discount_percentage'):
            tags.update([
                'sepet', 'sepette', 'sepette indirim',
                'sepet indirimi', 'sepette indirimli'
            ])
        
        # Marka/ürün adı varyasyonları
        brand_patterns = [
            r'(\w+)\s+marka',
            r'(\w+)\s+market',
            r'(\w+)\s+ürünleri'
        ]
        
        for pattern in brand_patterns:
            import re
            matches = re.findall(pattern, product_data['normalized_name'].lower())
            for match in matches:
                if len(match) >= 2:
                    tags.update(generate_turkish_variants(match))
        
        # Özel karakterleri ve fazla boşlukları temizle
        cleaned_tags = {re.sub(r'[^\w\s]', '', tag).strip() for tag in tags}
        cleaned_tags = {tag for tag in cleaned_tags if len(tag) >= 2}  # En az 2 karakterli tag'leri al
        
        return list(cleaned_tags)

def main():
    try:
        # Test modu
        test_mode = False  # Test modunu açıp kapatmak için bu değişkeni kullanın
        
        if test_mode:
            # Test için örnek URL ve kategori
            test_url = "https://www.migros.com.tr/meyve-sebze-c-2"
            test_category = "Et, Tavuk, Balık"
            
            # Scraper'ı başlat ve test fonksiyonunu çağır
            scraper = MigrosScraper("test")
            scraper.test_single_url(test_url, test_category)
        else:
            # Normal mod - tüm URL'leri işle
            input_file = "C:/Users/Shaumne/Desktop/script/url/url_kategorileri.xlsx"
            scraper = MigrosScraper(input_file)
            scraper.process_urls()
        
    except Exception as e:
        print(f"Main process error: {e}")
    
    finally:
        # Programın sonunda tüm Chrome process'lerini kontrol et
        for proc in psutil.process_iter(['pid', 'name']):
            try:
                if 'chromedriver' in proc.info['name'].lower():
                    os.kill(proc.info['pid'], 9)
            except:
                pass

if __name__ == "__main__":
    main()