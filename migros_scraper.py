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

    def scrape_product_page(self, url, category, max_page):
        driver = self.get_driver()
        all_products = []
        page_number = 1
        processed_pages = set()
        max_retries = 10
        retry_delay = 5
        
        def try_load_page(url, attempt=1):
            try:
                driver.get(url)
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
                    # print("! Cookie popup'ı bulunamadı veya zaten kabul edilmiş")
                    pass
                
                # Sayfanın yüklendiğini kontrol et
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "mdc-card"))
                )
                return True
                
            except Exception as e:
                if attempt < max_retries:
                    print(f"\n! Sayfa yüklenemedi (Deneme {attempt}/{max_retries})")
                    print(f"Hata: {str(e)}")
                    print(f"{retry_delay} saniye sonra tekrar denenecek...")
                    time.sleep(retry_delay)
                    return try_load_page(url, attempt + 1)
                else:
                    print(f"\n❌ Sayfa {max_retries} denemede de yüklenemedi: {url}")
                    print(f"Son hata: {str(e)}")
                    return False
        
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
            print(f"🔍 Aranan URL: {url}")
            print(f"📂 Kategori: {category}")
            print(f"📑 Toplam Sayfa: {max_page}")
            print(f"{'='*50}\n")
            
            while page_number <= max_page:
                current_url = url if page_number == 1 else f"{url}?sayfa={page_number}&sirala=onerilenler"
                
                print(f"\n📄 Sayfa {page_number}/{max_page} işleniyor...")
                print(f"🔗 URL: {current_url}")
                print(f"⏳ Kalan Sayfa: {max_page - page_number + 1}")
                
                if page_number > max_page:
                    print("\n✅ Maksimum sayfa sayısına ulaşıldı!")
                    break
                
                try:
                    if not try_load_page(current_url):
                        print(f"! Sayfa {page_number} yüklenemedi")
                        if page_number >= max_page:
                            break
                        page_number += 1
                        continue
                    
                    scroll_to_load_images()
                    
                    # Ürünleri topla
                    product_cards = driver.find_elements(By.CLASS_NAME, "mdc-card")
                    page_products = []
                    
                    for card in product_cards:
                        try:
                            product_data = self.process_product_card(card, category)
                            if product_data:
                                page_products.append(product_data)
                        except Exception as e:
                            print(f"! Ürün işlenirken hata: {str(e)}")
                            continue
                    
                    # Sayfadaki ürünleri ekle
                    all_products.extend(page_products)
                    print(f"✓ Sayfa {page_number}: {len(page_products)} ürün alındı")
                    print(f"✓ Toplam: {len(all_products)} ürün")
                    
                    processed_pages.add(page_number)
                    page_number += 1
                    
                    # İlerleme çubuğu
                    progress = "=" * (20 * len(processed_pages) // max_page)
                    remaining = " " * (20 - len(progress))
                    print(f"\nİlerleme: [{progress}{remaining}] {len(processed_pages)}/{max_page}")
                    
                    if page_number > max_page:
                        print("\n✅ Tüm sayfalar tarandı!")
                        break
                    
                except Exception as e:
                    print(f"! Sayfa işlenirken hata: {str(e)}")
                    if page_number >= max_page:
                        break
                    page_number += 1
                    continue
            
            # Sonuçları queue'ya ekle
            if all_products:
                self.results_queue.put(all_products)
                print(f"\n{'='*50}")
                print(f"✅ URL tarama tamamlandı: {url}")
                print(f"📊 Toplanan ürün sayısı: {len(all_products)}")
                print(f"📑 Taranan sayfa sayısı: {len(processed_pages)}")
                print(f"{'='*50}\n")
                return True
                
        except Exception as e:
            print(f"Genel hata: {str(e)}")
            return False
        
        finally:
            try:
                driver.quit()
            except:
                pass

    def save_results(self, total_urls):
        """Sonuçları toplama ve kaydetme işlemi"""
        all_products = []
        
        # Queue'dan tüm ürünleri al
        while not self.results_queue.empty():
            products = self.results_queue.get()
            if isinstance(products, list):  # Liste kontrolü ekle
                all_products.extend(products)
        
        if all_products:
            try:
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
                
                # Dosya adını oluştur
                output_file = f'migros_products_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx'
                
                # Excel'e kaydet
                output_df.to_excel(output_file, index=False)
                
                print(f"\n{'='*50}")
                print(f"✓ Sonuçlar {output_file} dosyasına kaydedildi")
                print(f"✓ Toplam {len(all_products)} ürün işlendi")
                print(f"✓ Toplam {total_urls} URL tarandı")
                print(f"✓ Toplam {len(self.processed_urls)} benzersiz URL işlendi")
                print(f"{'='*50}")
                
            except Exception as e:
                print(f"\n❌ Sonuçlar kaydedilirken hata oluştu: {str(e)}")
        else:
            print("\n❌ İşlenecek ürün bulunamadı!")

    def test_single_url(self, test_url, test_category, test_max_page=3):
        """Test amaçlı tek URL'yi işlemek için fonksiyon"""
        try:
            print("\n=== TEST MODU ===")
            print(f"Test URL: {test_url}")
            print(f"Test Kategori: {test_category}")
            print(f"Test Sayfa Sayısı: {test_max_page}")
            print("================\n")
            
            # Tüm sayfaları tek seferde işle
            if not self.scrape_product_page(test_url, test_category, test_max_page):
                print("! URL işlenemedi")
                return
            
            # Sonuçları kaydet
            self.save_results(1)  # 1 URL işlendiği için
            
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
        try:
            df = pd.read_excel(self.input_file)
            df = df[df['market'].str.lower() == 'migros'].copy().reset_index(drop=True)
            total_urls = len(df)
            total_products = 0
            
            print(f"\n🎯 Toplam {total_urls} Migros URL'si işlenecek")
            
            with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
                futures = []
                
                for index, row in df.iterrows():
                    base_url = row['url']
                    category = row['category']
                    max_page = int(row['max_page'])
                    
                    future = executor.submit(self.scrape_product_page, base_url, category, max_page)
                    futures.append(future)
                
                # Thread'lerin tamamlanmasını bekle
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        print(f"Thread işlemi sırasında hata: {str(e)}")
            
            # Tüm işlemler bittikten sonra sonuçları kaydet
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

    def process_product_card(self, card, category):
        """Ürün kartından veri çeken fonksiyon"""
        try:
            # JavaScript ile ürün verilerini çek
            product_data = self.thread_local.driver.execute_script("""
                var card = arguments[0];
                
                // Resim URL'sini kontrol et ve sadece tam yüklenmiş resimleri al
                var imageUrl = '';
                var imgElement = card.querySelector('#product-image-link > img');
                if (imgElement && imgElement.complete && 
                    imgElement.src && 
                    !imgElement.src.startsWith('data:image')) {
                    imageUrl = imgElement.src;
                }
                
                // Para birimi ve fiyat ayırma fonksiyonu
                function extractPriceAndCurrency(priceText) {
                    if (!priceText) return { price: null, currency: null };
                    const match = priceText.match(/([\d,.]+)\s*([A-Za-z₺]+)/);
                    if (match) {
                        return {
                            price: match[1].replace(',', '.'),
                            currency: match[2]
                        };
                    }
                    return { price: null, currency: null };
                }
                
                // Fiyat verilerini al
                var mainPrice = card.querySelector('.price span')?.textContent?.trim() || '';
                var moneyPrice = card.querySelector('.money-discount #sale-price')?.textContent?.trim() || '';
                var basketDiscount = card.querySelector('.crm-badge span')?.textContent?.trim() || '';
                
                // Fiyat ve para birimlerini ayır
                var mainPriceData = extractPriceAndCurrency(mainPrice);
                var moneyPriceData = extractPriceAndCurrency(moneyPrice);
                
                return {
                    name: card.querySelector('#product-name')?.textContent?.trim() || '',
                    url: card.querySelector('#product-image-link')?.href || '',
                    main_price: mainPriceData.price,
                    currency: mainPriceData.currency || 'TL',
                    money_price: moneyPriceData.price,
                    basket_discount: basketDiscount,
                    image: imageUrl
                }
            """, card)
            
            # Sepet indirim yüzdesini ayıkla
            basket_discount_percent = None
            if product_data['basket_discount']:
                basket_discount_percent = ''.join(filter(str.isdigit, product_data['basket_discount']))
            
            # Eğer resim base64 ise veya boşsa, tekrar deneme yap
            if not product_data['image'] or product_data['image'].startswith('data:image'):
                # Karta scroll yap ve bekle
                self.thread_local.driver.execute_script("arguments[0].scrollIntoView(true);", card)
                time.sleep(1)
                
                # Resmi tekrar kontrol et
                img = card.find_element(By.CSS_SELECTOR, "#product-image-link > img")
                if img.is_displayed() and img.get_attribute("src") and not img.get_attribute("src").startswith('data:image'):
                    product_data['image'] = img.get_attribute("src")
            
            return {
                'name': product_data['name'],
                'main_price': product_data['main_price'],
                'money_price': product_data['money_price'] or None,
                'basket_discount_percent': basket_discount_percent,
                'basket_price': None,  # Sepet fiyatını hesaplamak gerekirse buraya eklenebilir
                'image_url': product_data['image'],
                'url': product_data['url'],
                'category': category
            }
            
        except Exception as e:
            print(f"Ürün işlenirken hata: {str(e)}")
            return None

def main():
    try:
        # Test modu
        test_mode = False  # Test modunu açıp kapatmak için bu değişkeni kullanın
        
        if test_mode:
            # Test için örnek URL ve kategori
            test_url = "https://www.migros.com.tr/meyve-sebze-c-2"
            test_category = "Meyve, Sebze"
            test_max_page = 6  # Test için sayfa sayısı
            
            # Scraper'ı başlat ve test fonksiyonunu çağır
            scraper = MigrosScraper("test")
            scraper.test_single_url(test_url, test_category, test_max_page)
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