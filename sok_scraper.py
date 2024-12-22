import threading
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
from datetime import datetime
from queue import Queue
import psutil
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class SokScraper:
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
        max_retries = 3  # Maksimum deneme sayısı
        retry_delay = 5  # Denemeler arası bekleme süresi (saniye)

        def wait_for_page_load():
            """Sayfanın tamamen yüklenmesini bekle"""
            try:
                # DOM'un hazır olmasını bekle
                WebDriverWait(driver, 10).until(
                    lambda d: d.execute_script('return document.readyState') == 'complete'
                )
                # İlk ürün kartının görünür olmasını bekle
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 
                        "div.CProductCard-module_productCardWrapper__okAmT"))
                )
                return True
            except:
                return False
        
        def scroll_smoothly():
            """Sayfayı aşamalı olarak scroll et"""
            try:
                # Başlangıç yüksekliğini al
                last_height = driver.execute_script("return document.body.scrollHeight")
                # print(f"\nScroll başlıyor - Başlangıç yüksekliği: {last_height}px")
                
                while True:
                    # Sayfayı parça parça scroll et
                    current_height = 0
                    while current_height < last_height:
                        current_height += 800
                        # print(f"Scroll pozisyonu: {current_height}px")
                        
                        try:
                            # Önce smooth scroll dene
                            driver.execute_script(f"window.scrollTo({{top: {current_height}, behavior: 'smooth'}})")
                            
                            # Kısa bir bekleme
                            WebDriverWait(driver, 2).until(
                                lambda d: True
                            )
                            
                            # Scroll pozisyonunu kontrol et
                            actual_position = driver.execute_script("return window.pageYOffset")
                            if actual_position < current_height - 200:  # 200px tolerans
                                # print(f"Smooth scroll başarısız, direkt scroll deneniyor: {actual_position}px / {current_height}px")
                                # Direkt scroll dene
                                driver.execute_script(f"window.scrollTo(0, {current_height})")
                                
                                # Tekrar pozisyonu kontrol et
                                actual_position = driver.execute_script("return window.pageYOffset")
                                # print(f"Yeni scroll pozisyonu: {actual_position}px")
                            
                            # Yeni içeriğin yüklenmesini bekle
                            try:
                                WebDriverWait(driver, 5).until(
                                    lambda d: len(d.find_elements(By.CSS_SELECTOR, 
                                        "div.CProductCard-module_productCardWrapper__okAmT")) > 0
                                )
                            except Exception as e:
                                # print(f"Yeni içerik beklenirken hata: {str(e)}")
                                continue
                                
                        except Exception as e:
                            # print(f"Scroll hareketi sırasında hata: {str(e)}")
                            continue
                    
                    # Yeni yüksekliği kontrol et
                    new_height = driver.execute_script("return document.body.scrollHeight")
                    # print(f"Yeni sayfa yüksekliği: {new_height}px")
                    
                    # Eğer yükseklik artık artmıyorsa, sayfanın sonuna gelmişiz demektir
                    if new_height == last_height:
                        
                        # Son bir kez sayfanın en altına git
                        try:
                            driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
                            print("Son scroll tamamlandı")
                        except Exception as e:
                            # print(f"Son scroll sırasında hata: {str(e)}")
                            continue
                        
                        # Tüm resimlerin yüklenmesini bekle
                        try:
                            WebDriverWait(driver, 10).until(
                                lambda d: d.execute_script("""
                                    return Array.from(document.images).every(img => img.complete)
                                """)
                            )
                            print("Tüm resimler yüklendi")
                        except Exception as e:
                            # print(f"Resim yükleme hatası: {str(e)}")
                            continue
                        break
                    
                    last_height = new_height
                    # print(f"Scroll devam ediyor - Yeni hedef yükseklik: {last_height}px")
                
            except Exception as e:
                # print("\nScroll işlemi sırasında kritik hata:")
                # print(f"Hata türü: {type(e).__name__}")
                # print(f"Hata mesajı: {str(e)}")
                # print(f"Son scroll pozisyonu: {current_height if 'current_height' in locals() else 'Bilinmiyor'}")
                # print(f"Son sayfa yüksekliği: {last_height if 'last_height' in locals() else 'Bilinmiyor'}")
                return  # continue yerine return kullanıyoruz

        for attempt in range(max_retries):
            try:
                driver.get(url)
                
                if not wait_for_page_load():
                    print(f"Sayfa yüklenemedi. Deneme {attempt + 1}/{max_retries}")
                    continue
                
                # Cookie popup'ını kabul et
                try:
                    cookie_button = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, 
                            "/html/body/div[3]/div[2]/div/div/div[2]/div[2]/div/div[1]/button"))
                    )
                    cookie_button.click()
                except:
                    pass
                
                # Scroll işlemi
                scroll_smoothly()
                
                # Ürünleri topla
                product_cards = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, 
                        "div.CProductCard-module_productCardWrapper__okAmT"))
                )
                
                products = []
                for card in product_cards:
                    try:
                        product_data = driver.execute_script("""
                            var card = arguments[0];
                            
                            // İndirimli fiyat kontrolü
                            var discountContainer = card.querySelector('.CPriceBox-module_discountedPriceContainer__nsaTN');
                            var normalContainer = card.querySelector('.CPriceBox-module_priceContainer__ZROpc');
                            
                            // URL'yi al
                            var link = card.closest('a');
                            var productUrl = link ? link.href : '';
                            
                            var currentPrice = '';
                            var originalPrice = '';
                            var isDiscounted = false;
                            
                            if (discountContainer) {
                                // İndirimli ürün
                                originalPrice = discountContainer.querySelector('.CPriceBox-module_price__bYk-c span')?.textContent || '';
                                currentPrice = discountContainer.querySelector('.CPriceBox-module_discountedPrice__15Ffw')?.textContent || '';
                                isDiscounted = true;
                            } else if (normalContainer) {
                                // Normal fiyatlı ürün
                                currentPrice = normalContainer.querySelector('.CPriceBox-module_price__bYk-c')?.textContent || '';
                                originalPrice = '';
                            }
                            
                            return {
                                name: card.querySelector('h2.CProductCard-module_title__u8bMW')?.textContent || '',
                                image: card.querySelector('img')?.src || '',
                                current_price: currentPrice,
                                original_price: originalPrice,
                                is_discounted: isDiscounted,
                                url: productUrl
                            }
                        """, card)
                        
                        if product_data['url'] and not product_data['url'].startswith("http"):
                            product_data['url'] = "https://www.sokmarket.com.tr" + product_data['url']
                        
                        products.append(product_data)
                    except Exception as e:
                        continue
                
                if products:
                    self.results_queue.put((products, category))
                    print(f"\n✓ Toplam {len(products)} ürün başarıyla işlendi")
                    break  # Başarılı olduğunda döngüden çık
                
            except Exception as e:
                if attempt == max_retries - 1:  # Son denemeyse
                    print(f"HATA: {str(e)}")
                continue

    def save_results(self, total_urls):
        """Sonuçları toplama ve kaydetme işlemi"""
        all_products = []
        categories = []
        
        while not self.results_queue.empty():
            products, category = self.results_queue.get()
            all_products.extend(products)
            categories.extend([category] * len(products))
        
        if all_products:
            output_df = pd.DataFrame(all_products)
            
            # Sütun isimlerini düzelt
            column_mapping = {
                'name': 'name',
                'image': 'image_url',
                'current_price': 'current_price',
                'original_price': 'original_price',
                'is_discounted': 'is_discounted',
                'url': 'url'
            }
            
            # Sütun isimlerini değiştir
            output_df = output_df.rename(columns=column_mapping)
            
            # Discount percent hesapla
            output_df['discount_percent'] = None
            mask = output_df['is_discounted'] == True
            output_df.loc[mask, 'discount_percent'] = output_df.loc[mask].apply(
                lambda row: self.calculate_discount(row['original_price'], row['current_price']), 
                axis=1
            )
            
            # Category sütunu ekle
            output_df['category'] = categories
            
            # Sütun sırasını ayarla
            columns = ['name', 'original_price', 'current_price', 'discount_percent', 
                      'image_url', 'url', 'category']
            output_df = output_df[columns]
            
            output_file = f'sok_products_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx'
            output_df.to_excel(output_file, index=False)
            print(f"\n{'='*50}")
            print(f"✓ Sonuçlar {output_file} dosyasına kaydedildi")
            print(f"✓ Toplam {len(all_products)} ürün işlendi")
            print(f"✓ Toplam {total_urls} URL tarandı")
            print(f"✓ Toplam {len(self.processed_urls)} benzersiz URL işlendi")
            print(f"{'='*50}")
        else:
            print("\n❌ İşlenecek ürün bulunamadı!")

    def calculate_discount(self, original_price, current_price):
        """İndirim yüzdesini hesapla"""
        try:
            # Fiyatları temizle ve float'a çevir
            original = float(original_price.replace('₺', '').replace(',', '.').strip())
            current = float(current_price.replace('₺', '').replace(',', '.').strip())
            
            if original > 0:
                discount = ((original - current) / original) * 100
                return round(discount, 2)
        except:
            pass
        return None

    def process_urls(self):
        try:
            # Excel dosyasını oku
            df = pd.read_excel(self.input_file)
            
            # Sadece Şok URL'lerini filtrele
            sok_df = df[df['market'].str.lower() == 'sok'].copy()
            
            if sok_df.empty:
                print("Şok için URL bulunamadı!")
                return
            
            total_urls = len(sok_df)
            print(f"\nToplam işlenecek URL sayısı: {total_urls}")
            
            # URL ve kategori listesini hazırla
            url_category_pairs = list(zip(sok_df['url'], sok_df['category']))
            
            # Thread havuzu oluştur
            with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
                # URL'leri işle
                futures = []
                for url, category in url_category_pairs:
                    with self.url_lock:
                        if url not in self.processed_urls:
                            self.processed_urls.add(url)
                            futures.append(executor.submit(self.scrape_product_page, url, category))
                
                # İşlenen URL sayısını takip et
                completed_urls = 0
                for future in as_completed(futures):
                    completed_urls += 1
                    remaining_urls = total_urls - completed_urls
                    print(f"\nİşlenen URL: {completed_urls}")
                    print(f"Kalan URL sayısı: {remaining_urls} / {total_urls}")
            
            # Tüm sonuçları topla ve Excel'e kaydet
            self.save_results(total_urls)
            
        except Exception as e:
            print(f"İşlem sırasında hata: {str(e)}")
        
        finally:
            self.cleanup()  # Driver'ları temizle

    def cleanup(self):
        """Tüm Chrome driver'ları temizle"""
        print("Cleaning up Chrome drivers...")
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

def main():
    try:
        # Test modu
        test_mode = True  # Test modunu açıp kapatmak için bu değişkeni kullanın
        
        if test_mode:
            # Test için örnek URL ve kategori
            test_url = "https://www.sokmarket.com.tr/atistirmaliklar-c-20376"  # Test etmek istediğiniz URL
            test_category = "Elektronik"  # Test kategorisi
            
            # Scraper'ı başlat ve test fonksiyonunu çağır
            scraper = SokScraper("test")
            scraper.test_single_url(test_url, test_category)
        else:
            # Normal mod - tüm URL'leri işle
            input_file = "C:/Users/Shaumne/Desktop/script/url/url_kategorileri.xlsx"
            scraper = SokScraper(input_file)
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