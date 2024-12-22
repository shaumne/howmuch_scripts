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
import atexit
import psutil
import os
import random
import re

class CarrefoursaScraper:
    def __init__(self, input_file):
        self.input_file = input_file
        self.max_threads = 30
        self.results_queue = Queue()
        self.thread_local = threading.local()
        self.drivers = []
        self.processed_urls = set()
        self.url_lock = threading.Lock()
        
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]

    def get_driver(self):
        if not hasattr(self.thread_local, "driver"):
            chrome_options = webdriver.ChromeOptions()
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-logging')
            chrome_options.add_argument('--log-level=3')
            chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
            chrome_options.add_argument('--disable-notifications')
            
            # Bot tespitini engelleme
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            thread_id = len(self.drivers)
            user_agent = self.user_agents[thread_id % len(self.user_agents)]
            chrome_options.add_argument(f'user-agent={user_agent}')
            
            driver = webdriver.Chrome(options=chrome_options)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            driver.implicitly_wait(10)
            self.thread_local.driver = driver
            self.drivers.append(driver)
            
        return self.thread_local.driver

    def scroll_and_wait(self, driver):
        """Sayfayı aşağı kaydır ve lazy load elementlerinin yüklenmesini bekle"""
        last_height = driver.execute_script("return document.body.scrollHeight")
        
        while True:
            # Sayfayı aşağı kaydır
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            
            # Lazy load elementlerinin yüklenmesini bekle
            try:
                WebDriverWait(driver, 5).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
            except:
                pass
            
            # Yeni yüksekliği kontrol et
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

    def get_product_data(self, item):
        """Ürün verilerini çek"""
        try:
            # JavaScript ile ürün bilgilerini al
            product_data = self.thread_local.driver.execute_script("""
                var item = arguments[0];
                
                // Resim ve ürün adı
                var img = item.querySelector('img[loading="lazy"]');
                var name = item.querySelector('h3.item-name');
                
                // Fiyatlar
                var originalPrice = item.querySelector('span.priceLineThrough');
                var currentPrice = item.querySelector('span.item-price.js-variant-discounted-price');
                
                // İndirim bilgileri
                var cardDiscount = item.querySelector('span.discount-badge');
                var extraDiscount = item.querySelector('div.at-spt-cont');
                
                // Ürün linki
                var link = item.querySelector('a.product-return');
                
                function getPrice(element) {
                    if (!element) return '';
                    let mainPart = element.childNodes[0].textContent.trim();
                    let formatted = element.querySelector('.formatted-price');
                    if (formatted) {
                        mainPart += formatted.textContent.replace('TL', '').trim();
                    }
                    return mainPart.replace(',', '.').trim();
                }
                
                // İndirim bilgilerini birleştir
                function getDiscounts(cardDisc, extraDisc) {
                    let discounts = [];
                    if (cardDisc) discounts.push(cardDisc.textContent.trim());
                    if (extraDisc) discounts.push(extraDisc.textContent.trim());
                    return discounts.join(' | ');
                }
                
                return {
                    name: name ? name.textContent.trim() : '',
                    image_url: img ? img.getAttribute('data-src') || img.src : '',
                    original_price: originalPrice ? getPrice(originalPrice) : '',
                    current_price: currentPrice ? getPrice(currentPrice) : '',
                    card_discount: getDiscounts(cardDiscount, extraDiscount),
                    url: link ? link.href : ''
                };
            """, item)
            
            # Debug bilgileri
            print(f"\nÜrün: {product_data['name']}")
            print(f"Orijinal Fiyat: {product_data['original_price']}")
            print(f"İndirimli Fiyat: {product_data['current_price']}")
            print(f"İndirimler: {product_data['card_discount']}")
            
            return product_data
            
        except Exception as e:
            print(f"Ürün verisi çekilirken hata: {str(e)}")
            return None

    def process_page(self, url, category):
        """Tek bir sayfayı işle"""
        driver = self.get_driver()
        products = []
        
        try:
            print(f"\nSayfa işleniyor: {url}")
            driver.get(url)
            time.sleep(3)
            
            # Cookie popup'ını kabul et
            try:
                cookie_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.ID, "banner-accept-button"))
                )
                cookie_button.click()
                print("Cookie kabul edildi")
                time.sleep(2)
            except Exception as e:
                print(f"Cookie popup'ı bulunamadı veya zaten kabul edilmiş")
            
            # Lazy load elementlerinin yüklenmesi için scroll
            self.scroll_and_wait(driver)
            
            # Ana ürün listesini bul
            product_list = driver.find_element(By.CLASS_NAME, "product-grid")
            
            # Tüm ürün öğelerini bul ve "ürün önerme" divlerini filtrele
            product_items = driver.execute_script("""
                var items = arguments[0].querySelectorAll('.product-listing-item');
                return Array.from(items).filter(item => {
                    // Ürün önerme divini içeren öğeleri filtrele
                    return !item.querySelector('.advice');
                });
            """, product_list)
            
            print(f"Bulunan ürün sayısı: {len(product_items)}")
            
            for item in product_items:
                product_data = self.get_product_data(item)
                if product_data:
                    product_data['category'] = category
                    products.append(product_data)
            
            return products
            
        except Exception as e:
            print(f"Sayfa işlenirken hata: {str(e)}")
            return []

    def get_all_pages(self, url):
        """Tüm sayfa URL'lerini al"""
        driver = self.get_driver()
        pages = [url]
        
        try:
            driver.get(url)
            time.sleep(3)
            
            # Sayfalama elementlerini bul
            pagination = driver.find_elements(By.CSS_SELECTOR, "div.pagination-item a")
            
            if pagination:
                for page in pagination:
                    page_url = page.get_attribute('href')
                    if page_url and page_url not in pages:
                        pages.append(page_url)
            
        except Exception as e:
            print(f"Sayfa URL'leri alınırken hata: {str(e)}")
        
        return pages

    def scrape_product_page(self, url, category):
        """Tüm sayfaları işle"""
        all_products = []
        
        # Tüm sayfa URL'lerini al
        page_urls = self.get_all_pages(url)
        
        for page_url in page_urls:
            products = self.process_page(page_url, category)
            all_products.extend(products)
        
        if all_products:
            self.results_queue.put(all_products)
            print(f"\n✓ Toplam {len(all_products)} ürün bilgisi başarıyla çekildi")
        else:
            print("\n❌ Bu kategoride işlenebilir ürün bulunamadı")

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

    def save_results(self, total_urls):
        """Sonuçları toplama ve kaydetme işlemi"""
        all_products = []
        while not self.results_queue.empty():
            all_products.extend(self.results_queue.get())
        
        if all_products:
            output_df = pd.DataFrame(all_products)
            output_df = output_df[['name', 'original_price', 'current_price', 'card_discount', 'image_url', 'url', 'category']]
            output_file = f'carrefoursa_products_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx'
            output_df.to_excel(output_file, index=False)
            print(f"\n{'='*50}")
            print(f"✓ Sonuçlar {output_file} dosyasına kaydedildi")
            print(f"✓ Toplam {len(all_products)} ürün işlendi")
            print(f"✓ Toplam {total_urls} URL tarandı")
            print(f"✓ Toplam {len(self.processed_urls)} benzersiz URL işlendi")
            print(f"{'='*50}")
        else:
            print("\n❌ İşlenecek ürün bulunamadı!")

    def cleanup(self):
        """Tüm Chrome driver'ları temizle"""
        print("Cleaning up Chrome drivers...")
        for driver in self.drivers:
            try:
                driver.quit()
            except:
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
            
            # Sadece Carrefoursa market verilerini filtrele
            df = df[df['market'].str.lower() == 'carrefoursa'].copy()
            
            # Boş olmayan URL'leri al
            urls_and_categories = df[['url', 'category']].dropna().values.tolist()
            total_urls = len(urls_and_categories)
            
            if total_urls == 0:
                print("İşlenecek Carrefoursa URL'si bulunamadı!")
                return
            
            print(f"\nToplam {total_urls} Carrefoursa URL'si işlenecek")
            
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

def main():
    try:
        test_mode = False
        
        if test_mode:
            test_url = "https://www.carrefoursa.com/multipack-dondurma/c/1270"
            test_category = "Dondurma"
            
            scraper = CarrefoursaScraper("test")
            scraper.test_single_url(test_url, test_category)
        else:
            input_file = "C:/Users/Shaumne/Desktop/script/url/url_kategorileri.xlsx"
            scraper = CarrefoursaScraper(input_file)
            scraper.process_urls()
        
    except Exception as e:
        print(f"Main process error: {e}")
    
    finally:
        for proc in psutil.process_iter(['pid', 'name']):
            try:
                if 'chromedriver' in proc.info['name'].lower():
                    os.kill(proc.info['pid'], 9)
            except:
                pass

if __name__ == "__main__":
    main() 