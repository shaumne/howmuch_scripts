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

class A101Scraper:
    def __init__(self, input_file):
        self.input_file = input_file
        self.max_threads = 20
        self.results_queue = Queue()
        self.thread_local = threading.local()
        self.drivers = []
        self.processed_urls = set()
        self.url_lock = threading.Lock()
        
        # User-Agent listesi
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]

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
            chrome_options.add_experimental_option('prefs', {
                'profile.default_content_setting_values.notifications': 2
            })
            
            # Bot tespitini engelleme
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # Random User-Agent seç
            thread_id = len(self.drivers)
            user_agent = self.user_agents[thread_id % len(self.user_agents)]
            chrome_options.add_argument(f'user-agent={user_agent}')
            
            driver = webdriver.Chrome(options=chrome_options)
            
            # JavaScript ile webdriver özelliklerini gizle
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['tr-TR', 'tr', 'en-US', 'en']})")
            
            driver.implicitly_wait(10)
            self.thread_local.driver = driver
            self.drivers.append(driver)
            
            print(f"Thread {thread_id + 1} created with User-Agent: {user_agent}")
            
        return self.thread_local.driver
    
    def scrape_product_page(self, url, category):
        driver = self.get_driver()
        
        try:
            driver.get(url)
            time.sleep(3)
            
            # Cookie popup'ını kabul et
            try:
                cookie_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, "/html/body/div[2]/div/div[4]/div/div[2]/button[4]"))
                )
                cookie_button.click()
                time.sleep(2)
            except Exception as e:
                pass
            
            # Sayfayı en alta kadar scroll et
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(3)
            
            # Ürün grid div'lerini bul
            grid_selector = (
                "div[class*='gap-2'][class*='grid'][class*='grid-cols-3']"
                "[class*='justify-items-center'][class*='w-full']"
            )
            product_grids = driver.find_elements(By.CSS_SELECTOR, grid_selector)
            
            products = []
            
            for grid in product_grids:
                try:
                    product_items = grid.find_elements(By.CSS_SELECTOR, 
                        "div.w-full.border.cursor-pointer.rounded-2xl.overflow-hidden")
                    
                    for item in product_items:
                        try:
                            product_data = driver.execute_script("""
                                var item = arguments[0];
                                
                                // Resim bilgisini al - düzeltilmiş seçici
                                var imgContainer = item.querySelector('.bg-white.relative');
                                var img = imgContainer ? imgContainer.querySelector('img[draggable="false"]') : null;
                                
                                // Fiyat bilgilerini al ve temizle
                                var originalPrice = item.querySelector('div[class*="text-xs"][class*="text-[#333]"][class*="line-through"]');
                                var currentPrice = item.querySelector('div[class*="text-md"][class*="absolute"][class*="bottom-0"]');
                                
                                // Fiyatları temizle (₺ ve virgül karakterlerini kaldır)
                                function cleanPrice(price) {
                                    if (!price) return '';
                                    return price.textContent.trim()
                                        .replace('₺', '')
                                        .replace(',', '.')
                                        .trim();
                                }
                                
                                // Ürün adını al
                                var name = item.querySelector('div[class*="mobile:text-xs"][class*="line-clamp-3"]');
                                
                                // URL'yi al
                                var link = item.querySelector('a');
                                
                                return {
                                    name: name ? name.textContent.trim() : '',
                                    image_url: img ? img.src : '',
                                    original_price: cleanPrice(originalPrice),
                                    current_price: cleanPrice(currentPrice),
                                    url: link ? link.href : ''
                                };
                            """, item)
                            
                            # İndirim yüzdesini hesapla
                            try:
                                if product_data['original_price'] and product_data['current_price']:
                                    original = float(product_data['original_price'])
                                    current = float(product_data['current_price'])
                                    if original > current:
                                        discount = ((original - current) / original) * 100
                                        product_data['discount_percent'] = round(discount, 2)
                                    else:
                                        product_data['discount_percent'] = None
                                else:
                                    product_data['discount_percent'] = None
                            except:
                                product_data['discount_percent'] = None
                            
                            # URL'yi düzelt
                            if product_data['url'] and not product_data['url'].startswith('https://www.a101.com.tr'):
                                product_data['url'] = "https://www.a101.com.tr" + product_data['url']
                            
                            # Kategori bilgisini ekle
                            product_data['category'] = category
                            
                            products.append(product_data)
                            
                        except Exception as e:
                            print(f"❌ Ürün detayı çekilirken hata: {str(e)}")
                
                except Exception as e:
                    print(f"❌ Grid işlenirken hata: {str(e)}")
            
            # Sonuçları kuyruğa ekle
            if products:
                self.results_queue.put(products)
        
        except Exception as e:
            print(f"❌ HATA: {str(e)}")
    
    def process_urls(self):
        try:
            df = pd.read_excel(self.input_file)
            a101_df = df[df['market'].str.lower() == 'a101'].copy()
            
            if a101_df.empty:
                print("❌ A101 için URL bulunamadı!")
                return
            
            total_urls = len(a101_df)
            url_category_pairs = list(zip(a101_df['url'], a101_df['category']))
            
            with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
                futures = []
                for url, category in url_category_pairs:
                    with self.url_lock:
                        if url not in self.processed_urls:
                            self.processed_urls.add(url)
                            futures.append(executor.submit(self.scrape_product_page, url, category))
                
                completed_urls = 0
                for future in as_completed(futures):
                    completed_urls += 1
                    remaining_urls = total_urls - completed_urls
                    print(f"İşlenen URL: {completed_urls}")
                    print(f"Kalan URL sayısı: {remaining_urls} / {total_urls}")
            
            self.save_results(total_urls)
            
        except Exception as e:
            print(f"❌ İşlem sırasında hata: {str(e)}")
        
        finally:
            self.cleanup()

    def save_results(self, total_urls):
        all_products = []
        while not self.results_queue.empty():
            all_products.extend(self.results_queue.get())
        
        if all_products:
            output_df = pd.DataFrame(all_products)
            output_df = output_df[['name', 'original_price', 'current_price', 'image_url', 'url', 'category', 'discount_percent']]
            output_file = f'a101_products_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx'
            output_df.to_excel(output_file, index=False)
            print(f"✓ {len(all_products)} ürün {output_file} dosyasına kaydedildi")
        else:
            print("❌ İşlenecek ürün bulunamadı!")

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
            test_url = "https://www.a101.com.tr/kapida/anne-bebek/bebek-bezi"
            test_category = "Meyve, Sebze"
            
            # Scraper'ı başlat ve test fonksiyonunu çağır
            scraper = A101Scraper("test")
            scraper.test_single_url(test_url, test_category)
        else:
            # Normal mod - tüm URL'leri işle
            input_file = "C:/Users/Shaumne/Desktop/script/url/url_kategorileri.xlsx"
            scraper = A101Scraper(input_file)
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