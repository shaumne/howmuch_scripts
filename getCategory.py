import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor
import random
from tqdm import tqdm
import warnings
import threading
import time
import os
import psutil
import requests
from datetime import datetime

class BreadcrumbExtractor:
    def __init__(self):
        self.max_retries = 3
        self.thread_local = threading.local()
        self.drivers = []
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0'
        ]

    def get_driver(self):
        """Her thread için ayrı bir driver oluştur"""
        if not hasattr(self.thread_local, "driver"):
            chrome_options = webdriver.ChromeOptions()
            # chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-logging')
            chrome_options.add_argument('--log-level=3')
            chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
            
            # Bot tespitini engelleme
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # CloudFront için ek ayarlar
            chrome_options.add_argument('--disable-web-security')
            chrome_options.add_argument('--allow-running-insecure-content')
            chrome_options.add_argument('--ignore-certificate-errors')
            chrome_options.add_argument('--window-size=1920,1080')
            
            # Random User-Agent seç
            user_agent = random.choice(self.user_agents)
            chrome_options.add_argument(f'user-agent={user_agent}')
            
            driver = webdriver.Chrome(options=chrome_options)
            
            # JavaScript ile webdriver özelliklerini gizle
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['tr-TR', 'tr', 'en-US', 'en']})")
            driver.execute_script("Object.defineProperty(navigator, 'platform', {get: () => 'Win32'})")
            
            # CloudFront için ek JavaScript
            driver.execute_script("""
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5].map(() => ({
                        name: Math.random().toString(36),
                        description: Math.random().toString(36)
                    }))
                });
            """)
            
            self.thread_local.driver = driver
            self.drivers.append(driver)
        return self.thread_local.driver

    def cleanup(self):
        """Tüm Chrome driver'ları temizle"""
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
            except:
                pass

    def extract_sok(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        breadcrumb = soup.find('div', class_='Breadcrumb_breadcrumbs__4gBPU')
        if breadcrumb:
            items = [a.text for a in breadcrumb.find_all('a')] + \
                   [p.text for p in breadcrumb.find_all('p', class_='Breadcrumb_lastCrumb__5leb0')]
            return self.clean_breadcrumb(' > '.join(items))
        return None

    def extract_a101(self, driver):
        try:
            breadcrumb = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.flex.flex-wrap"))
            )
            items = [a.text for a in breadcrumb.find_elements(By.TAG_NAME, "a")]
            return self.clean_breadcrumb(' > '.join(items))
        except:
            return None

    def extract_migros(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        breadcrumb = soup.find('ul', class_='breadcrumbs')
        if breadcrumb:
            items = [a.text for a in breadcrumb.find_all('a')]
            return self.clean_breadcrumb(' > '.join(items))
        return None

    def clean_breadcrumb(self, text):
        """Gereksiz kelimeleri ve menü linklerini temizle"""
        # İstenmeyen kelimeler listesi
        remove_words = [
            'Anasayfa', 'Market', 'kapida', 'Ana Sayfa', 'Kampanyalar', 'Hakkımızda',
            'Bilgi Toplumu Hizmetleri', 'Kişisel Verilerin Korunması', 'İletişim',
            'Canlı Destek', 'İptal İade Koşulları', 'Üyelik Sözleşmesi',
            'Ön Bilgilendirme Formu', 'Mesafeli Satış Sözleşmesi', 'Aydınlatma Metni',
            'Açık Rıza Metni', 'Cayma Hakkı', 'Müşteri Danışma Hattı Aydınlatma Metni'
        ]
        
        # Boş string'leri ve istenmeyen kelimeleri temizle
        items = [item.strip() for item in text.split('>')]
        cleaned_items = [
            item for item in items 
            if item and item.strip() not in remove_words
        ]
        
        # En az bir geçerli kategori varsa birleştir
        if cleaned_items:
            return ' > '.join(cleaned_items)
        return None

    def process_url(self, args):
        """Tek bir URL'yi işle"""
        url, market_name = args
        
        if pd.isna(url) or not isinstance(url, str) or url.strip() == '':
            return None
            
        for attempt in range(self.max_retries):
            try:
                # Her denemede farklı bir bekleme süresi
                time.sleep(random.uniform(2, 5))
                
                if market_name.lower() == 'a101':
                    driver = self.get_driver()
                    driver.delete_all_cookies()  # Her denemede cookie'leri temizle
                    
                    # CloudFront için ek headers
                    driver.execute_cdp_cmd('Network.setExtraHTTPHeaders', {
                        'headers': {
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                            'Accept-Language': 'tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7',
                            'Accept-Encoding': 'gzip, deflate, br',
                            'Connection': 'keep-alive',
                            'Upgrade-Insecure-Requests': '1',
                            'Sec-Fetch-Dest': 'document',
                            'Sec-Fetch-Mode': 'navigate',
                            'Sec-Fetch-Site': 'none',
                            'Sec-Fetch-User': '?1'
                        }
                    })
                    
                    driver.get(url)
                    result = self.extract_a101(driver)
                    if result:
                        return result
                    
                else:  # Diğer marketler için mevcut kod...
                    response = requests.get(url.strip(), headers={'User-Agent': random.choice(self.user_agents)})
                    if market_name.lower() == 'sok':
                        return self.extract_sok(response.text)
                    elif market_name.lower() == 'migros':
                        return self.extract_migros(response.text)
                    
            except Exception as e:
                print(f"Deneme {attempt + 1} başarısız: {str(e)}")
                if attempt == self.max_retries - 1:
                    with self.url_lock:
                        self.failed_urls.append({
                            'url': url,
                            'market': market_name,
                            'error': str(e)
                        })
                continue
                
        return None

    def process_excel(self, file_path, market_name):
        try:
            df = pd.read_excel(file_path)
            
            if 'url' not in df.columns:
                print(f"Hata: {file_path} dosyasında 'url' sütunu bulunamadı!")
                return
            
            valid_df = df[df['url'].notna() & (df['url'].str.strip() != '')].copy()
            skipped_count = len(df) - len(valid_df)
            
            if skipped_count > 0:
                print(f"\n{skipped_count} adet boş URL atlandı.")
            
            if len(valid_df) == 0:
                print("İşlenecek URL bulunamadı!")
                return
            
            urls = [(row['url'], market_name) for _, row in valid_df.iterrows()]
            
            df['breadcrumb'] = None
            
            # ThreadPoolExecutor ile işle
            with ThreadPoolExecutor(max_workers=10) as executor:
                results = list(tqdm(
                    executor.map(self.process_url, urls),
                    total=len(urls),
                    desc=f"{market_name} işleniyor"
                ))
                
                df.loc[valid_df.index, 'breadcrumb'] = results
            
            # Her market için ayrı sonuç dosyası
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            output_file = f"{file_path.rsplit('.', 1)[0]}_breadcrumbs_{timestamp}.xlsx"
            df.to_excel(output_file, index=False)
            
            # Başarısız URL'leri kaydet
            failed_urls = [url for url, result in zip(urls, results) if result is None]
            if failed_urls:
                failed_df = pd.DataFrame(failed_urls, columns=['url', 'market'])
                failed_file = f"failed_{market_name}_{timestamp}.xlsx"
                failed_df.to_excel(failed_file, index=False)
                print(f"\nBaşarısız URL'ler {failed_file} dosyasına kaydedildi.")
            
            # İstatistikleri göster
            success_count = df['breadcrumb'].notna().sum()
            print(f"\n{market_name.upper()} için işlem tamamlandı:")
            print(f"Toplam ürün: {len(df)}")
            print(f"Boş URL: {skipped_count}")
            print(f"Başarılı: {success_count}")
            print(f"Başarısız: {len(valid_df) - success_count}")
            print(f"Sonuçlar {output_file} dosyasına kaydedildi.")
            
        except Exception as e:
            print(f"Excel işleme hatası: {str(e)}")
        finally:
            self.cleanup()

def main():
    extractor = BreadcrumbExtractor()
    
    # Her market için ayrı Excel dosyaları
    markets = {
        'a101': 'C:/Users/Shaumne/Desktop/script/products/a101_products.xlsx',
        'migros': 'C:/Users/Shaumne/Desktop/script/products/migros_products.xlsx'
    }
    
    for market, file in markets.items():
        print(f"\n{market.upper()} işleniyor...")
        try:
            extractor.process_excel(file, market)
            print(f"{market.upper()} işlemi tamamlandı ve kaydedildi.")
            time.sleep(5)  # Marketler arası bekleme süresi
        except Exception as e:
            print(f"{market.upper()} işlenirken hata oluştu: {str(e)}")
            continue  # Hata olsa bile diğer markete geç

if __name__ == "__main__":
    main()
