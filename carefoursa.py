from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
import pdf2image
import os
import time
from firebase_admin import credentials, firestore, initialize_app
import uuid
from datetime import datetime, timedelta
import tempfile

# CarrefourSA için sabitler
IMGBB_API_KEY = "7cdeec56c481908e61d969ca08e5d8f7"
POPPLER_PATH = r"C:\Program Files\poppler-24.08.0\Library\bin"

def retry_operation(func, max_retries=3, delay=2):
    """Başarısız işlemleri tekrar dener"""
    def wrapper(*args, **kwargs):
        retries = 0
        while retries < max_retries:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                retries += 1
                if retries == max_retries:
                    raise e
                print(f"Retry {retries}/{max_retries} after error: {e}")
                time.sleep(delay * retries)
    return wrapper

def initialize_firebase():
    """Firebase'i başlatır"""
    try:
        cred = credentials.Certificate('C:\\Users\\Shaumne\\Desktop\\script\\serviceAccoutKey.json')
        initialize_app(cred)
        return firestore.client()
    except Exception as e:
        print(f"Error initializing Firebase: {e}")
        return None

@retry_operation
def upload_to_imgbb(image_path):
    """Görüntüyü ImgBB'ye yükler"""
    try:
        with open(image_path, "rb") as file:
            payload = {
                "key": IMGBB_API_KEY,
            }
            files = {
                "image": file
            }
            response = requests.post("https://api.imgbb.com/1/upload", data=payload, files=files)
            json_response = response.json()
            
            if 'data' in json_response and 'url' in json_response['data']:
                return json_response['data']
            else:
                print(f"ImgBB API error: {json_response}")
                return None
    except Exception as e:
        print(f"Error uploading to imgbb: {str(e)}")
        return None

def cleanup_files(paths):
    """Geçici dosyaları temizler"""
    for path in paths:
        try:
            if os.path.exists(path):
                os.remove(path)
                print(f"Cleaned up: {path}")
        except Exception as e:
            print(f"Error cleaning up {path}: {e}")

@retry_operation
def process_pdf(pdf_url, catalog_title, page_num) -> dict:
    """PDF'i işler ve görüntüye çevirir"""
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            # PDF'i indir
            response = requests.get(pdf_url)
            pdf_path = os.path.join(temp_dir, f'catalog_{page_num}.pdf')
            
            with open(pdf_path, 'wb') as f:
                f.write(response.content)
            
            # PDF'i görüntüye çevir
            images = pdf2image.convert_from_path(
                pdf_path,
                poppler_path=POPPLER_PATH,
                dpi=200
            )
            
            image_path = os.path.join(temp_dir, f'page_{page_num}.jpg')
            images[0].save(image_path, 'JPEG', quality=85)
            
            # ImgBB'ye yükle
            imgbb_response = upload_to_imgbb(image_path)
            
            if imgbb_response and 'url' in imgbb_response:
                return {
                    'title': f"{catalog_title} - Sayfa {page_num}",
                    'imageUrl': imgbb_response['url'],
                    'page': page_num,
                    'campaignType': 'weekly'
                }
            return None
            
        except Exception as e:
            print(f"Error processing PDF: {e}")
            return None

@retry_operation
def scrape_catalog() -> list[dict]:
    """Web sitesinden katalog bilgilerini çeker"""
    driver = webdriver.Chrome()
    catalog_data = []
    
    try:
        driver.get('https://www.carrefoursa.com/kataloglar')
        time.sleep(3)
        
        # Cookie'yi kabul et
        try:
            cookie_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "/html/body/div[2]/div/div[2]/button[1]"))
            )
            cookie_button.click()
            time.sleep(2)
        except Exception as e:
            print(f"Cookie consent error: {e}")
        
        # Katalog container'ını bul
        catalog_container = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '/html/body/main/div[4]/div[2]/div[3]/div/div/div[2]'))
        )
        
        catalog_links = catalog_container.find_elements(By.XPATH, './/ul/li/p/a')
        
        for i, link in enumerate(catalog_links, 1):
            try:
                catalog_url = link.get_attribute('href')
                catalog_title = link.text.strip()
                
                driver.execute_script("window.open('');")
                driver.switch_to.window(driver.window_handles[-1])
                driver.get(catalog_url)
                time.sleep(3)
                
                pdf_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "/html/body/div[2]/div/div[1]/nav/a[5]"))
                )
                pdf_url = pdf_button.get_attribute('href')
                
                if pdf_url:
                    result = process_pdf(pdf_url, catalog_title, i)
                    if result:
                        catalog_data.append(result)
                
                driver.close()
                driver.switch_to.window(driver.window_handles[0])
                
            except Exception as e:
                print(f"Error processing catalog {i}: {e}")
                if len(driver.window_handles) > 1:
                    driver.close()
                    driver.switch_to.window(driver.window_handles[0])
                continue
        
        return catalog_data
        
    except Exception as e:
        print(f"Error scraping catalogs: {e}")
        return []
        
    finally:
        driver.quit()

def update_firestore(db, catalog_data: list[dict]) -> None:
    """Firestore'u yeni katalog verileriyle günceller"""
    try:
        # Önce mevcut Carrefour kataloglarını sil
        catalogs_ref = db.collection('catalogs')
        existing_catalogs = catalogs_ref.where('marketId', '==', 'carrefoursa').stream()
        
        for doc in existing_catalogs:
            doc.reference.delete()
            print(f"Deleted old catalog: {doc.id}")

        # Yeni katalogları batch işlemle ekle
        batch = db.batch()
        
        # Varsayılan olarak 1 haftalık geçerlilik süresi
        now = datetime.now()
        start_date = now
        end_date = now + timedelta(days=7)
        
        for data in catalog_data:
            catalog_id = f"catalog_{uuid.uuid4().hex[:6]}"
            catalog_ref = catalogs_ref.document(catalog_id)
            
            batch.set(catalog_ref, {
                'id': catalog_id,
                'marketId': 'carrefoursa',
                'title': data['title'],
                'imageUrl': data['imageUrl'],
                'startDate': start_date,
                'endDate': end_date,
                'isApproved': False,
                'createdAt': firestore.SERVER_TIMESTAMP,
                'page': data['page'],
                'rejectionReason': None,
                'approvedAt': None,
                'rejectedAt': None,
                'campaignType': data['campaignType']
            })
        
        # Batch işlemi commit et
        batch.commit()
        print(f"Successfully processed {len(catalog_data)} Carrefour catalogs")
    
    except Exception as e:
        print(f"Error updating Firestore: {e}")
        raise e

def main():
    print("Starting Carrefour catalog processing...")
    
    try:
        # Initialize Firebase
        db = initialize_firebase()
        if not db:
            raise Exception("Failed to initialize Firebase")
        
        # Scrape catalog data
        catalog_data = scrape_catalog()
        
        if catalog_data and len(catalog_data) > 0:
            # Update Firestore with all images
            update_firestore(db, catalog_data)
            print(f"Successfully processed {len(catalog_data)} catalog images")
        else:
            print("Failed to get catalog images")
    
    except Exception as e:
        print(f"Error in main process: {e}")

if __name__ == "__main__":
    main()
