from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from firebase_admin import credentials, firestore, initialize_app
import uuid
from datetime import datetime, timedelta
import time

# BİM kampanya türleri ve kuralları
CAMPAIGN_TYPES = {
    'weekly': {
        'name': 'Aktüel Ürünler',
        'days': 7,
        'start_day': 4  # Cuma
    }
}

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

def calculate_campaign_dates(campaign_type: str) -> tuple[datetime, datetime]:
    """Kampanya başlangıç ve bitiş tarihlerini hesaplar"""
    now = datetime.now()
    campaign = CAMPAIGN_TYPES[campaign_type]
    
    # En yakın kampanya gününü bul
    days_until_start = (campaign['start_day'] - now.weekday()) % 7
    start_date = now + timedelta(days=days_until_start)
    end_date = start_date + timedelta(days=campaign['days'] - 1)
    
    # Eğer bugün kampanya günüyse ve öğleden sonraysa, gelecek haftayı hesapla
    if days_until_start == 0 and now.hour >= 12:
        start_date += timedelta(days=7)
        end_date += timedelta(days=7)
    
    return start_date, end_date

@retry_operation
def scrape_catalog() -> list[dict]:
    """Web sitesinden katalog bilgilerini çeker"""
    driver = webdriver.Chrome()
    catalog_data = []
    
    try:
        driver.get('https://www.bim.com.tr/Categories/680/afisler.aspx?top=1&Bim_AfisKey=753')
        time.sleep(5)
        
        # Handle cookie consent
        try:
            cookie_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "/html/body/form/div/footer/div/div[5]/button[1]"))
            )
            cookie_button.click()
            time.sleep(3)
        except Exception as e:
            print(f"Cookie consent error: {e}")
        
        # Find all rows container
        rows_container = driver.find_element(By.XPATH, "/html/body/form/div/div[2]/div/div/div/div[3]")
        rows = rows_container.find_elements(By.XPATH, "./div")
        
        start_date, end_date = calculate_campaign_dates('weekly')
        
        for row_index, row in enumerate(rows, 1):
            try:
                campaign_name = row.find_element(By.XPATH, "./a/span").text
                
                # Get main catalog
                main_catalog = row.find_element(By.XPATH, "./div/div[2]/div/div[1]/a[1]")
                main_catalog_url = main_catalog.get_attribute("href")
                
                if main_catalog_url and 'uploads/afisler' in main_catalog_url:
                    catalog_data.append({
                        'title': f"{campaign_name} - Ana Katalog",
                        'imageUrl': main_catalog_url,
                        'startDate': start_date,
                        'endDate': end_date,
                        'campaignType': 'weekly',
                        'page': 1
                    })
                
                # Get other catalogs
                try:
                    other_catalogs_container = row.find_element(By.XPATH, "./div/div[2]/div/div[2]")
                    other_catalogs = other_catalogs_container.find_elements(By.XPATH, ".//a")
                    
                    for i, catalog in enumerate(other_catalogs, 2):
                        catalog_url = catalog.get_attribute("href")
                        
                        if catalog_url and 'uploads/afisler' in catalog_url:
                            catalog_data.append({
                                'title': f"{campaign_name} - Katalog {i}",
                                'imageUrl': catalog_url,
                                'startDate': start_date,
                                'endDate': end_date,
                                'campaignType': 'weekly',
                                'page': i
                            })
                            
                except Exception as e:
                    print(f"No additional catalogs for this row: {str(e)}")
                    continue
                
            except Exception as e:
                print(f"Error processing row {row_index}: {e}")
                continue
        
        return catalog_data
        
    except Exception as e:
        print(f"Error processing catalogs: {e}")
        return []
        
    finally:
        driver.quit()

def update_firestore(db, catalog_data: list[dict]) -> None:
    """Firestore'u yeni katalog verileriyle günceller"""
    try:
        # Önce mevcut BİM kataloglarını sil
        catalogs_ref = db.collection('catalogs')
        existing_catalogs = catalogs_ref.where('marketId', '==', 'bim').stream()
        
        for doc in existing_catalogs:
            doc.reference.delete()
            print(f"Deleted old catalog: {doc.id}")

        # Yeni katalogları batch işlemle ekle
        batch = db.batch()
        
        for data in catalog_data:
            catalog_id = f"catalog_{uuid.uuid4().hex[:6]}"
            catalog_ref = catalogs_ref.document(catalog_id)
            
            batch.set(catalog_ref, {
                'id': catalog_id,
                'marketId': 'bim',
                'title': data['title'],
                'imageUrl': data['imageUrl'],
                'startDate': data['startDate'],
                'endDate': data['endDate'],
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
        print(f"Successfully processed {len(catalog_data)} BİM catalogs")
    
    except Exception as e:
        print(f"Error updating Firestore: {e}")
        raise e

def main():
    print("Starting BİM catalog processing...")
    
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
