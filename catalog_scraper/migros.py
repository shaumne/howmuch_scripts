from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from firebase_admin import credentials, firestore, initialize_app
import uuid
from datetime import datetime, timedelta
import time

# Migros kampanya türleri ve kuralları
CAMPAIGN_TYPES = {
    'weekly': {
        'name': 'Haftalık Kampanyalar',
        'days': 7,
        'start_day': 0  # Pazartesi
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
    
    return start_date, end_date

@retry_operation
def scrape_catalog() -> list[dict]:
    """Web sitesinden katalog bilgilerini çeker"""
    driver = webdriver.Chrome()
    catalog_data = []
    
    try:
        driver.get('https://www.migros.com.tr/kampanyalar')
        time.sleep(5)  # Sayfanın yüklenmesi için bekle
        
        # Kampanyalar container'ını bul
        campaigns_container = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, 
                "/html/body/sm-root/div/main/fe-campaigns/div/div/div[5]/mat-tab-group/div/mat-tab-body/div/div"))
        )
        
        # Tüm kampanyaları bul
        campaigns = campaigns_container.find_elements(By.XPATH, "./div")
        print(f"Found {len(campaigns)} campaigns")
        
        start_date, end_date = calculate_campaign_dates('weekly')
        
        for idx, campaign in enumerate(campaigns, 1):
            try:
                # Kampanya resmini al
                image = campaign.find_element(By.XPATH, "./img")
                image_url = image.get_attribute('src')
                
                # Kampanya başlığını al
                title = campaign.find_element(By.XPATH, "./div/span").text
                
                catalog_data.append({
                    'title': title,
                    'imageUrl': image_url,
                    'startDate': start_date,
                    'endDate': end_date,
                    'campaignType': 'weekly',
                    'page': idx
                })
                
                print(f"Processed campaign {idx}: {title}")
                
            except Exception as e:
                print(f"Error processing campaign {idx}: {e}")
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
        # Önce mevcut Migros kataloglarını sil
        catalogs_ref = db.collection('catalogs')
        existing_catalogs = catalogs_ref.where('marketId', '==', 'migros').stream()
        
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
                'marketId': 'migros',
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
        print(f"Successfully processed {len(catalog_data)} Migros catalogs")
    
    except Exception as e:
        print(f"Error updating Firestore: {e}")
        raise e

def main():
    print("Starting Migros catalog processing...")
    
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