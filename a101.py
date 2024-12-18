from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from firebase_admin import credentials, firestore, initialize_app
import uuid
from datetime import datetime, timedelta
import time

# A101 kampanya türleri ve kuralları
CAMPAIGN_TYPES = {
    'weekly': {
        'name': 'Haftanın Yıldızları',
        'days': 7,
        'start_day': 3  # Perşembe
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
    cred = credentials.Certificate('C:\\Users\\Shaumne\\Desktop\\script\\serviceAccoutKey.json')
    initialize_app(cred)
    return firestore.client()

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
    
    try:
        # Navigate to A101 page
        driver.get('https://www.a101.com.tr/afisler-haftanin-yildizlari')
        
        # Wait for cookie consent button and click it
        try:
            cookie_button = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll"))
            )
            cookie_button.click()
        except Exception as e:
            print(f"Cookie consent error: {e}")
        
        # Wait for page to load
        time.sleep(3)
        
        # Get all catalog image URLs
        image_elements = driver.find_elements(By.CSS_SELECTOR, "#img-mapper > img")
        image_urls = [element.get_attribute('src') for element in image_elements]
        
        start_date, end_date = calculate_campaign_dates('weekly')
        
        return [{
            'imageUrl': url,
            'startDate': start_date,
            'endDate': end_date,
            'campaignType': 'weekly',
            'page': idx + 1
        } for idx, url in enumerate(image_urls)]
    
    except Exception as e:
        print(f"Error scraping images: {e}")
        return []
    
    finally:
        driver.quit()

def update_firestore(db, catalog_data: list[dict]) -> None:
    """Firestore'u yeni katalog verileriyle günceller"""
    try:
        # Önce mevcut A101 kataloglarını sil
        catalogs_ref = db.collection('catalogs')
        existing_catalogs = catalogs_ref.where('marketId', '==', 'a101').stream()
        
        for doc in existing_catalogs:
            doc.reference.delete()
            print(f"Deleted old catalog: {doc.id}")

        # Yeni katalogları ekle
        for data in catalog_data:
            catalog_id = f"catalog_{uuid.uuid4().hex[:6]}"
            catalog_ref = catalogs_ref.document(catalog_id)
            
            catalog_ref.set({
                'id': catalog_id,
                'marketId': 'a101',
                'title': CAMPAIGN_TYPES['weekly']['name'],
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
            
            print(f"Created catalog with ID: {catalog_id}")
    
    except Exception as e:
        print(f"Error updating Firestore: {e}")
        raise e

def main():
    print("Starting A101 catalog processing...")
    
    try:
        # Initialize Firebase
        db = initialize_firebase()
        
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
