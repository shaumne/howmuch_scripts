from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from firebase_admin import credentials, firestore, initialize_app
import uuid
from datetime import datetime, timedelta
import time
import requests
import base64
import json
import os
from pdf2image import convert_from_path
import tempfile

# Sabitler
IMGBB_API_KEY = "7cdeec56c481908e61d969ca08e5d8f7"
POPPLER_PATH = r"C:\Program Files\poppler-24.08.0\Library\bin"

# Kampanya türleri ve kuralları
CAMPAIGN_TYPES = {
    'wednesday': {
        'name': 'Çarşamba Fırsatları',
        'days': 7,
        'start_day': 2  # Çarşamba
    },
    'saturday': {
        'name': 'Cumartesi Fırsatları',
        'days': 7,
        'start_day': 5  # Cumartesi
    }
}

# Kampanya URL'leri
CAMPAIGNS = [
    {
        'url': 'https://images.ceptesok.com/cms-assets/sub-folder/04_10_aralik_fmcg_nf_032f04f811.pdf',
        'type': 'wednesday'
    },
    {
        'url': 'https://images.ceptesok.com/cms-assets/sub-folder/07_10_Aralik_NF_Poster1_fb26a87ec7.pdf',
        'type': 'saturday'
    }
]

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
def process_pdf(pdf_url: str, campaign_type: str) -> list[dict]:
    """PDF'i indirip görüntüye çevirir ve ImgBB'ye yükler"""
    try:
        # PDF'i indir
        pdf_path = download_pdf(pdf_url)
        if not pdf_path:
            return []
        
        # PDF'i görüntülere çevir
        image_paths = convert_pdf_to_images(pdf_path)
        
        # Görüntüleri ImgBB'ye yükle
        campaign_name = CAMPAIGN_TYPES[campaign_type]['name']
        start_date, end_date = calculate_campaign_dates(campaign_type)
        
        uploaded_images = []
        for i, img_path in enumerate(image_paths, 1):
            image_url = upload_to_imgbb(img_path)
            if image_url:
                uploaded_images.append({
                    'url': image_url,
                    'campaign': campaign_name,
                    'page': i,
                    'startDate': start_date,
                    'endDate': end_date,
                    'campaignType': campaign_type
                })
            time.sleep(2)  # Rate limiting
        
        # Temizlik
        cleanup_files(pdf_path, image_paths)
        return uploaded_images
    
    except Exception as e:
        print(f"Error processing PDF: {e}")
        return []

def update_firestore(db, image_data: dict) -> None:
    """Firestore'u yeni katalog verisiyle günceller"""
    catalog_id = f"catalog_{uuid.uuid4().hex[:6]}"
    catalog_ref = db.collection('catalogs').document(catalog_id)
    
    catalog_ref.set({
        'id': catalog_id,
        'marketId': 'sok',
        'title': f"{image_data['campaign']} - Sayfa {image_data['page']}",
        'imageUrl': image_data['url'],
        'startDate': image_data['startDate'],
        'endDate': image_data['endDate'],
        'isApproved': False,
        'createdAt': firestore.SERVER_TIMESTAMP,
        'page': image_data['page'],
        'rejectionReason': None,
        'approvedAt': None,
        'rejectedAt': None,
        'campaignType': image_data['campaignType']
    })

def initialize_firebase():
    cred = credentials.Certificate('C:\\Users\\Shaumne\\Desktop\\script\\serviceAccoutKey.json')
    initialize_app(cred)
    return firestore.client()

def download_pdf(url):
    try:
        print(f"Downloading PDF from: {url}")
        # Download PDF directly using URL
        response = requests.get(url)
        if response.status_code == 200:
            # Create temp file
            temp_pdf = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf')
            temp_pdf.write(response.content)
            temp_pdf.close()
            print("PDF downloaded successfully")
            return temp_pdf.name
        print(f"Failed to download PDF. Status code: {response.status_code}")
        return None
    except Exception as e:
        print(f"Error downloading PDF: {e}")
        return None

def convert_pdf_to_images(pdf_path):
    try:
        print("Converting PDF to images...")
        # Poppler path'i belirterek dönüştürme işlemi
        images = convert_from_path(pdf_path, 300, poppler_path=POPPLER_PATH)  # 300 DPI
        image_paths = []
        
        # Save each page as image
        for i, image in enumerate(images):
            temp_image = tempfile.NamedTemporaryFile(delete=False, suffix='.jpg')
            image.save(temp_image.name, 'JPEG')
            image_paths.append(temp_image.name)
            print(f"Converted page {i+1}/{len(images)}")
        
        return image_paths
    except Exception as e:
        print(f"Error converting PDF: {e}")
        return []

def upload_to_imgbb(image_path):
    try:
        with open(image_path, 'rb') as file:
            # Read image file and convert to base64
            image_base64 = base64.b64encode(file.read()).decode('utf-8')
        
        url = "https://api.imgbb.com/1/upload"
        payload = {
            "key": IMGBB_API_KEY,
            "image": image_base64,
        }
        
        response = requests.post(url, payload)
        if response.status_code == 200:
            json_data = response.json()
            return json_data['data']['display_url']
        else:
            print(f"ImgBB API error: {response.text}")
        
        return None
    except Exception as e:
        print(f"Error uploading to ImgBB: {e}")
        return None

def cleanup_files(pdf_path, image_paths):
    try:
        # Remove PDF file
        if pdf_path and os.path.exists(pdf_path):
            os.remove(pdf_path)
            print("PDF file removed")
        
        # Remove image files
        for img_path in image_paths:
            if os.path.exists(img_path):
                os.remove(img_path)
        print("Image files removed")
    except Exception as e:
        print(f"Error during cleanup: {e}")

def main():
    print("Starting catalog processing...")
    db = initialize_firebase()
    
    try:
        # Önce eski katalogları sil
        catalogs_ref = db.collection('catalogs')
        old_catalogs = catalogs_ref.where('marketId', '==', 'sok').stream()
        for doc in old_catalogs:
            doc.reference.delete()
            print(f"Deleted old catalog: {doc.id}")
        
        # Yeni katalogları işle
        for campaign in CAMPAIGNS:
            campaign_images = process_pdf(campaign['url'], campaign['type'])
            
            for image_data in campaign_images:
                update_firestore(db, image_data)
            
            time.sleep(5)  # Kampanyalar arası bekleme
        
    except Exception as e:
        print(f"Error in main process: {e}")

if __name__ == "__main__":
    main()