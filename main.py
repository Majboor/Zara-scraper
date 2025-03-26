from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import csv
import os
import concurrent.futures
import queue
from urllib.parse import urlparse
import threading
import xml.etree.ElementTree as ET
import random
# Constants
MAX_THREADS = 10
CSV_FILE = 'zara_products.csv'
COMPLETED_URLS_FILE = 'completed_urls.txt'
CSV_HEADERS = ['brand', 'location', 'product_title', 'short_description', 'price', 'description', 'images']
SITEMAP_FILE = 'sitemap-product-us-en.xml'

def get_urls_from_sitemap():
    try:
        tree = ET.parse(SITEMAP_FILE)
        root = tree.getroot()
        # Handle namespace in the XML
        namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        urls = []
        for url in root.findall('.//ns:url', namespace):
            loc = url.find('ns:loc', namespace)
            if loc is not None:
                urls.append(loc.text)
        return urls
    except Exception as e:
        print(f"Error parsing sitemap: {str(e)}")
        return []

def get_pending_urls():
    # Read all URLs from sitemap
    all_urls = get_urls_from_sitemap()
    
    # Read completed URLs
    completed_urls = set()
    if os.path.exists(COMPLETED_URLS_FILE):
        with open(COMPLETED_URLS_FILE, 'r') as f:
            completed_urls = set(line.strip() for line in f)
    
    # Return only pending URLs
    return [url for url in all_urls if url not in completed_urls]

# Add new constants
MAX_THREADS = 10
# Update constants
CSV_FILE = 'zara_us_products.csv'
COMPLETED_URLS_FILE = 'completed_urls_us.txt'
CSV_HEADERS = ['brand', 'location', 'product_title', 'short_description', 'price', 'description', 'images']
SITEMAP_FILE = 'sitemap-product-us-en.xml'

# Update product info in fetch function
def fetch_zara_product_info_selenium(url, retry_count=0):
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    try:
        # Create a Service object using ChromeDriverManager
        service = Service(ChromeDriverManager().install())
        
        # Initialize the Chrome driver with the service and options parameters
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(30)
        
        # Add random delay between requests
        time.sleep(random.uniform(*RANDOM_DELAY_RANGE))
        
        driver.get(url)

        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.ID, 'app-root'))
        )

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        product_info = {
            'brand': 'Zara USA',
            'location': 'United States'
        }

        # Get basic product info
        title_tag = soup.find('title')
        product_info['product_title'] = title_tag.get_text(strip=True) if title_tag else ''
        product_info['short_description'] = soup.find('meta', {'name': 'description'})['content'] if soup.find('meta', {'name': 'description'}) else ''

        # Get price
        price_element = soup.find('span', class_='price-current__amount')
        if price_element:
            price_wrapper = price_element.find('div', class_='money-amount price-formatted__price-amount')
            if price_wrapper:
                price_main = price_wrapper.find('span', class_='money-amount__main')
                if price_main:
                    product_info['price'] = price_main.get_text(strip=True)

        # Get product description
        description_element = soup.find('div', class_='product-detail-info__header')
        if description_element:
            product_info['description'] = description_element.get_text(strip=True)

        # Images
        images = []
        picture_sources = soup.select('picture.media-image source')
        for source_tag in picture_sources:
            srcset = source_tag.get('srcset', '')
            for item in srcset.split(','):
                url_part = item.strip().split(' ')[0]
                if url_part and 'w=1500' in url_part:  # Only get high-res images
                    images.append(url_part)

        img_tags = soup.find_all('img', class_=lambda c: c and 'media-image__image' in c)
        for img_tag in img_tags:
            src = img_tag.get('src', '')
            if src and 'w=1500' in src:  # Only get high-res images
                images.append(src)

        # Remove duplicates and filter out transparent background
        images = [img for img in list(set(images)) 
                 if 'transparent-background' not in img]
        
        # Take only unique image IDs (remove size variants)
        unique_images = []
        seen_ids = set()
        for img in images:
            img_id = img.split('?')[0]  # Get base URL without parameters
            if img_id not in seen_ids:
                seen_ids.add(img_id)
                unique_images.append(img)

        product_info['images'] = '|'.join(unique_images[:5])  # Limit to 5 unique images

        driver.quit()
        return product_info, url

    except Exception as e:
        print(f"Error processing {url}: {str(e)}")
        if 'driver' in locals():
            driver.quit()
        return None, url

def process_url(url):
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            print(f"\n[Starting] Processing URL: {url} (Attempt {attempt + 1}/{max_attempts})")
            product_info, url = fetch_zara_product_info_selenium(url)
            if product_info:
                save_to_csv(product_info)
                mark_url_as_completed(url)
                print(f"[Success] Processed: {url}")
                print(f"[Details] Title: {product_info['product_title']}")
                print(f"[Details] Price: {product_info.get('price', 'N/A')}")
                print(f"[Details] Images count: {len(product_info['images'].split('|'))}")
                break
            else:
                print(f"[Error] Failed to process: {url}")
                time.sleep(RETRY_DELAY * (attempt + 1))
        except Exception as e:
            print(f"[Error] Exception while processing {url}: {str(e)}")
            time.sleep(RETRY_DELAY * (attempt + 1))

def save_to_csv(product_data):
    global processed_count
    with csv_lock:
        file_exists = os.path.exists(CSV_FILE)
        with open(CSV_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
            if not file_exists:
                writer.writeheader()
            writer.writerow(product_data)
        processed_count += 1
        total_urls = len(get_urls_from_sitemap())
        print(f"\n[Progress] Processed {processed_count}/{total_urls} products ({(processed_count/total_urls)*100:.2f}%)")

def mark_url_as_completed(url):
    with completed_lock:
        with open(COMPLETED_URLS_FILE, 'a') as f:
            f.write(f"{url}\n")
        print(f"[Completed] Added to tracking file: {url}")

def process_url(url):
    try:
        print(f"\n[Starting] Processing URL: {url}")
        product_info, url = fetch_zara_product_info_selenium(url)
        if product_info:
            save_to_csv(product_info)
            mark_url_as_completed(url)
            print(f"[Success] Processed: {url}")
            print(f"[Details] Title: {product_info['product_title']}")
            print(f"[Details] Price: {product_info.get('price', 'N/A')}")
            print(f"[Details] Images count: {len(product_info['images'].split('|'))}")
        else:
            print(f"[Error] Failed to process: {url}")
    except Exception as e:
        print(f"[Error] Exception while processing {url}: {str(e)}")

def main():
    start_time = time.time()
    print("[Setup] Creating necessary files...")
    
    # Create files if they don't exist
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
            writer.writeheader()
            print(f"[Setup] Created new CSV file: {CSV_FILE}")
    
    if not os.path.exists(COMPLETED_URLS_FILE):
        open(COMPLETED_URLS_FILE, 'w').close()
        print(f"[Setup] Created new tracking file: {COMPLETED_URLS_FILE}")

    pending_urls = get_pending_urls()
    total_urls = len(get_urls_from_sitemap())
    
    if not pending_urls:
        print("\n[Status] No pending URLs to process. All URLs have been completed!")
        return
    
    print(f"\n[Status] Total URLs in sitemap: {total_urls}")
    print(f"[Status] Pending URLs to process: {len(pending_urls)}")
    print(f"[Status] Already processed: {total_urls - len(pending_urls)}")
    print(f"[Status] Using {MAX_THREADS} concurrent threads")
    print("\n[Starting] Beginning processing...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        executor.map(process_url, pending_urls)

    end_time = time.time()
    duration = end_time - start_time
    print(f"\n[Completed] Finished processing all URLs")
    print(f"[Summary] Total time: {duration:.2f} seconds")
    print(f"[Summary] Average time per product: {duration/len(pending_urls):.2f} seconds")
    print(f"[Summary] Total products processed in this session: {len(pending_urls)}")

if __name__ == '__main__':
    main()
