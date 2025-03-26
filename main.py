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
RANDOM_DELAY_RANGE = (2, 7)  # Add this line for random delay between requests
RETRY_DELAY = 5  # Add this line for retry delay

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
# Add to constants at the top
PAGE_LOAD_TIMEOUT = 90  # Increased from 60 to 90 seconds
ELEMENT_WAIT_TIMEOUT = 60  # Increased from 45 to 60 seconds
SCRIPT_TIMEOUT = 60  # Add script timeout

def fetch_zara_product_info_selenium(url, retry_count=0):
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('--disable-blink-features=AutomationControlled')
    
    try:
        # service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
        
        # Add random delay between requests
        time.sleep(random.uniform(*RANDOM_DELAY_RANGE))
        driver.get(url)

        wait = WebDriverWait(driver, ELEMENT_WAIT_TIMEOUT)

        # Handle geolocation popup if it appears
        try:
            geolocation_button = wait.until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "button[data-qa-action='stay-in-store']")
                )
            )
            geolocation_button.click()
            time.sleep(2)
        except Exception as e:
            print(f"Geolocation button not found: {str(e)}")

        # Wait for main content and scroll
        wait.until(EC.presence_of_element_located((By.ID, 'app-root')))
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)
        driver.execute_script("window.scrollTo(0, 0);")

        # Wait for price element
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "span.price-current__amount")))
        except Exception as e:
            driver.save_screenshot(f"debug_{url.split('/')[-1]}.png")
            print(f"Price element not found, screenshot saved")

        # Rest of the parsing code remains the same...
        
        # Clear cache and cookies
        driver.delete_all_cookies()
        
        try:
            driver.get(url)
        except Exception as e:
            print(f"Initial page load failed: {str(e)}")
            if 'driver' in locals():
                driver.quit()
            return None, url

        # Wait for initial page load with multiple conditions
        try:
            WebDriverWait(driver, ELEMENT_WAIT_TIMEOUT).until(
                EC.presence_of_element_located((By.ID, 'app-root'))
            )
            
            # Additional waits for key elements
            WebDriverWait(driver, ELEMENT_WAIT_TIMEOUT).until(
                EC.presence_of_element_located((By.CLASS_NAME, 'product-detail-info'))
            )
            
            # Scroll to ensure dynamic content loads
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)
            
        except Exception as e:
            print(f"Wait for elements failed: {str(e)}")
            if 'driver' in locals():
                driver.quit()
            return None, url

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        product_info = {
            'brand': 'Zara USA',
            'location': 'United States'
        }

        # Get basic product info
        title_tag = soup.find('title')
        product_info['product_title'] = title_tag.get_text(strip=True) if title_tag else ''
        product_info['short_description'] = soup.find('meta', {'name': 'description'})['content'] if soup.find('meta', {'name': 'description'}) else ''

        # Get price - Updated selector
        price_element = soup.find('span', class_='price-current__amount')
        if price_element:
            money_amount = price_element.find('div', class_='money-amount')
            if money_amount:
                main_amount = money_amount.find('span', class_='money-amount__main')
                if main_amount:
                    product_info['price'] = main_amount.get_text(strip=True)

        # Get product description - Updated selectors
        description_element = None
        description_selectors = [
            'div.product-detail-description.product-detail-info__description p',
            'div.expandable-text__inner-content p',
            'div.product-detail-info__header-content h1'
        ]
        
        for selector in description_selectors:
            description_element = soup.select_one(selector)
            if description_element:
                break
        
        product_info['description'] = description_element.get_text(strip=True) if description_element else ''

        # Get product title - Updated selector
        title_element = soup.find('h1', class_='product-detail-info__header-name')
        if title_element:
            product_info['product_title'] = title_element.get_text(strip=True)
        elif title_tag:  # Fallback to existing title tag
            product_info['product_title'] = title_tag.get_text(strip=True)

        # Enhanced image extraction with multiple selectors
        images = []
        # Try picture tags first
        picture_sources = soup.select('picture.media-image source, picture source[srcset]')
        for source_tag in picture_sources:
            srcset = source_tag.get('srcset', '')
            for item in srcset.split(','):
                url_part = item.strip().split(' ')[0]
                if url_part and ('w=1500' in url_part or 'w=1920' in url_part):
                    images.append(url_part)

        # Try multiple image selectors if needed
        if not images:
            img_selectors = [
                'img.product-media__image',
                'img.media-image__image',
                'img[data-role="product-image"]'
            ]
            for selector in img_selectors:
                img_tags = soup.select(selector)
                for img in img_tags:
                    src = img.get('src', '')
                    if src and ('w=1500' in src or 'w=1920' in src):
                        images.append(src)
                if images:
                    break

        # Process and deduplicate images
        images = [img for img in list(set(images)) 
                 if 'transparent-background' not in img 
                 and not img.endswith('.gif')]
        
        # Take only unique image IDs with better URL handling
        unique_images = []
        seen_ids = set()
        for img in images:
            # Handle both query params and path-based identifiers
            img_id = img.split('?')[0].split('/')[-1]
            if img_id not in seen_ids:
                seen_ids.add(img_id)
                unique_images.append(img)

        product_info['images'] = '|'.join(unique_images[:5])

        driver.quit()
        return product_info, url

    except Exception as e:
        print(f"Error processing {url}: {str(e)}")
        if 'driver' in locals():
            driver.quit()
        return None, url

# Add at the top with other imports and before any function definitions
import threading

# Global variables and locks
csv_lock = threading.Lock()
completed_lock = threading.Lock()
processed_count = 0

# Remove the duplicate process_url function and keep only one version
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
