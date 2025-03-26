import os
import time
import csv
import boto3
import xml.etree.ElementTree as ET

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

SITEMAP_FILE = 'sitemap-product-us-en.xml'
COMPLETED_URLS_FILE = 'completed_urls.txt'
MAIN_CSV = 'all_products.csv'
CHUNK_SIZE = 75  # number of products to trigger a split CSV
S3_BUCKET = 'product-scraping'
S3_PREFIX = 'zara_csv/'  # folder within the bucket
CHROME_DRIVER_PATH = 'chromedriver'  # Adjust if needed

def read_sitemap_urls(sitemap_path):
    """
    Parse the sitemap XML file and extract all <loc> URLs.
    """
    tree = ET.parse(sitemap_path)
    root = tree.getroot()
    # Provide the namespace for proper parsing
    ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    # Extract all <loc> tags under <url>
    urls = [loc.text for loc in root.findall('ns:url/ns:loc', ns)]
    return urls

def load_completed_urls(filepath):
    """
    Load completed URLs from a text file into a set.
    If the file doesn't exist, return an empty set.
    """
    if not os.path.exists(filepath):
        return set()
    with open(filepath, 'r', encoding='utf-8') as f:
        return set(line.strip() for line in f)

def append_to_completed_urls(filepath, url):
    """
    Append a single URL to the completed_urls.txt file.
    """
    with open(filepath, 'a', encoding='utf-8') as f:
        f.write(url + '\n')

def write_to_main_csv(filepath, product_info, write_header=False):
    """
    Write a single product_info dict to the main CSV file.
    If write_header is True, write the header row first.
    """
    fieldnames = [
        'product_url',
        'product_title',
        'short_description',
        'price',
        'description',
        'images',
        'brand',
        'location'
    ]
    mode = 'a'
    with open(filepath, mode, newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow(product_info)

def write_chunk_to_csv(chunk, filename):
    """
    Write a list of product_info dicts to a CSV file.
    """
    if not chunk:
        return  # Nothing to write
    fieldnames = [
        'product_url',
        'product_title',
        'short_description',
        'price',
        'description',
        'images',
        'brand',
        'location'
    ]
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in chunk:
            writer.writerow(row)

def upload_to_s3(local_path, bucket, s3_key):
    """
    Upload a local file to S3 at a specified key.
    """
    s3_client = boto3.client('s3')
    s3_client.upload_file(local_path, bucket, s3_key)

def get_product_info(url):
    # --- Selenium setup ---
    options = Options()
    options.add_argument('--headless')  # Enable headless mode
    options.add_argument('--disable-gpu')
    # Set window size to a common desktop resolution
    options.add_argument("window-size=1920,1080")
    # Use a common user-agent string to mimic a real browser
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    )
    # Disable automation flags
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('--disable-blink-features=AutomationControlled')
    
    driver = webdriver.Chrome(options=options)

    product_info = {
        'product_url': url,
        'product_title': '',
        'short_description': '',
        'price': '',
        'description': '',
        'images': '',
        'brand': 'ZARA USA',
        'location': 'USA'
    }

    try:
        driver.get(url)
        wait = WebDriverWait(driver, 15)

        # --- CLICK THE "YES, STAY ON UNITED STATES" BUTTON IF IT APPEARS ---
        try:
            geolocation_button = wait.until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "button[data-qa-action='stay-in-store']")
                )
            )
            if geolocation_button.is_displayed() and geolocation_button.is_enabled():
                geolocation_button.click()
                time.sleep(2)  # Allow time for the page to update after clicking
        except Exception:
            # If not found, just continue
            pass
        
        # --- SCROLL DOWN to help trigger lazy-loaded content ---
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)

        # --- WAIT FOR THE PRICE ELEMENT (best effort) ---
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "span.price-current__amount")))
        except Exception:
            # Not critical – we'll just attempt parse anyway
            pass
        
        # --- PARSE PAGE WITH BEAUTIFULSOUP ---
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        # --- Title & Short Description ---
        title_tag = soup.find('title')
        product_info['product_title'] = title_tag.get_text(strip=True) if title_tag else ''

        meta_desc = soup.find('meta', {'name': 'description'})
        product_info['short_description'] = meta_desc['content'].strip() if meta_desc else ''

        # --- PRICE EXTRACTION ---
        price_element = soup.find('span', class_='price-current__amount')
        if price_element:
            price_wrapper = price_element.find('div', class_='money-amount price-formatted__price-amount')
            if price_wrapper:
                price_main = price_wrapper.find('span', class_='money-amount__main')
                if price_main:
                    product_info['price'] = price_main.get_text(strip=True)
        else:
            product_info['price'] = "Not found"

        # --- DESCRIPTION EXTRACTION ---
        description_element = soup.find('div', class_='product-detail-description product-detail-info__description')
        if description_element:
            product_info['description'] = description_element.get_text(strip=True)
        else:
            header_desc = soup.find('div', class_='product-detail-info__header')
            if header_desc:
                product_info['description'] = header_desc.get_text(strip=True)
            else:
                product_info['description'] = "Not found"

        # --- IMAGE EXTRACTION ---
        images = []
        # 1) Try <picture> tags
        picture_sources = soup.select('picture.media-image source')
        for source_tag in picture_sources:
            srcset = source_tag.get('srcset', '')
            for item in srcset.split(','):
                url_part = item.strip().split(' ')[0]
                if url_part and 'w=1500' in url_part:
                    images.append(url_part)

        # 2) Fallback to <img> tags
        if not images:
            img_tags = soup.find_all('img', class_='product-media__image')
            for img in img_tags:
                src = img.get('src', '')
                if src and 'w=1500' in src:
                    images.append(src)

        # Remove duplicates & filter out "transparent-background"
        images = [img for img in set(images) if 'transparent-background' not in img]

        # Keep only unique base URLs (strip query parameters) and limit to 5
        unique_images = []
        seen_ids = set()
        for img in images:
            base_url = img.split('?')[0]
            if base_url not in seen_ids:
                seen_ids.add(base_url)
                unique_images.append(img)

        product_info['images'] = '|'.join(unique_images[:5])

    finally:
        driver.quit()

    return product_info

def main():
    # 1) Read all URLs from the sitemap
    all_urls = read_sitemap_urls(SITEMAP_FILE)
    total_products = len(all_urls)
    print(f"\nTotal products to process: {total_products}")
    
    # 2) Load the set of completed URLs
    completed_set = load_completed_urls(COMPLETED_URLS_FILE)
    print(f"Already completed products: {len(completed_set)}")
    print(f"Remaining products: {total_products - len(completed_set)}\n")

    # 3) Check if the main CSV exists; if not, we'll write a header
    write_header_needed = not os.path.exists(MAIN_CSV)

    # 4) Prepare to process in chunks
    chunk_data = []
    chunk_count = 1

    # 5) Iterate over each URL
    for index, url in enumerate(all_urls, 1):
        if url in completed_set:
            continue  # skip already processed

        print(f"\n{'='*80}")
        print(f"Processing product {index}/{total_products}")
        print(f"URL: {url}")
        
        # Scrape product info
        product_info = get_product_info(url)
        print(f"Successfully scraped: {product_info['product_title']}")
        print(f"Price: {product_info['price']}")
        print(f"Number of images: {len(product_info['images'].split('|')) if product_info['images'] else 0}")
        
        # Write to main CSV
        write_to_main_csv(MAIN_CSV, product_info, write_header=write_header_needed)
        write_header_needed = False  # only write once if needed

        # Add to our in-memory chunk
        chunk_data.append(product_info)

        # Mark as completed
        append_to_completed_urls(COMPLETED_URLS_FILE, url)
        completed_set.add(url)

        # Once we reach CHUNK_SIZE, write out a split file and upload to S3
        if len(chunk_data) == CHUNK_SIZE:
            split_filename = f'split_{chunk_count}.csv'
            write_chunk_to_csv(chunk_data, split_filename)
            
            # Upload to S3
            s3_key = f'{S3_PREFIX}{split_filename}'
            upload_to_s3(split_filename, S3_BUCKET, s3_key)
            
            print(f'Uploaded {split_filename} to s3://{S3_BUCKET}/{s3_key}')

            # Reset chunk data
            chunk_data = []
            chunk_count += 1

    # If there's any leftover in chunk_data that didn't reach 75,
    # you can decide whether to split it as well:
    if chunk_data:
        split_filename = f'split_{chunk_count}_partial.csv'
        write_chunk_to_csv(chunk_data, split_filename)
        s3_key = f'{S3_PREFIX}{split_filename}'
        upload_to_s3(split_filename, S3_BUCKET, s3_key)
        print(f'Uploaded leftover partial chunk {split_filename} to s3://{S3_BUCKET}/{s3_key}')

    # After processing each product
    print(f"Completed: {len(completed_set)}/{total_products} products")
    print(f"Progress: {(len(completed_set)/total_products)*100:.2f}%")

if __name__ == "__main__":
    main()
