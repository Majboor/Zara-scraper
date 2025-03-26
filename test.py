from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time

def get_product_info(url):
    # --- Selenium setup ---
    options = Options()
    options.add_argument('--headless')  # Enable headless mode
    options.add_argument('--disable-gpu')
    # Set window size to a common desktop resolution
    options.add_argument("window-size=1920,1080")
    # Use a common user-agent string to mimic a real browser
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")
    # Disable automation flags
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('--disable-blink-features=AutomationControlled')
    
    driver = webdriver.Chrome(options=options)

    try:
        driver.get(url)
        wait = WebDriverWait(driver, 15)

        # --- CLICK THE "YES, STAY ON UNITED STATES" BUTTON IF IT APPEARS ---
        try:
            geolocation_button = wait.until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "button[data-qa-action='stay-in-store']")
                )
            )
            geolocation_button.click()
            time.sleep(2)  # Allow time for the page to update after clicking
        except Exception as e:
            print("Geolocation button not found or not clickable:", e)
        
        # --- SCROLL DOWN to help trigger lazy-loaded content ---
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)

        # --- WAIT FOR THE PRICE ELEMENT ---
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "span.price-current__amount")))
        except Exception as e:
            # Save a screenshot for debugging
            driver.save_screenshot("debug_screenshot.png")
            print("Price element not found after waiting; screenshot saved as debug_screenshot.png")
        
        # --- PARSE PAGE WITH BEAUTIFULSOUP ---
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        product_info = {}

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
            img_id = img.split('?')[0]
            if img_id not in seen_ids:
                seen_ids.add(img_id)
                unique_images.append(img)
        product_info['images'] = '|'.join(unique_images[:5])

        return product_info

    finally:
        driver.quit()

if __name__ == "__main__":
    url = "https://www.zara.com/us/en/combination-skirt-limited-edition-p01966917.html"
    info = get_product_info(url)
    print(info)
