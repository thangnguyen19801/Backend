import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import requests
from kafka import KafkaProducer
import json
import logging
from time import sleep
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import easyocr 

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:55.0) Gecko/20100101 Firefox/55.0',
]

HEADERS = {'Accept-Language': 'en-US,en;q=0.5'}

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

reader = easyocr.Reader(['en'])

def solve_captcha(captcha_image_url):
    # Download the CAPTCHA image
    response = requests.get(captcha_image_url, stream=True)
    if response.status_code == 200:
        with open('captcha.jpg', 'wb') as file:
            file.write(response.content)

        # Use EasyOCR to read text from the image
        result = reader.readtext('captcha.jpg')
        if result:
            # Extract the text from the OCR result
            captcha_text = result[0][-2]
            logging.info(f"CAPTCHA text: {captcha_text}")
            return captcha_text
    return None

def handle_captcha(url, headers):
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    captcha_image_url = soup.find('img')['src']
    captcha_solution = solve_captcha(captcha_image_url)
    
    if captcha_solution:
        form_action = soup.find('form')['action']
        amzn = soup.find('input', {'name': 'amzn'})['value']
        amzn_r = soup.find('input', {'name': 'amzn-r'})['value']
        
        captcha_data = {
            'amzn': amzn,
            'amzn-r': amzn_r,
            'field-keywords': captcha_solution
        }
        
        captcha_response = requests.get(f'https://www.amazon.com{form_action}', headers=headers, params=captcha_data)
        return captcha_response
    return None

# Initialize KafkaProducer with your Kafka broker(s) configuration
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Function to check if a substring is convertible to a number
def is_convertible_to_number(substring):
    try:
        float(substring)
        return True
    except ValueError:
        return False

# Function to extract Product Title
def get_title(soup):
    try:
        # Outer Tag Object
        title = soup.select_one('#productTitle')
        # Inner NavigatableString Object
        title_value = title.text

        # Title as a string value
        title_string = title_value.strip()

    except AttributeError:
        try:
            # Outer Tag Object
            title = soup.find("h1", attrs={"id":'title'})
            # Inner NavigatableString Object
            title_value = title.text

            # Title as a string value
            title_string = title_value.strip()

        except AttributeError:
            title_string = ""

    return title_string

# Function to extract Product Price
def get_price(soup):
    try:
        price_string = soup.find("span", attrs={'class': 'a-offscreen'}).string.strip()[1:]
        price = float(price_string)
    except (AttributeError, ValueError):
        price = None
    return price

def get_discount(soup):
    try:
        discount_string = soup.find("span", attrs={'class': 'a-size-large a-color-price savingPriceOverride aok-align-center reinventPriceSavingsPercentageMargin savingsPercentage'}).string.strip()[1:-1]
        discount = float(discount_string)
    except (AttributeError, ValueError):
        discount = 0
    return discount

def get_brand(soup):
    try:
        tr_element = soup.find('tr', attrs={'class':'po-brand'})
        td_element = tr_element.find_all('td')[1]

        # Title as a string value
        brand_string = td_element.text.strip()
    except (AttributeError, ValueError):
        brand_string = ""
    return brand_string

# Function to extract Product Rating
def get_rating(soup):
    try:
        rating_string = soup.find("i", attrs={'class': 'a-icon a-icon-star a-star-4-5'}).string.strip().split()[0]
        rating = float(rating_string)
    except (AttributeError,ValueError) as e:
        try:
            rating_string = soup.find("span", attrs={'class': 'a-icon-alt'}).string.strip().split()[0]
            rating = float(rating_string)
        except AttributeError:
            rating = None
    return rating

# Function to extract Number of User Reviews
def get_review_count(soup):
    try:
        review_count_string = soup.find("span", attrs={'id': 'acrCustomerReviewText'}).string.strip().split()[0]
        review_count_string_no_commas = review_count_string.replace(",", "")
        review_count = int(review_count_string_no_commas)
    except AttributeError:
        review_count = 0
    return review_count

# Function to extract Availability Status
def get_availability(soup):
    try:
        available = soup.find("div", attrs={'id': 'availability'})
        available = available.find("span").string.strip()
    except AttributeError:
        available = "Not Available"
    return available

def get_detail(soup):
    try:
        table = soup.find('table', {'id': 'productDetails_detailBullets_sections1'})
        rows = table.find_all('tr')

        # Create the dictionary
        detail = {}
        for row in rows:
            header = row.find('th').get_text(strip=True)
            value = row.find('td').get_text(strip=True)
            detail[header] = value
    except:
        detail = {}
    return detail

def fetch_product_details(product, page, headers):
    url = f"https://www.amazon.com/s?k={product}&page={page}&ref=nb_sb_noss_2"
    try:
        webpage = requests.get(url, headers=headers)
        soup = BeautifulSoup(webpage.content, "html.parser")
        links = soup.find_all('a', attrs={'class': 'a-link-normal s-underline-text s-underline-link-text s-link-style a-text-normal'})
        links_list = [link.get('href') for link in links]
        return links_list
    except requests.RequestException as e:
        logging.error(f"Request error for URL {url}: {e}")
        return []

def process_product_link(link, headers):
    try:
        HEADERS['User-Agent'] = random.choice(USER_AGENTS)
        full_link = 'https://www.amazon.com'+ link
        new_webpage = requests.get(full_link, headers=headers)
        new_soup = BeautifulSoup(new_webpage.content, "lxml")
        
        # Validate product information
        title = get_title(new_soup)
        brand = get_brand(new_soup)
        price = get_price(new_soup)
        discount = get_discount(new_soup)
        rating = get_rating(new_soup)
        reviews = get_review_count(new_soup)
        availability = get_availability(new_soup)
        detail = get_detail(new_soup)
        
        if brand != "":
            product_info = {
                "title": title,
                "brand": brand,
                "price": price,
                "discount": discount,
                "rating": rating,
                "reviews": reviews,
                "availability": availability,
                "detail": detail
            }
            return product_info
        else:
            logging.warning(f"Skipping product due to missing information: {full_link}")
            return None
        
    except requests.RequestException as e:
        logging.error(f"Request error for link {link}: {e}")
        return None
    

def main():
    HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36', 'Accept-Language': 'en-US, en;q=0.5'}
    products = [
    "gaming+headset",
    "electronics",
    "smartphones",
    "laptops",
    "tablets",
    "cameras",
    "audio+equipment",
    "fashion",
    "men's+clothing",
    "women's+clothing",
    "shoes",
    "accessories",
    "sportswear",
    ]

    batch_size = 20
    
    with ThreadPoolExecutor(max_workers=5) as executor:  # Adjust max_workers based on your system capabilities
        futures = []
        for product in products:
            futures.append(executor.submit(process_product, product, HEADERS, batch_size))

        for future in futures:
            future.result()  # Wait for each thread to complete

def process_product(product, headers, batch_size):
    batch = []
    for page in range(1, 7001):
        links_list = fetch_product_details(product, page, headers)
        for link in links_list:
            product_info = process_product_link(link, headers)
            random_delay()
            if product_info:
                print(product_info)
                batch.append(product_info)
                if len(batch) >= batch_size:
                    logging.info(f"Sending batch of {batch_size} to Kafka")
                    json_data = json.dumps(batch)
                    producer.send('amazon-products', key=product.encode('utf-8'), value=json_data.encode('utf-8'))
                    batch = []

    # Send any remaining products in the final batch
    if batch:
        logging.info(f"Sending final batch of {len(batch)} to Kafka")
        json_data = json.dumps(batch)
        producer.send('amazon-products', key=product.encode('utf-8'), value=json_data.encode('utf-8'))


def random_delay():
    sleep(random.uniform(1, 5))

if __name__ == '__main__':
    main()