import requests
from bs4 import BeautifulSoup
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import time

# Base URL of the website
base_url = 'https://library.stikom-bali.ac.id/index.php?keywords=&search=search'

# Headers to mimic a real browser visit
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36'
}

# Function to scrape the total number of pages
def get_total_pages():
    response = requests.get(base_url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    last_page_link = soup.find('a', class_='last_link')
    if last_page_link:
        total_pages = int(re.search(r'page=(\d+)', last_page_link['href']).group(1))
    else:
        total_pages = 1

    return total_pages

# Function to scrape data from a single page, inserting empty strings for missing data
def scrape_page(page_num, retries=3):
    url = f'{base_url}&page={page_num}'
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            books = []
            for item in soup.find_all('div', class_='item biblioRecord'):
                # Extract title, author, and detail URL, using empty strings if missing
                title_tag = item.find('a', class_='titleField')
                author_tag = item.find('span', class_='author-name')
                
                title = title_tag.text.strip() if title_tag else ""
                author = author_tag.text.strip() if author_tag else ""
                detail_url = 'https://library.stikom-bali.ac.id' + title_tag['href'] if title_tag else ""

                books.append([title, author, detail_url])

            time.sleep(1)
            return books
        except requests.exceptions.RequestException as e:
            print(f"Error on page {page_num} (attempt {attempt + 1}): {e}")
            time.sleep(2)
            if attempt == retries - 1:
                print(f"Failed to retrieve page {page_num} after {retries} attempts.")
                return []

# Function to save data incrementally to CSV
def save_to_csv(data, filename='books.csv'):
    with open(filename, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerows(data)
    print(f'Saved {len(data)} records to {filename}')

# Function to handle scraping across multiple pages with threading and batch saving
def scrape_multiple_pages():
    all_books = []
    total_pages = get_total_pages()
    print(f"Total pages to scrape: {total_pages}")

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(scrape_page, page): page for page in range(1, total_pages + 1)}
        
        batch = []
        for future in as_completed(futures):
            page_num = futures[future]
            try:
                books_on_page = future.result()
                batch.extend(books_on_page)
                print(f"Scraped page {page_num}")

                if len(batch) >= 500:
                    save_to_csv(batch)
                    batch = []
            except Exception as e:
                print(f"Error processing page {page_num}: {e}")

        if batch:
            save_to_csv(batch)

# Set up CSV file with header
with open('books.csv', mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(['Title', 'Author', 'Detail URL'])

# Run the scraping process
scrape_multiple_pages()
print('Scraping completed and data saved to books.csv')
