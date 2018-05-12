
# coding: utf-8
import io, time, json
import requests
import re
from bs4 import BeautifulSoup
import warnings
import pandas as pd
import sys
warnings.filterwarnings('ignore')

website = 'https://www.amazon.com'


def retrieve_html(url):
    """
    Return the raw HTML at the specified URL.

    Args:
        url (string): 

    Returns:
        status_code (integer):
        raw_html (string): the raw HTML content of the response, properly encoded according to the HTTP headers.
    """
    
    # Write solution here
    headers = headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36'}
    r = requests.get(url, headers = headers, verify = False)
    return (r.status_code, r.text)
    pass


def scrape_bookName_id(url):
    if 'ref=' in url:
        url = url[:url.rfind('/ref')]
    code, html_book = retrieve_html(url)
    if(code != 200):
        return {}
    data = {}
    data['url'] = url
    try:
        book_soup  = BeautifulSoup(html_book, "html.parser")
        prod_title = book_soup.find('span',id = "productTitle")
        if prod_title is not None:
            data['name'] = prod_title.get_text()
        else:
            print('\nProduct Title not found')
        author = book_soup.find('a',{'class' : "contributorNameID"})
        if author is not None:
            data['author'] = author.get_text()
        else:
            print('\nAuthor Name not found. Trying Plan B for author Extraction.')
            author_spans = book_soup.find_all('span',{'class' : "author"})
            if author_spans is not None and len(author_spans) >= 1:
                auth_list = []
                for author_span in author_spans:
                    author = author_span.find('a', {'class' : 'a-link-normal'})
                    auth_list.append(author.get_text())
                data['author'] = ','.join(auth_list)
                print('Plan B: Successful')
            else:
                print('Still no luck! Giving up :( on Author name extraction.')
        image_block = book_soup.find('img',{'class' : "frontImage"})
        if image_block is not None:
            data['image_url'] =list(json.loads(image_block.get('data-a-dynamic-image')).keys())[0]
        else:
            print('\nImage Not Found')
        review = book_soup.find('a',{'data-hook' : "see-all-reviews-link-foot"})
        if review is not None:
            data['review_url'] = website + review.get('href')
        else:
            print('\nNo Reviews found')

        prod_info_pre = book_soup.find('td', {'class' : 'bucket'})
        if prod_info_pre is not None:
            product_info_block = book_soup.find('td', {'class' : 'bucket'}).find('ul')
            info_list = product_info_block.find_all('li')
            for info in info_list:
                text = info.get_text().strip()
                if text.startswith('ISBN-10:'):
                    data['ISBN-10'] = text.replace('ISBN-10:','').strip().upper()
                if text.startswith('ISBN-13:'):
                    data['ISBN-13'] = text.replace('ISBN-13:','').strip().upper()
                if text.startswith('Language:'):
                    data['Language'] = text.replace('Language:','').strip().lower()
        else:
            raise Exception('\nNo Product info block found')
        return data
    except Exception as e:
        print('Except block hit, error: :',e)
        return data

if __name__ == "__main__":
    in_file = sys.argv[1]
    out_file = sys.argv[2]
    log_file = sys.argv[3]

    sys.stdout = open(log_file, "w")

    urls = pd.read_csv(in_file).ix[:,0]

    book_info = []
    for i, url in enumerate(urls):
        print(i,"---URL: ", url)
        info = scrape_bookName_id(url)
        if info is not {}:
            book_info.append(info)
        else:
            print("Stopped at url number: ",i,"\n url was: ", url)
        if i%50 == 0:
            pd.DataFrame(book_info).to_csv('temp_out.csv', index=False)
            time.sleep(10)
        time.sleep(2)
    pd.DataFrame(book_info).to_csv(out_file, index=False)
    # print(scrape_bookName_id(urls[0]))