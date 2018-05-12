import io, time, json
import requests
from bs4 import BeautifulSoup
import warnings
import pandas as pd
warnings.filterwarnings('ignore')
import sys

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
    headers = headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36'}
    r = requests.get(url, headers=headers, verify=False)
    return (r.status_code, r.text)
    pass


def extract_reviews(url):
    reviews = []
    code, html_rev = retrieve_html(url)
    if (code != 200):
        print('Erorrrr:----->  Status Code: ', code)
        return []

    with open('htmlfile.html', 'wb') as htmlfile:
        htmlfile.write(html_rev.encode('utf-8'))

    review_soup = BeautifulSoup(html_rev, "html.parser")

    title = review_soup.find('title')
    if title is not None:
        print(title.get_text())
        if 'Robot Check' in title.get_text():
            raise ValueError('Robo Check hit. Stopping here')

    review_div = review_soup.find('div', {'id': 'cm_cr-review_list'})
    review_list = []
    if review_div is not None:
        review_list = review_div.find_all('div', {'data-hook': 'review'})
    for review in review_list:
        rev_data = {}
        rev_data['rating'] = float(review.find('i', {'data-hook': 'review-star-rating'}).span.get_text()[:3])
        user_details = review.find('a', {'data-hook': 'review-author'})
        if user_details is None:
            continue

        rev_data['user_name'] = user_details.get_text()
        rev_data['user_link'] = user_details.get('href')
        if 'ref=' in rev_data['user_link']:
            rev_data['user_link'] = rev_data['user_link'][:rev_data['user_link'].rfind('/ref')]
        reviews.append(rev_data)
    #     print(reviews)
    #     page_info = review_soup.find('ul')
    page_info = review_soup.find('ul', {'class': 'a-pagination'})

    next_page = None
    try:
        if (page_info is not None):
            next_page = website + page_info.find('li', {'class': 'a-last'}).a.get('href')
            print('next page is ', next_page)
    except:
        print('Could not find next page')
    sys.stdout.flush()
    return (reviews, next_page)

# test = 'http://www.amazon.com/Outsiders-S-Hinton/product-reviews/014240733X'
# url = 'https://www.amazon.com/That-Was-Then-This-Now/product-reviews/0140389660'
def extract_all_reviews_for_one(url, book_id):
    print('**************************************************************************************************')
    print('extracting all reviews for ',url,' ',book_id)
    try:
        if 'ref=' in url:
            url = url[:url.rfind('/ref')]
        all_reviews = []
        next_link = url
        page = 1
        while next_link is not None:
            print("Review page: ", page)
            page += 1
            rev, next_link = extract_reviews(next_link)
            print('## next link : ', next_link)
            all_reviews.extend(rev)
            time.sleep(2)
        review_pd = pd.DataFrame(all_reviews)
        review_pd['book_id'] = book_id
        return review_pd
    except Exception as e:
        print(e.__class__.__name__)
        if e.__class__.__name__ == 'ValueError':
            raise e
        return pd.DataFrame()


if __name__ == "__main__":
    in_file = sys.argv[1]
    out_file = sys.argv[2]
    log_file = sys.argv[3]

    sys.stdout = open(log_file, "w")
    df = pd.read_csv(in_file)
    df = df[170:200]  # chunks
    book_info = []
    try:
        for i, row in df.iterrows():  # enumerate(urls[:2]):
            print(i, "---URL: ", row['review_url'])
            info = extract_all_reviews_for_one(row['review_url'], row['ISBN.10'])
            if info is not {}:
                book_info.append(info)
            else:
                print("Stopped at url number: ", i, "\n url was: ", row['review_url'])
            pd.concat(book_info).to_csv('temp_out.csv', index=False)
            time.sleep(10)
    except Exception as e:
        pd.concat(book_info).to_csv(out_file, index=False)
        raise e