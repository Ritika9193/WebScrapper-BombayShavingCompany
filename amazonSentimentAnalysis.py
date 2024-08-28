import asyncio
import pandas as pd
from playwright.async_api import async_playwright, TimeoutError
from datetime import datetime
import google.generativeai as genai
from Asin_SKU_table import get_platform_code, save_to_database

async def take_screenshot(page, filename):
    await page.screenshot(path=filename)

genai.configure(api_key="Api-Key")
model = genai.GenerativeModel("gemini-pro")

def get_gemini_response(prompt):
    response = model.generate_content(prompt)
    return response.text

async def analyze_sentiment_batch(reviews):
    if not reviews:
        print("No reviews to analyze.")
        return None, [], []

    # prompt = "\n".join([f"Review: {review}" for review in reviews])
    # prompt += """
    # Analyze the following reviews and provide the following:
    # 1. A concise summary (50-100 words) of the overall sentiment expressed in the reviews.
    # 2. positive qualities of this product that doesn't contradict the negative aspect but instead offers a different perspective
    # 3. negative qualities of this product that doesn't contradict the positive aspect but instead offers a different perspective
    # Note:
    # - The positive and negative aspects must be distinct and non-contradictory.
    # - Focus on specific features such as "durability," "performance," "customer service," etc., and avoid vague terms.
    # - If an aspect is mentioned positively and negatively, provide it in the context that makes the most sense (e.g., "durable" as positive and "too heavy" as negative).

    # Please ensure to provide both positive and negative keywords, even if one set is less common. Format the response as follows:
    # - Sentiment: <Sentiment summary>
    # - Top Positive Keywords: keyword1, keyword2, keyword3, keyword4, keyword5
    # - Top Negative Keywords: keyword1, keyword2, keyword3, keyword4, keyword5
    # Please provide the output as plain text without any special formatting or symbols.
    # """
    prompt = "\n".join([f"Review: {review}" for review in reviews])
    prompt += """
    Analyze the following reviews and provide the following:
    1. A concise summary (50-100 words) of the overall sentiment expressed in the reviews.
    2. Identify from the summary and list at most 5 words that reviewers frequently mention as things they like the most about the product, even if they are minor or rare.
    3. Identify from the summary and list at most 5 words that reviewers frequently mention as things they dislike the most about the product, even if they are minor or rare.
    Please ensure to provide both positive and negative keywords which are non-contradictory, even if one set is less common. Format the response as follows:
    - Sentiment: <Sentiment summary>
    - Top Positive Keywords: keyword1, keyword2, keyword3, keyword4, keyword5
    - Top Negative Keywords: keyword1, keyword2, keyword3, keyword4, keyword5
    Please provide the output as plain text without any special formatting or symbols.
    """

    try:
        response_text = get_gemini_response(prompt)
        if response_text:
            sentiment = None
            top_positive_keywords = []
            top_negative_keywords= []

            for line in response_text.strip().split('\n'):
                if "Sentiment:" in line:
                    sentiment = line.split(':')[-1].strip()
                elif "Top Positive Keywords:" in line:
                    top_positive_keywords = [kw.strip() for kw in line.split(':')[-1].strip().split(',')]
                elif "Top Negative Keywords:" in line:
                    top_negative_keywords = [kw.strip() for kw in line.split(':')[-1].strip().split(',')]
            
            if not top_positive_keywords or top_positive_keywords == ['']:
                print("No positive keywords found in the response.")
                top_positive_keywords = ["No significant positive keywords found"]

            if not top_negative_keywords or top_negative_keywords == ['']:
                print("No negative keywords found in the response.")
                top_negative_keywords = ["No significant negative keywords found"]

            return sentiment, top_positive_keywords, top_negative_keywords
        else:
            print("Empty response from Gemini.")
            return None, []
    except Exception as e:
        print(f"Error during sentiment analysis: {str(e)}")
        return None, []

async def scrape_amazon_product(asin):
    url = f"https://www.amazon.in/dp/{asin}"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        try:
            await page.goto(url, timeout=30000)
        except TimeoutError:
            print(f"TimeoutError: Failed to load page for ASIN: {asin}")
            await take_screenshot(page, f"screenshots/{asin}_error_loading_page.png")
            await browser.close()
            return pd.DataFrame(), pd.DataFrame(), []

        product_info = {'ASIN': asin}

        try:
            product_info['title'] = (await page.title()).strip()[:255]
            print(f"Title: {product_info['title']}")
        except Exception as e:
            product_info['title'] = f"Error: {str(e)}"
            await take_screenshot(page, f"screenshots/{asin}_error_title.png")

        try:
            price_whole_element = await page.query_selector('span.a-price-whole')
            price_fraction_element = await page.query_selector('span.a-price-fraction')
            if price_whole_element:
                price_whole = (await price_whole_element.inner_text()).replace('\n', '').strip()
                price_fraction = (await price_fraction_element.inner_text()).replace('\n', '').strip() if price_fraction_element else "00"
                product_info['price'] = f"{price_whole}.{price_fraction}".replace('..', '.')
                product_info['price'] = product_info['price'].replace(',', '')  
                print(f"Price: {product_info['price']}")
            else:
                product_info['price'] = "Price not found"
        except Exception as e:
            product_info['price'] = f"Error: {str(e)}"
            await take_screenshot(page, f"screenshots/{asin}_error_price.png")

        try:
            rating_text = (await (await page.query_selector('span.a-icon-alt')).inner_text()).strip()
            product_info['rating'] = rating_text.split(' ')[0] if rating_text else "Rating not found"
            print(f"Rating: {product_info['rating']}")
        except Exception as e:
            product_info['rating'] = f"Error: {str(e)}"
            await take_screenshot(page, f"screenshots/{asin}_error_rating.png")

        try:
            reviews_count_text = (await (await page.query_selector('span#acrCustomerReviewText')).inner_text()).replace('\n', '').strip()
            product_info['NumberOfRatings'] = ''.join(filter(str.isdigit, reviews_count_text)) if reviews_count_text else "Reviews count not found"
            print(f"Number of Ratings: {product_info['NumberOfRatings']}")
        except Exception as e:
            product_info['NumberOfRatings'] = f"Error: {str(e)}"
            await take_screenshot(page, f"screenshots/{asin}_error_reviews_count.png")

        try:
            see_all_reviews_link = await page.query_selector('a[data-hook="see-all-reviews-link-foot"]')
            if see_all_reviews_link:
                await see_all_reviews_link.click()
                await page.select_option('select#sort-order-dropdown', 'recent')
                await page.wait_for_timeout(2000)
            else:
                raise Exception("See all reviews link not found")
        except Exception as e:
            product_info['reviews_navigation_error'] = f"Error: {str(e)}"
            await take_screenshot(page, f"screenshots/{asin}_error_navigation.png")  

        product_info['scrape_date'] = datetime.now().strftime('%Y-%m-%d')

        reviews = []
        review_texts = []
        while True:
            try:
                review_elements = await page.query_selector_all('div[data-hook="review"]')
                for review_element in review_elements:
                    review = {}
                    try:
                        review['review_id'] = await review_element.get_attribute('id')
                    except Exception as e:
                        review['review_id'] = f"Error: {str(e)}"
                        await take_screenshot(page, f"screenshots/{asin}_error_review_id.png")

                    try:
                        review['name'] = (await (await review_element.query_selector('span.a-profile-name')).inner_text()).strip()[:255]
                    except Exception as e:
                        review['name'] = f"Error: {str(e)}"
                        await take_screenshot(page, f"screenshots/{asin}_error_reviewer_name.png")

                    try:
                        review_text = (await (await review_element.query_selector('i[data-hook="review-star-rating"] span.a-icon-alt')).inner_text()).strip()
                        review['rating'] = review_text.split(' ')[0]
                    except Exception as e:
                        review['rating'] = f"Error: {str(e)}"
                        await take_screenshot(page, f"screenshots/{asin}_error_review_rating.png")

                    # try:
                    #     review_text = (await (await review_element.query_selector('span[data-hook="review-body"] span')).inner_text()).strip()
                    #     review['review'] = review_text
                    #     review_texts.append(review_text)
                    #     # print(f"Review: {review['review']}")
                    # except Exception as e:
                    #     print(f"Error retrieving review text: {str(e)}")
                    #     await take_screenshot(page, f"screenshots/{asin}_error_review_text.png")

                    try:
                        review_body_element = await review_element.query_selector('span[data-hook="review-body"] span')
                        if review_body_element:
                            review_text = (await review_body_element.inner_text()).strip()
                            review['review'] = review_text
                            review_texts.append(review_text)
                        else:
                            review['review'] = "Review text not found"
                            await take_screenshot(page, f"screenshots/{asin}_error_review_text_missing.png")
                    except Exception as e:
                        review['review'] = f"Error: {str(e)}"
                        print(f"Error retrieving review text: {str(e)}")
                        await take_screenshot(page, f"screenshots/{asin}_error_review_text.png")


                    review['ASIN'] = asin
                    review['scrape_date'] = datetime.now().strftime('%Y-%m-%d')

                    reviews.append(review)

                next_page_link = await page.query_selector('li.a-last a')
                if not next_page_link:
                    break
                await next_page_link.click()
                await page.wait_for_timeout(2000)

            except Exception as e:
                print(f"Error while scraping reviews for ASIN: {asin}. Error: {str(e)}")
                await take_screenshot(page, f"screenshots/{asin}_error_scraping_reviews.png")
                break

        await browser.close()

    product_info_df = pd.DataFrame([product_info])
    reviews_df = pd.DataFrame(reviews)
    return product_info_df, reviews_df, review_texts

async def main():
    asins = get_platform_code()
    # asins = ['B098P8NRRW']
    all_products_df = pd.DataFrame()
    all_reviews_df = pd.DataFrame()

    for asin in asins:
        product_df, reviews_df, review_texts = await scrape_amazon_product(asin)

        if not review_texts:
            print(f"No reviews found for ASIN: {asin}")
            continue

        print("Analyzing sentiment for the scraped reviews...")
        overall_sentiment,top_positive_keywords,top_negative_keywords = await analyze_sentiment_batch(review_texts)
        print(f"Overall Sentiment: {overall_sentiment}")
        print(f"Top Positive Keywords: {top_positive_keywords}")
        print(f"Top Negative Keywords: {top_negative_keywords}")

        all_products_df = pd.concat([all_products_df, product_df], ignore_index=True)
        all_reviews_df = pd.concat([all_reviews_df, reviews_df], ignore_index=True)

    return all_products_df, all_reviews_df

if __name__ == "__main__":
    products_df, reviews_df = asyncio.run(main())
