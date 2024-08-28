from product_id import get_flipkart_product_ids , save_to_database
import asyncio
import pandas as pd
from playwright.async_api import async_playwright, TimeoutError
from datetime import datetime

async def take_screenshot(page, filename):
    await page.screenshot(path=filename)

async def scrape_flipkart_product(product_id):
    url = f"https://www.flipkart.com/item/p/{product_id}"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()

        try:
            await page.goto(url)
        except TimeoutError:
            print(f"Failed to load page for Product ID: {product_id}")
            await take_screenshot(page, f"screenshots/{product_id}_error_loading_page.png")
            await browser.close()
            return pd.DataFrame(), pd.DataFrame()

        product_info = {'product_id': product_id}

        try:
            product_info['title'] = (await page.title()).strip()[:255]if await page.title() else "Title not found"
            print(f"Title: {product_info['title']}")
        except Exception as e:
            product_info['title'] = f"Error: {str(e)}"
            await take_screenshot(page, f"screenshots/{product_id}_error_title.png")

        try:
            price_element = await page.query_selector('//div[@class="Nx9bqj CxhGGd"]')
            product_info['price'] = (await price_element.inner_text()).replace('\n', '').strip().replace('₹', '').replace(',', '') if price_element else "Price not found"
            print(f"Price: {product_info['price']}")
        except Exception as e:
            product_info['price'] = f"Error: {str(e)}"
            await take_screenshot(page, f"screenshots/{product_id}_error_price.png")

        try:
            rating_element = await page.query_selector('//div[@class="XQDdHH"]')
            rating_text = (await rating_element.inner_text()).replace('\n', '').strip() if rating_element else None
            product_info['rating'] = float(rating_text) if rating_text and rating_text.replace('.', '', 1).isdigit() else None
            print(f"Rating: {product_info['rating']}")
        except Exception as e:
            product_info['rating'] = None
            await take_screenshot(page, f"screenshots/{product_id}_error_rating.png")

        try:
            ratings_reviews_element = await page.query_selector('//span[@class="Wphh3N"]/span/span[1]')
            ratings_text = (await ratings_reviews_element.inner_text()).split(' Ratings')[0] if ratings_reviews_element else None
            product_info['number_of_ratings'] = int(''.join(filter(str.isdigit, ratings_text))) if ratings_text else None
            print(f"Number of Ratings: {product_info['number_of_ratings']}")
        except Exception as e:
            product_info['number_of_ratings'] = None
            await take_screenshot(page, f"screenshots/{product_id}_error_reviews_count.png")

        try:
            see_all_reviews_link = await page.query_selector('a[href*="product-reviews"]')
            if see_all_reviews_link:
                await see_all_reviews_link.click()
                await page.wait_for_timeout(1000)
            else:
                raise Exception("See all reviews link not found")
        except Exception as e:
            product_info['reviews_navigation_error'] = f"Error: {str(e)}"
            await take_screenshot(page, f"screenshots/{product_id}_error_navigation.png")

        try:
            sort_filter = await page.query_selector('select[name="sortFilter"]')
            if sort_filter:
                await page.select_option('select.OZuttk.JEZ5ey[name="sortFilter"]', 'MOST_RECENT')
                await page.wait_for_timeout(2000)
        except Exception as e:
            await take_screenshot(page, f"screenshots/{product_id}_error_sort_filter.png")
            print(f"An error occurred while setting the sort filter: {e}")

        product_info['scrape_date'] = datetime.now().strftime('%Y-%m-%d')

        base_url, page_no, reviews = page.url, 1, []
        while True:
            try:
                review_elements = await page.query_selector_all('//div[contains(@class, "col") and contains(@class, "EPCmJX") and contains(@class, "Ma1fCG")]')
                if not review_elements:
                    break
                for review_element in review_elements:
                    await review_element.click()
                    await page.wait_for_timeout(1000)
                    review = {}
                    try:
                        review_id_element = await review_element.query_selector('p[id]')
                        review['reviewid'] = await review_id_element.get_attribute('id') if review_id_element else "Review ID not found"
                    except Exception as e:
                        review['reviewid'] = f"Error: {str(e)}"
                        await take_screenshot(page, f"screenshots/{product_id}_error_review_id.png")


                    try:
                        name_element = await review_element.query_selector('//div[@class="row gHqwa8"]/div[@class="row"]/p[@class="_2NsDsF AwS1CA"]')
                        review['reviewer_name'] = (await name_element.inner_text()).replace('\n', '').strip()[:255] if name_element else "Name not found"
                    except Exception as e:
                        review['reviewer_name'] = f"Error: {str(e)}"
                        await take_screenshot(page, f"screenshots/{product_id}_error_review_name.png")

                    try:
                        rating_text = await (await review_element.query_selector('//div[@class="XQDdHH Ga3i8K"]')).inner_text()
                        review['rating'] = float(rating_text.strip()) if rating_text else None
                    except Exception as e:
                        review['rating'] = None
                        await take_screenshot(page, f"screenshots/{product_id}_error_review_rating.png")

                    try:
                        review_text_element = await review_element.query_selector('//div[@class="row"]/div[@class="ZmyHeo"]/div/div[@class=""]')
                        review['review'] = (await review_text_element.inner_text()).replace('\n', '').strip()[:700] if review_text_element else "Review text not found"
                    except Exception as e:
                        review['review'] = f"Error: {str(e)}"
                        await take_screenshot(page, f"screenshots/{product_id}_error_review_text.png")

                    review['product_id'] = product_id
                    review['scrape_date'] = datetime.now().strftime('%Y-%m-%d')
                    reviews.append(review)

                page_no += 1
                next_page_url = f"{base_url}&page={page_no}"
                await page.goto(next_page_url)
                # await page.wait_for_selector('//div[contains(@class, "col") and contains(@class, "EPCmJX") and contains(@class, "Ma1fCG")]')

            except Exception as e:
                await take_screenshot(page, f"screenshots/{product_id}_error_pagination.png")
                break

        await browser.close()

        product_info_df = pd.DataFrame([product_info])
        reviews_df = pd.DataFrame(reviews)
        return product_info_df, reviews_df

async def main():
    product_ids = get_flipkart_product_ids()

    all_products_df, all_reviews_df = pd.DataFrame(), pd.DataFrame()

    for product_id in product_ids:
        product_df, reviews_df = await scrape_flipkart_product(product_id)
        all_products_df = pd.concat([all_products_df, product_df], ignore_index=True)
        all_reviews_df = pd.concat([all_reviews_df, reviews_df], ignore_index=True)

    return all_products_df, all_reviews_df

if __name__ == "__main__":
    products_df, reviews_df = asyncio.run(main())
    save_to_database(products_df, reviews_df)