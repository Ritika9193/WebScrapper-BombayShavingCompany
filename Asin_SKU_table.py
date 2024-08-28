import psycopg2
import pandas as pd
def insert_data(ip_code, whsku, platform_code):
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="Ritika@22",  
            host="localhost",
            port=5432
        )
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO Amazon.Info  (IP_Code, WHSKU, Platform_Code)
            VALUES (%s, %s, %s)
        """, (ip_code, whsku, platform_code))

        conn.commit()
        print("Data inserted successfully.")

    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def get_platform_code():
    conn = None
    cursor = None
    platform_codes = []

    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="Ritika@22",
            host="localhost",
            port=5432
        )
        cursor = conn.cursor()

        cursor.execute("SELECT DISTINCT Platform_Code FROM Amazon.Info;")
        platform_codes = [row[0] for row in cursor.fetchall()]

    except Exception as e:
        print(f"Error retrieving Platform Codes: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return platform_codes

def save_to_database(df_products: pd.DataFrame, df_reviews: pd.DataFrame):
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="Ritika@22",
            host="localhost",
            port=5432
        )
        cursor = conn.cursor()

        for _, row in df_products.iterrows():
            cursor.execute("""
                INSERT INTO Amazon.products (scrape_date, asin, title, price, rating, number_of_ratings)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                row['scrape_date'],
                row['ASIN'],
                row['title'],
                row['price'],
                row['rating'],
                row['NumberOfRatings']
            ))

        for _, row in df_reviews.iterrows():
            cursor.execute("""
                INSERT INTO Amazon.reviews (scrape_date, asin, reviewer_name, rating, review_id, review, review_date)
                VALUES (%s,%s, %s, %s, %s, %s, %s)
                ON CONFLICT (review_id) DO UPDATE SET
                    scrape_date = EXCLUDED.scrape_date,
                    asin = EXCLUDED.asin,
                    reviewer_name = EXCLUDED.reviewer_name,
                    rating = EXCLUDED.rating,
                    review = EXCLUDED.review,
                    review_date = EXCLUDED.review_date;
            """, (
                row['scrape_date'],
                row['ASIN'],
                row['name'],
                row['rating'],
                row['review_id'],
                row['review'],
                row['date'] 
            ))

        conn.commit()
        print("Data inserted successfully.")

    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":

    while True:
        try:
            ip_code = float(input("Enter IP Code (numeric): "))
            whsku = input("Enter WHSKU (text): ")
            platform_code = input("Enter Platform Code (text): ")

            insert_data(ip_code, whsku, platform_code)

            more_data = input("Do you want to insert more data? (y/n): ").strip().lower()
            if more_data != 'y':
                break

        except ValueError as e:
            print(f"Invalid input: {e}. Please try again.")

    print("Exiting the script.")



