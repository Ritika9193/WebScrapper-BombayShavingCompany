
import psycopg2
import pandas as pd
def insert_flipkart_product_id(product_id):
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
            INSERT INTO Flipkart.product_ids (product_id)
            VALUES (%s)
            ON CONFLICT (product_id) DO NOTHING;
        """, (product_id,))
        conn.commit()

    except Exception as e:
        print(f"Error inserting Flipkart product ID: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_flipkart_product_ids():
    conn = None
    cursor = None
    product_ids = []

    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="Ritika@22",
            host="localhost",
            port=5432
        )
        cursor = conn.cursor()

        cursor.execute("SELECT product_id FROM Flipkart.product_ids;")
        product_ids = [row[0] for row in cursor.fetchall()]

    except Exception as e:
        print(f"Error retrieving Flipkart product IDs: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return product_ids

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

        # Insert into products table
        for _, row in df_products.iterrows():
            cursor.execute("""
                INSERT INTO Flipkart.products (scrape_date, product_id, title, price, rating, number_of_ratings)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                row['scrape_date'],
                row['product_id'],
                row['title'],
                row['price'],
                row['rating'],
                row['number_of_ratings']
            ))

        # Insert into reviews table
        for _, row in df_reviews.iterrows():
            cursor.execute("""
                INSERT INTO Flipkart.reviews (scrape_date, product_id, reviewid , reviewer_name, rating, review)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (reviewid)
                    DO UPDATE SET
                        rating = EXCLUDED.rating,
                        scrape_date = EXCLUDED.scrape_date,
                        product_id=EXCLUDED.product_id,
                        reviewer_name = EXCLUDED.reviewer_name,
                        review = EXCLUDED.review
                           
            """, (
                row['scrape_date'],
                row['product_id'],
                row['reviewid'],
                row['reviewer_name'],
                row['rating'],
                row['review']
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
        product_id = input("Enter Flipkart product ID (or 'done' to finish): ")
        if product_id.lower() == 'done':
            break
        insert_flipkart_product_id(product_id)




# import psycopg2
# import pandas as pd

# def connect_to_db():
#     try:
#         conn = psycopg2.connect(
#             dbname="postgres",
#             user="postgres",
#             password="Ritika@22",
#             host="localhost",
#             port=5432
#         )
#         return conn
#     except Exception as e:
#         print(f"Error connecting to the database: {str(e)}")
#         return None

# def get_flipkart_product_ids():
#     conn = None
#     cursor = None
#     product_ids = []

#     try:
#         conn = connect_to_db()
#         if conn is None:
#             return product_ids

#         cursor = conn.cursor()
#         cursor.execute("SELECT platformcode FROM Flipkart.productmaster LIMIT 1;")
#         product_ids = [row[0] for row in cursor.fetchall()]

#     except Exception as e:
#         print(f"Error retrieving Flipkart product IDs: {str(e)}")
#     finally:
#         if cursor:
#             cursor.close()
#         if conn:
#             conn.close()

#     return product_ids

# def save_to_database(df_products: pd.DataFrame, df_reviews: pd.DataFrame):
#     conn = None
#     cursor = None
#     try:
#         conn = connect_to_db()
#         if conn is None:
#             return

#         cursor = conn.cursor()

#         # Insert into products table
#         for _, row in df_products.iterrows():
#             cursor.execute("""
#                 INSERT INTO Flipkart.products (scrape_date, product_id, title, price, rating, number_of_ratings)
#                 VALUES (%s, %s, %s, %s, %s, %s)
#             """, (
#                 row['scrape_date'],
#                 row['product_id'],
#                 row['title'],
#                 row['price'],
#                 row['rating'],
#                 row['number_of_ratings']
#             ))

#         # Insert into reviews table
#         for _, row in df_reviews.iterrows():
#             cursor.execute("""
#                 INSERT INTO Flipkart.reviews (scrape_date, product_id, reviewid , reviewer_name, rating, review)
#                     VALUES (%s, %s, %s, %s, %s, %s)
#                     ON CONFLICT (reviewid)
#                     DO UPDATE SET
#                         rating = EXCLUDED.rating,
#                         scrape_date = EXCLUDED.scrape_date,
#                         product_id=EXCLUDED.product_id,
#                         reviewer_name = EXCLUDED.reviewer_name,
#                         review = EXCLUDED.review
                           
#             """, (
#                 row['scrape_date'],
#                 row['product_id'],
#                 row['reviewid'],
#                 row['reviewer_name'],
#                 row['rating'],
#                 row['review']
#             ))

#         conn.commit()
#         print("Data inserted successfully.")

#     except Exception as e:
#         print(f"Error: {str(e)}")
#     finally:
#         if cursor:
#             cursor.close()
#         if conn:
#             conn.close()

# if __name__ == "__main__":
#     pass