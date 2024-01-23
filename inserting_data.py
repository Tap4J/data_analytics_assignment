import json
import psycopg2

# Connect to PostgreSQL database
conn = psycopg2.connect(
    host='localhost',
    database='postgres',
    user='postgres',
    password='pivicko',
    port="5432"
)

cur = conn.cursor()

try:
    # Read the ndjson file and insert data into the database
    with open('dataset/data.ndjson', 'r') as file:
        for line in file:
            data = json.loads(line)

            # Extract data from the JSON structure
            order_id = data['id']
            created = data['created']
            user_id = data['user']['id']

            # Insert data into Customers table
            cur.execute("""
                INSERT INTO Customers (user_id, user_name, user_city)
                VALUES (%s, %s, %s)
                ON CONFLICT (user_id) DO NOTHING;
            """, (user_id, data['user']['name'], data['user']['city']))

            # Insert data into Products table and create a separate row for each product
            for i, product in enumerate(data['products']):
                product_id = product['id']
                cur.execute("""
                    INSERT INTO Products (product_id, product_name, product_price)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (product_id) DO NOTHING;
                """, (product_id, product['name'], product['price']))

                # Insert data into Orders table for each product
                cur.execute("""
                    INSERT INTO Orders (order_id, created, customer_id, product_id)
                    VALUES (%s, %s, %s, %s);
                """, (order_id, created, user_id, product_id))

    conn.commit()

except Exception as e:
    print(f"Error: {e}")

finally:
    cur.close()
    conn.close()
