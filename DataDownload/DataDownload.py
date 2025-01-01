import requests
import json
import os
import sys
import pandas   as pd
import numpy    as np
from datetime import datetime
import psycopg2 as pg
from psycopg2 import sql, extras
from tools.deribit import get_option_order_book
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Access the environment variables
dbname = os.getenv('DB_NAME')
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT')

def insert_data(df):
    try:
        db_params = {
            'dbname': dbname,
            'user': user,
            'password': password,
            'host': host,
            'port': port
        }
        # Connect to PostgreSQL
        conn = pg.connect(**db_params)
        cur = conn.cursor()
    
        # Define the insert statement
        insert_query = sql.SQL("""
            INSERT INTO market_data (
                mid_price, volume_usd, estimated_delivery_price, quote_currency, 
                creation_timestamp, base_currency, underlying_index, underlying_price, 
                mark_iv, volume, interest_rate, price_change, open_interest, ask_price, 
                bid_price, instrument_name, mark_price, last, low, high
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """)
    
        # Insert data row by row
        for _, row in df.iterrows():
            try:
                cur.execute(insert_query, (
                    row['mid_price'], row['volume_usd'], row['estimated_delivery_price'], row['quote_currency'], 
                    row['creation_timestamp'], row['base_currency'], row['underlying_index'], row['underlying_price'], 
                    row['mark_iv'], row['volume'], row['interest_rate'], row['price_change'], row['open_interest'], 
                    row['ask_price'], row['bid_price'], row['instrument_name'], row['mark_price'], row['last'], 
                    row['low'], row['high']
                ))
            except Exception as e:
                print(f"Error inserting row {row.name}: {e}")
    
        # Commit changes
        conn.commit()
    
    except pg.Error as db_err:
        print(f"Database error: {db_err}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close cursor and connection
        if 'cur' in locals() and cur is not None:
            cur.close()
        if 'conn' in locals() and conn is not None:
            conn.close()
        print('Insert done')

def main(arg):
    asset = arg
    df = pd.DataFrame(get_option_order_book(asset=asset))
    df['creation_timestamp'] = datetime.now()
    insert_data(df)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <argument>")
    else:
        argument = sys.argv[1]
        main(argument)