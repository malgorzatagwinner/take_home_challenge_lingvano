import psycopg2

conn = psycopg2.connect("host='localhost' port='5432' dbname='mydb' user='myuser' password='mysecretpassword'")
cur = conn.cursor()

#delete duplicates
duplicates = """DELETE from customer_transactions WHERE "Order ID" IN (SELECT "Order ID" FROM customer_transactions GROUP BY "Order ID" HAVING COUNT("Order ID") > 1)"""
cur.execute(duplicates)

#delete other products
other_products = """DELETE from customer_transactions WHERE "Order Type" = 'product'"""
cur.execute(other_products)

#delete nulls
null_users = """ DELETE from customer_transactions WHERE "User Subscription ID" IS NULL """
cur.execute(null_users)

#delete test coupons"
test_coupons = """
DELETE from customer_transactions WHERE "Coupon Applied" LIKE '%TEST%'
"""
cur.execute(test_coupons)

columns_int = [
        "Customer age (at first purchase)",
        "Quantity",
]

columns_double = [
        "Total",
        "Tax",
        "Fee",
        "Balance Earnings",
]



for c in columns_int:
    cast = f""" ALTER TABLE customer_transactions ALTER COLUMN "{c}" TYPE INT USING "{c}"::integer """
    cur.execute(cast)


for c in columns_double:
    cast = f""" ALTER TABLE customer_transactions ALTER COLUMN "{c}" TYPE float8 USING "{c}"::float8 """
    cur.execute(cast)



conn.commit()
conn.close()

