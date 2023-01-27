import sqlite3
conn = sqlite3.connect('lrnyc.sqlite')

#CREATE TABLE
conn.execute('''CREATE TABLE COMPANY
         (ID INT PRIMARY KEY     NOT NULL,
         NAME           TEXT    NOT NULL,
         AGE            INT     NOT NULL,
         ADDRESS        CHAR(50),
         SALARY         REAL);''')
# print("Table created successfully")
#
# conn.close()

#RECHARGE CHARGE HOLDER
# conn.execute('''CREATE TABLE CUST_CHARGES
#          (CUSTOMER_ID INT  NOT NULL,
#          CHARGE    TEXT    NOT NULL);''')
# print("Table created successfully")
#
# conn.close()
id = 1
charge = "CHArGES"
cmd = f"""INSERT INTO CUST_CHARGES (CUSTOMER_ID,CHARGE) VALUES (?, ?)"""
#ADD DATA TO TABLE
#cur = conn.execute("""INSERT INTO CUST_CHARGES (CUSTOMER_ID,CHARGE) VALUES (?, ?)""",[id,charge])
# conn.execute("INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) \
#       VALUES (1, 'Paul', 32, 'California', 20000.00 )")
#
# conn.execute("INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) \
#       VALUES (2, 'Allen', 25, 'Texas', 15000.00 )")
#
# conn.execute("INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) \
#       VALUES (3, 'Teddy', 23, 'Norway', 20000.00 )")
#
# conn.execute("INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) \
#       VALUES (4, 'Mark', 25, 'Rich-Mond ', 65000.00 )")
#
#conn.commit()
# print("Records created successfully")
#conn.close()

#GET DATA FROM TABLE
# cursor = conn.execute("SELECT id, name, address, salary from COMPANY")
# for row in cursor:
#    print("ID = ", row[0])
#    print("NAME = ", row[1])
#    print("ADDRESS = ", row[2])
#    print("SALARY = ", row[3], "\n")
#
# print("Operation done successfully")
# conn.close()

#UPDATE DATA IN TABLE
# conn.execute("UPDATE COMPANY set SALARY = 50000.00 where SALARY < 25000")
# conn.commit()
# print("Total number of rows updated :", conn.total_changes)
#
# cursor = conn.execute("SELECT id, name, address, salary from COMPANY")
# for row in cursor:
#    print("ID = ", row[0])
#    print("NAME = ", row[1])
#    print("ADDRESS = ", row[2])
#    print("SALARY = ", row[3], "\n")
#
# print("Operation done successfully")
# conn.close()

#DELETE RECORD FROM TABLE
#conn.execute("DELETE from COMPANY where ID = 2;")
# conn.execute("DELETE from CUST_CHARGES")
# conn.commit()
# print("Total number of rows deleted :", conn.total_changes)
#
# cursor = conn.execute("SELECT * from CUST_CHARGES")
# for row in cursor:
#     print(row)
   # print("ID = ", row[0])
   # print("NAME = ", row[1])
   # print("ADDRESS = ", row[2])
   # print("SALARY = ", row[3], "\n")

# print("Operation done successfully")
# conn.close()

from pprint import pprint

tim = {'address_id': 94661934, 'analytics_data': {'utm_params': []}, 'billing_address': {'address1': '128 India St', 'address2': 'Apt 4', 'city': 'Brooklyn', 'company': None, 'country': 'United States', 'first_name': 'Alberto', 'last_name': 'Lalama', 'phone': '6507760485', 'province': 'New York', 'zip': '11222'}, 'client_details': {'browser_ip': None, 'user_agent': None}, 'created_at': '2022-05-27T00:39:40', 'currency': 'USD', 'customer_hash': 'b31cdc07919d33f56ee50cc556b9e0', 'customer_id': 87171804, 'discount_codes': [], 'email': 'alalama92@gmail.com', 'first_name': 'Alberto', 'has_uncommited_changes': False, 'id': 588865961, 'last_name': 'Lalama', 'line_items': [{'grams': 0, 'images': {'large': 'https://cdn.shopify.com/s/files/1/1812/9769/products/IMG_6590_large.jpg', 'medium': 'https://cdn.shopify.com/s/files/1/1812/9769/products/IMG_6590_medium.jpg', 'original': 'https://cdn.shopify.com/s/files/1/1812/9769/products/IMG_6590.jpg', 'small': 'https://cdn.shopify.com/s/files/1/1812/9769/products/IMG_6590_small.jpg'}, 'price': '59.35', 'properties': [{'name': '_ZapietId', 'value': 'M=P&L=10404'}, {'name': 'charge_interval_frequency', 'value': 1}, {'name': 'shipping_interval_frequency', 'value': 1}, {'name': 'shipping_interval_unit_type', 'value': 'week'}], 'quantity': 1, 'shopify_product_id': '6539427151926', 'shopify_variant_id': '39266065186870', 'sku': 'Salad Mix,Mushrooms,Fruit,Vegetables', 'subscription_id': 244029566, 'tax_lines': [], 'title': 'The Veggie Bundle Auto renew', 'type': 'SUBSCRIPTION', 'variant_title': 'Mushrooms / None', 'vendor': 'Local Roots NYC'}], 'note': '', 'note_attributes': [{'name': 'Checkout-Method', 'value': 'pickup'}, {'name': 'Pickup-Location-Id', 'value': '10404'}, {'name': 'Custom-Attribute-1', 'value': 'Tuesday, 5pm - 7pm'}, {'name': 'Pickup-Location-Company', 'value': 'Greenpoint - Threes Brewing'}, {'name': 'Pickup-Location-Address-Line-1', 'value': '113 Franklin St'}, {'name': 'Pickup-Location-City', 'value': 'Brooklyn'}, {'name': 'Pickup-Location-Region', 'value': 'New York'}, {'name': 'Pickup-Location-Postal-Code', 'value': '11222'}, {'name': 'Pickup-Location-Country', 'value': 'United States'}], 'processor_name': 'stripe', 'scheduled_at': '2022-06-03T00:00:00', 'shipments_count': None, 'shipping_address': {'address1': '113 Franklin St', 'address2': None, 'city': 'Brooklyn', 'company': 'Greenpoint - Threes Brewing', 'country': 'United States', 'first_name': 'Alberto', 'last_name': 'Lalama', 'phone': '(650) 776-0485', 'province': 'New York', 'zip': '11222'}, 'shipping_lines': [{'code': 'flat-rate', 'description': None, 'price': '0.00', 'source': '', 'tax_lines': [], 'title': 'Market Pick-Up (Threes Brewing)'}], 'shopify_order_id': None, 'status': 'QUEUED', 'sub_total': None, 'subtotal_price': '59.35', 'tags': 'Subscription, Subscription Recurring Order', 'tax_lines': 0.0, 'total_discounts': '0.00', 'total_line_items_price': '59.35', 'total_price': '59.35', 'total_refunds': None, 'total_tax': 0.0, 'total_weight': 0, 'transaction_id': None, 'type': 'RECURRING', 'updated_at': '2022-05-27T13:16:33'}

pprint(tim)
import pendulum
print(pendulum.now().subtract(months=1).to_date_string())

import sqlite3

conn = sqlite3.connect('lrnyc.sqlite')

# get all recharge charges between 7-18-21 and today
#all_recharge_charges = get_all_recharge_charges_thread()
# for charge in all_recharge_charges:
#     print(charge[0], charge[1])
#     id = int(charge[0])
#     charge = json.dumps(charge[1])
# cur = conn.execute("""INSERT INTO CUST_CHARGES (CUSTOMER_ID,CHARGE) VALUES (?, ?)""", [id, charge])

# cursor = conn.execute("SELECT CUSTOMER_ID,CHARGE from CUST_CHARGES")
# for row in cursor:
#     print(row)
# conn.commit()
# conn.close()
# value_range_body = {'values': [[charge[0], str(charge[1])]]
#                   }
# # APPEND
# range_ = 'Sheet2!A1:B1'
# value_input_option = 'USER_ENTERED'
# insert_data_option = 'INSERT_ROWS'
#
# try:
#     request2 = service.spreadsheets().values().append(spreadsheetId=spreadsheet_id, range=range_,
#                                                       valueInputOption=value_input_option,
#                                                       insertDataOption=insert_data_option,
#                                                       body=value_range_body).execute()
#     print(request2)
# except Exception as e:
#     print('error', e)
# sleep(1.1)

