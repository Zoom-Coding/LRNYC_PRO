import os
import knackpy
from knack_api_lrnycpro import *

print('INITIALIZE', pendulum.now())
api_lrnycpro_id = '6085d5eae6dded001b98d07b'
api_lrnycpro_key = '0b663d88-7d2c-4a61-b90d-169e0ee07a95'

api_lrnycpro_id2 = '61e3cfbe42e3ef001e60b3d3'
api_lrnycpro_key2 = '36dfa40d-d541-4d99-9aaa-ea854baf2d6b'

knack = knackpy.App(app_id=api_lrnycpro_id, api_key=api_lrnycpro_key)

from colorama import Fore, Back, Style
from time import sleep
import random
import json
import requests
import syslog

update = True
update_shopify = False
update_weeks = False
update_abandoned = True
update_subscription = False


def send_ifttt_hook(topic, order_id, body):
    myheaders = {
        "Accept": "*/*",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive"
    }
    payload = json.dumps(
        {"value1": topic, "value2": order_id,
         "value3": body})
    print("just sent ifttt")
    syslog.syslog("just sent ifttt")

    r = requests.post(url="https://maker.ifttt.com/trigger/lrnyc/with/key/begiApwWH4h8xvb_2TfrOf", data=payload,
                      headers=myheaders)


def get_sub_from_id(sub_id):
    return recharge.Subscription.get(sub_id)


def get_onetime_from_id(onetime_id):
    return recharge.Onetime.get(onetime_id)


def get_batch_knack_orders():
    order_filter = {"match": "and", "rules": [{"field": "field_142", "operator": "is", "value": "ACTIVE"},
                                              {"field": "field_81", "operator": "does not contain", "value": "SKIPPED"},
                                              {"field": "field_81", "operator": "does not contain", "value": "Manual"}]}
    response = knack.get('object_6', record_limit=1800, filters=order_filter, refresh=True, generate=True)
    return response


def update_week(recharge_week, knack_order_id, order_dates):
    rand = random.uniform(.3, 1)
    # print("sleeping for", rand )
    sleep(rand)
    # print(" done sleeping for", rand)

    if 'CURRENT WEEK' in recharge_week and 'NEXT WEEK' in recharge_week or 'ON HOLD' in recharge_week:
        last_charge = pendulum.parse(order_dates.get('processed_at')).format('MM/DD/YYYY HH:mm A')
        next_charge = pendulum.parse(order_dates.get('scheduled_at')).format('MM/DD/YYYY HH:mm A')
    elif 'CURRENT WEEK' in recharge_week or 'PROCESSED' in recharge_week:
        last_charge = pendulum.parse(order_dates.get('processed_at')).format('MM/DD/YYYY HH:mm A')
        next_charge = pendulum.parse(order_dates.get('processed_at')).format('MM/DD/YYYY HH:mm A')
    elif 'NEXT WEEK' in recharge_week:
        if order_dates.get('scheduled_at'):
            last_charge = next_charge = pendulum.parse(order_dates.get('scheduled_at')).format('MM/DD/YYYY HH:mm A')
        else:
            last_charge = next_charge = pendulum.parse(order_dates.get('processed_at')).format('MM/DD/YYYY HH:mm A')
    else:
        if order_dates.get('scheduled_at'):
            last_charge = next_charge = pendulum.parse(order_dates.get('scheduled_at')).format('MM/DD/YYYY HH:mm A')
        else:
            last_charge = next_charge = pendulum.parse(order_dates.get('processed_at')).format('MM/DD/YYYY HH:mm A')

    if update:
        payload = {'id': knack_order_id,
                   'field_350': recharge_week,
                   'field_142': 'ACTIVE' if 'PROCESSED' not in recharge_week else 'INACTIVE',
                   'field_278': f'BATCH UPDATE {pendulum.now().date()}',
                   'field_913': last_charge,
                   'field_914': next_charge,
                   'field_915': pendulum.now('US/Eastern').format('MM/DD/YYYY HH:mm A'),
                   }
        syslog.syslog(f'Im in update week')

        logger.error('UPDATE_WEEK UPDATING Subscription!!!')
        update_knack_order(payload)


def get_knack_subdict_cust_shoporders(knack_orders):
    knack_sub_dict = {}
    knack_active_cust = []
    shopify_orders = []
    for orders in knack_orders:
        try:
            if 'SHOPIFY' in orders.get('field_81', ''):
                shopify_orders.append(dict(orders))
                continue
            key = str(orders.get('field_903')) + ';' + str(orders.get('field_902'))
            week = orders.get('field_350') if orders.get('field_350') else []
            knack_sub_dict[key] = {'week': week, 'knack_order_id': orders.get('id'), 'title': orders.get('field_203')}
            knack_active_cust.append(orders.get('field_906'))
        except Exception as e:
            print('get_knack_subdict_cust_shoporders error', e)
    unique_custs = list(set(knack_active_cust))

    return knack_sub_dict, unique_custs, shopify_orders


def get_active_recharge_customers():
    recharge_active_customers = []
    cust_page_1 = recharge.Customer.list({'status': 'ACTIVE', 'limit': 250, 'page': 1}).get('customers')
    cust_page_2 = recharge.Customer.list({'status': 'ACTIVE', 'limit': 250, 'page': 2}).get('customers')
    total_cust = cust_page_1 + cust_page_2
    for custs in total_cust:
        recharge_active_customers.append(custs.get('id'))
    return recharge_active_customers


def get_customer_charges(master_charge_list, customer_id):
    customer_charges = [x[1] for i, x in enumerate(master_charge_list) if x[0] == customer_id]
    top_3_charges = customer_charges[0:3]
    return top_3_charges


def get_charges(page_num):
    look_back_date = pendulum.now().subtract(months=1).to_date_string()
    return recharge.Charge.list({'updated_at_min': look_back_date, 'limit': 250, 'page': page_num,
                                 'sort_by': 'id-desc', }).get('charges')


def _get_charges(page_num):
    look_back_date = pendulum.now().subtract(months=1).to_date_string()
    return recharge.Charge.list({'date_min': look_back_date, 'limit': 250, 'page': page_num,
                                 'sort_by': 'id-desc', }).get('charges')


def get_all_recharge_charges_thread():
    master_charge_list = []
    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        for page_num in range(1, 10):
            future = executor.submit(get_charges, page_num)
            futures.append(future)
        for future in futures:
            all_charges = future.result()
            if len(all_charges) == 0: break
            print(len(all_charges), end=' ')
            for charge in all_charges:
                master_charge_list.append((charge.get('customer_id'), charge))
                # print('added customer',charge)
    print('total charges', len(master_charge_list))
    syslog.syslog(f'total charges {len(master_charge_list)}')
    return master_charge_list


def get_all_customer_data(num, customer_order_list, customer_id, knack_sub_dict={}, debug=True):
    recharge_week_dict = {}
    order_dates = {}
    for customer_order in customer_order_list:
        valid_orders = ['SUCCESS', 'REFUNDED', 'PARTIALLY_REFUNDED']
        status = customer_order.get('status')
        tags = customer_order.get('tags')

        if status in valid_orders:
            week = get_batch_subscription_weeks(customer_order.get('processed_at'), tags)
            if order_dates.get('processed_at'):
                if (pendulum.parse(order_dates.get('processed_at')) >= pendulum.parse(
                        customer_order.get('processed_at'))):
                    order_dates['processed_at'] = order_dates.get('processed_at')
            else:
                order_dates['processed_at'] = customer_order.get('processed_at')
        elif 'QUEUED' in status:
            week = get_batch_subscription_weeks(customer_order.get('scheduled_at'), tags)
            order_dates['scheduled_at'] = customer_order.get('scheduled_at')
        elif 'ERROR' in status:
            week = [customer_order.get('error_type')]
            order_dates['scheduled_at'] = customer_order.get('updated_at')
        else:
            week = ['NEEDS REVIEW']
            order_dates['scheduled_at'] = customer_order.get('updated_at')

        for items in customer_order.get('line_items'):
            id = f'{items.get("subscription_id")};{items.get("shopify_variant_id")};{items.get("title")};{items.get("title")}'
            if 'PROCESSED' not in week:
                if recharge_week_dict.get(id):
                    if week[0] not in recharge_week_dict[id]:
                        recharge_week_dict[id] = week + recharge_week_dict[id]
                else:
                    recharge_week_dict[id] = week

    for dict_key, recharge_week in recharge_week_dict.items():
        print(num, 'dic key', dict_key.split(';', maxsplit=2))
        sub_id, shop_variant, title = dict_key.split(';', maxsplit=2)
        key = f'{sub_id};{shop_variant}'
        if knack_sub_dict.get(key, {}):
            syslog.syslog(f'found key {num}, dict key, {dict_key}, recharge week {recharge_week}')
            knack_order_week = knack_sub_dict.get(key, {}).get('week', [])
            if len(recharge_week) > 1 and 'ON HOLD' in recharge_week:
                recharge_week.remove('ON HOLD')

            if knack_order_week != recharge_week:
                if 'ERROR CHECK' in knack_order_week or 'MANUAL EDIT' in knack_order_week:
                    knack_sub_dict.pop(key, 'Not Found')
                    continue
                customer_data = recharge.Customer.get(customer_id).get('customer')
                print(Back.BLUE, Fore.BLACK)
                print(customer_data.get('email'))
                print(key, ' recharge->', recharge_week, 'will overwrite knack_week->', knack_sub_dict.get(key),
                      'Match' if knack_order_week == recharge_week else 'Nope')
                if knack_sub_dict.get(key, {}).get('knack_order_id'):
                    if update_weeks:
                        update_week(recharge_week, knack_sub_dict.get(key, {}).get('knack_order_id'), order_dates)
                print(Style.RESET_ALL)
            knack_sub_dict.pop(key, 'Not Found')
        else:
            # check to see if the order was actually swapped out by adding swapped to subscription and skip if its found
            sub_id, shop_variant = key.split(';')
            new_key = f'{sub_id}SWAPPEDOUT;{shop_variant}'
            if knack_sub_dict.get(new_key, {}):
                continue
            # if recharge says the order is there but knack cant find it, get the missing orders status
            print("get sub status", sub_id)
            # todo fix bottle neck
            status = recharge.Subscription.get(sub_id).get('subscription', {}).get('status')
            # skip if missing order is a cancelled order
            if status and 'CANCELLED' in status:
                continue
            print(Back.YELLOW, Fore.BLACK, '\nKNACK ORDER MATCH NOT FOUND FOR', key, recharge_week, tags,
                  Style.RESET_ALL)
            if 'PROCESSED' in week:
                print(Back.YELLOW, Fore.BLACK, 'Order is Cancelled, Manual or PROCESSED', Style.RESET_ALL)
            # get the recharge subscription id for this missing oerder and use it to request the subscription record
            # todo fix bottle neck
            sub = ""  # get_sub_from_id(sub_id)
            # get proper subscription order record or onetime
            if sub:
                print('It is a subscription')
                hook = sub
            else:
                print('It is a onetime')
                # todo fix bottle neck
                hook = "speed test"  # get_onetime_from_id(sub_id)

            # post order to knack using sub update or onetime update to replace missing active order.
            if hook.get('subscription'):
                hook['subscription']['status'] = 'ACTIVE'
                topic = 'subscription/updated'
                print('update that subscription', hook)
                if update_subscription:
                    # todo fix bottle neck
                    SubscriptionUpdated(hook, topic)
            elif hook.get('onetime'):
                hook['onetime']['status'] = 'ACTIVE'
                topic = 'onetime/updated'
                print('update that onetime', hook)
                if update_subscription:
                    # todo fix bottle neck
                    OnetimeUpdated(hook, topic)
            else:
                print('The missing order ', new_key, 'The subscription Error', hook)
    return week


def manual_batch_run(all_customers, islive, debug):
    print('START', pendulum.now())
    try:
        send_ifttt_hook('ENTIRE BATCH START', 'ORDERS BEGIN', f'WITH TIME {pendulum.now().to_datetime_string()} ORDERS')
    except:
        pass
    today = pendulum.now("US/Eastern")
    lastp, nextp = get_processing_dates_batch(today)
    all_recharge_charges = get_all_recharge_charges_thread()
    # get all active knack orders minus skipped and manual changes
    active_orders = get_batch_knack_orders()
    print('active', )
    # get dictionary of of sub;variant key,week,order_id,title, list of rechargecust id's and list of shopify orders
    knack_sub_dict, knack_active_cust_list, shopify_orders = get_knack_subdict_cust_shoporders(active_orders)
    # get list of all active recharge customer id's
    recharge_customer_list = get_active_recharge_customers()
    # merge knack and recharge customer id's list
    total_cust_list_knack_recharge = list(set(knack_active_cust_list + recharge_customer_list))
    print('knack-cust', len(knack_active_cust_list), 'recharge-cust', len(recharge_customer_list), 'total',
          len(total_cust_list_knack_recharge))
    print('-------knack-subscription_dict', len(knack_sub_dict), 'shopify-orders', len(shopify_orders))

    # use shopify list to find shopify orders and mark them as processed or current week based on orderdate
    shopnum = len(shopify_orders)
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        for orders in shopify_orders:
            if islive:
                # get datetime from orderdate
                date = orders.get('field_140').get('iso_timestamp')
                tags = 'First Shopify'
                week = get_batch_subscription_weeks(date, tags)
                if orders.get('field_350') != week:
                    # print('this is field_350', orders.get('field_350'))
                    if 'TGVG' in orders.get('field_350') or 'MANUAL EDIT' in orders.get('field_350'):
                        continue
                    print(shopnum, 'id', orders.get('field_903'), date, orders.get('field_350'), 'will update to', week,
                          'knack_order_id', orders.get('id'))
                    pendulum.now().to_atom_string()
                    update_dates = {'processed_at': lastp.to_atom_string(), 'scheduled_at': nextp.to_atom_string()}
                    if update_shopify:
                        syslog.syslog(f'shopify {week}, order id, {orders.get("id")}')
                        futures = [executor.submit(update_week, week, orders.get('id'), update_dates)]
                    # update_week(week,orders.get('id'),update_dates)
                else:
                    print('shopify orders match', orders.get('field_350'), 'week', week, 'date',
                          date) if debug else None
                shopnum += -1
    try:
        send_ifttt_hook('SHOPIFY', 'ORDERS COMPLETE', f'WITH LENGTH {len(shopify_orders)} ORDERS')
    except:
        pass
    print('Finished Shopify Orders', pendulum.now())
    print()

    # set list of all customers to process
    if all_customers:
        customers_list = total_cust_list_knack_recharge
    else:
        customers_list = [72431620]

    x = 1
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        for customer_id in customers_list:
            x += 1
            # if x > 50:
            #     continue
            # loop through customers and get all their orders from the allrecharge charges list of 7-21 to today
            if customer_id:
                # get top 3 orders
                customer_order_list = get_customer_charges(all_recharge_charges, int(customer_id))
                # print(x, end=' ')
                futures = [
                    executor.submit(get_all_customer_data, x, customer_order_list, int(customer_id), knack_sub_dict,
                                    debug)]
                # use the list of top 3 orders, the customers id, the knack sub;variant dict, to check customer for needed updates

                # print(futures)

            else:
                print('Error: Customer ID not Found', customer_id)
        try:
            send_ifttt_hook('RECHARGE KNACK CHECKER ', 'CHECKING COMPLETE',
                            f'WITH LENGTH {len(customers_list)} CUSTOMERS AND {len(all_recharge_charges)} CHARGES')
        except:
            pass
    # print('Continue?')
    # z = input()
    # if 'y' not in z:
    #     return
    countdown = len(knack_sub_dict)
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        if islive:
            print(len(knack_sub_dict))
            # With the remaining items is the dictionary of all knack orders, skipp any that are left over swaps
            for key, abandonded_items in knack_sub_dict.items():
                if 'SWAPPED' in key or 'Thanks' in abandonded_items.get('title', ''):
                    continue
                print('\n->', countdown, ' abandoned items', key, abandonded_items)

                update_dates = {'processed_at': lastp.to_atom_string(), 'scheduled_at': nextp.to_atom_string()}
                # update orders left in dictionary to processed using update dates lastp and next p
                if update_abandoned:
                    syslog.syslog(f'abandoned KEy {key}, abandoned items, {abandonded_items}')
                    futures = [executor.submit(update_week, ['PROCESSED'], abandonded_items.get('knack_order_id'),
                                               update_dates)]
                    # update_week(['PROCESSED'], abandonded_items.get('knack_order_id'), update_dates)
                countdown += -1
            try:
                send_ifttt_hook('ABANDONED KNACK ORDERS', 'ORDERS COMPLETE',
                                f'WITH LENGTH {len(knack_sub_dict)} ORDERS')
            except:
                pass

    try:
        send_ifttt_hook('ENTIRE BATCH FINISHED', 'ORDERS COMPLETE',
                        f'WITH TIME {pendulum.now().to_datetime_string()} ORDERS')
    except:
        pass


## ----- Nightly Batch Checker -----
send_ifttt_hook('Nighly Batch Checker Start', 'Checking for Payments Complete',
                f'WITH TIME {pendulum.now().to_datetime_string()} Charges')

charge_len_holder = 0
started = False
order_count_history = []
# while True:
#     if pendulum.now().day_of_week == pendulum.FRIDAY:
#         next_friday = str(pendulum.now().next(pendulum.FRIDAY))
#         charges = all_queued_charges = recharge.Charge.list(
#             {'status': 'queued', 'date_max': next_friday, 'limit': 250, 'page': 1,
#              'sort_by': 'id-desc', }).get(
#             'charges')
#         order_count_history.append(len(charges))
#         if not all_queued_charges:
#             print('batch_over')
#             try:
#                 send_ifttt_hook('RECharge BATCH Payments', 'PAYments COMPLETE',
#                                 f'WITH TIME {pendulum.now().to_datetime_string()} Charges')
#             except:
#                 pass
#             break
#         else:
#
#             if len(order_count_history) >= 2:
#                 print(order_count_history)
#                 print(order_count_history[:-1])
#                 print(order_count_history[:-2])
#             print(len(charges), charges[0]['scheduled_at'], next_friday, pendulum.now().to_day_datetime_string())
#             syslog.syslog(
#                 f'Len Charges  {len(charges)} {charges[0]["scheduled_at"]} {next_friday} {pendulum.now().to_day_datetime_string()}')
#             sleep(30)
#     else:
#         print('Not friday Yet')
#         sleep(30)

try:
    manual_batch_run(True, True, False)
except Exception as e:
    print('this went wrong', e)
    syslog.syslog(f'This went Wrong in Batch Checker  {e} (at Time) {pendulum.now()}')

print('FINISHED FINAL CYA NEXT WEEK', pendulum.now())
syslog.syslog(f'FINISHED FINAL CYA NEXT WEEK {pendulum.now()}')

