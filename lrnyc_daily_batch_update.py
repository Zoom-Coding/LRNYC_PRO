import os
import knackpy
from datetime import datetime
print('INITIALIZE', datetime.now())
lrnycpro_id = '6085d5eae6dded001b98d07b'
lrnycpro_key = '0b663d88-7d2c-4a61-b90d-169e0ee07a95'
knack = knackpy.App(app_id=lrnycpro_id, api_key=lrnycpro_key)
from knack_api_lrnycpro import *
from colorama import Fore, Back, Style
from operator import itemgetter
from requests.adapters import HTTPAdapter
from requests import Session, exceptions
from urllib3.util import Retry
s = Session()
s.mount('https://', HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1,
                                                  method_whitelist=frozenset(['GET', 'POST', 'DELETE', 'PUT']),
                                                  status_forcelist=[500, 502, 503, 504, 521])))
print('READY', datetime.now())
update = False
update_shopify = False
update_weeks = False
update_abandoned = False
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
    r = s.post(url="https://maker.ifttt.com/trigger/lrnyc/with/key/begiApwWH4h8xvb_2TfrOf", data=payload,
               headers=myheaders)

def get_sub_from_id(sub_id):
    return recharge.Subscription.get(sub_id)

def get_onetime_from_id(onetime_id):
    return recharge.Onetime.get(onetime_id)

def get_batch_knack_orders():
    order_filter = {"match":"and","rules":[{"field":"field_142","operator":"is","value":"ACTIVE"},
                                           {"field":"field_81","operator":"does not contain","value":"SKIPPED"},
                                           {"field":"field_81","operator":"does not contain","value":"Manual"}]}
    response = knack.get('object_6', record_limit=2000, filters=order_filter, refresh=True, generate=True)
    #print('batch orders',response )
    return response

def update_week(recharge_week, knack_order_id,order_dates):
    print('values', order_dates)
    if 'CURRENT WEEK' in recharge_week and 'NEXT WEEK' in recharge_week or 'ON HOLD' in recharge_week:
        print('CWNW', recharge_week)
        last_charge = pendulum.parse(order_dates.get('processed_at')).format('MM/DD/YYYY HH:mm A')
        next_charge = pendulum.parse(order_dates.get('scheduled_at')).format('MM/DD/YYYY HH:mm A')
    elif 'CURRENT WEEK' in recharge_week or 'PROCESSED' in recharge_week:
        print('CW', recharge_week)
        last_charge = pendulum.parse(order_dates.get('processed_at')).format('MM/DD/YYYY HH:mm A')
        next_charge = pendulum.parse(order_dates.get('processed_at')).format('MM/DD/YYYY HH:mm A')
    elif 'NEXT WEEK' in recharge_week:
        print('NW', recharge_week)
        if order_dates.get('scheduled_at'):
            last_charge = next_charge = pendulum.parse(order_dates.get('scheduled_at')).format('MM/DD/YYYY HH:mm A')
        else:
            last_charge = next_charge = pendulum.parse(order_dates.get('processed_at')).format('MM/DD/YYYY HH:mm A')
    else:
        print('ERROR', recharge_week)
        if order_dates.get('scheduled_at'):
            last_charge = next_charge = pendulum.parse(order_dates.get('scheduled_at')).format('MM/DD/YYYY HH:mm A')
        else:
            last_charge = next_charge = pendulum.parse(order_dates.get('processed_at')).format('MM/DD/YYYY HH:mm A')


    print('update week, day data last and next',last_charge, next_charge)
    print()

    if update:
        payload = {'id': knack_order_id,
                            'field_350': recharge_week,
                            'field_142': 'ACTIVE' if 'PROCESSED' not in recharge_week else 'INACTIVE',
                            'field_278': f'BATCH UPDATE {pendulum.now().date()}',
                            'field_913': last_charge,
                            'field_914': next_charge,
                            'field_915': pendulum.now('US/Eastern').format('MM/DD/YYYY HH:mm A'),
                            }

        print('payload batch', payload)
        logger.error('UPDATE_WEEK UPDATING Subscription!!!')
        update_knack_order(payload)

def get_knack_subdict_cust_shoporders(knack_orders):
    knack_sub_dict = {}
    knack_active_cust = []
    shopify_orders = []
    for orders in knack_orders:
        if 'SHOPIFY' in orders.get('field_81',''):
            shopify_orders.append(dict(orders))
            continue
        key = str(orders.get('field_903')) + ';' + str(orders.get('field_902'))
        #print(key)
        week = orders.get('field_350') if orders.get('field_350') else []
        knack_sub_dict[key] = {'week':week, 'knack_order_id':orders.get('id'), 'title':orders.get('field_203')}
        knack_active_cust.append(orders.get('field_906'))
    print('active custs', len(knack_active_cust))
    unique_custs = list(set(knack_active_cust))
    # for x, orders in enumerate(shopify_orders,1):
    #     print(x, orders)
    return knack_sub_dict, unique_custs, shopify_orders

def get_active_recharge_customers():
    recharge_active_customers = []
    cust_page_1 = recharge.Customer.list({'status': 'ACTIVE', 'limit': 250, 'page': 1}).get('customers')
    cust_page_2 = recharge.Customer.list({'status': 'ACTIVE', 'limit': 250, 'page': 2}).get('customers')
    total_cust = cust_page_1 + cust_page_2
    for custs in total_cust:
        recharge_active_customers.append(custs.get('id'))
    return recharge_active_customers

def get_customer_charges(master_charge_list,customer_id):
    #print('the customer charge',customer_id)
    customer_charges = [x[1] for i, x in enumerate(master_charge_list) if x[0] == customer_id]
    #print('get charges', customer_charges)

    top_3_charges = customer_charges[0:3]
    sorted_list = sorted(top_3_charges, key=itemgetter('id'))
    #print(len(top_3_charges))

    #for charges in top_3_charges:
        #print(charges.get('email'), charges.get('status'), charges.get('created_at'), charges.get('updated_at'),charges.get('tags'))
    #yep = recharge.Charge.list({'customer_id': customer_id}).get('charges')
    #print(yep)
    #print(f'top 3 {top_3_charges}')
    return top_3_charges

def get_all_recharge_charges():
    master_charge_list = []
    page_num = 1
    z = 0
    result_size = 250
    while result_size == 250:
        all_queued_charges = recharge.Charge.list(
            {'date_min': '2021-09-01', 'date_max': '2022-10-28', 'limit': 250, 'page': page_num,
             'sort_by': 'id-desc', }).get(
            'charges')  # ,'status':'REFUNDED,QUEUED', 'limit':250, 'page':page_num, 'sort_by':'id-desc', 'date_min':'2021-06-18', 'date_max': '2021-09-28'
        # do something with data here
        for charge in all_queued_charges:
            z += 1
            #print(z, charge.get('customer_id'), charge.get('status'), charge.get('id'), charge.get('scheduled_at'),
                  #charge.get('type'), charge.get('tags'))
            master_charge_list.append((charge.get('customer_id'), charge))
        result_size = len(all_queued_charges)
        print('page:', page_num, 'result_size:', result_size)
        page_num += 1
    print(len(master_charge_list))
    # for charge in master_charge_list:
    #     print(charge[0], charge[1].get('email'))


    return master_charge_list

def get_charges(page_num):
    return recharge.Charge.list({'date_min': '2021-07-01', 'limit': 250, 'page': page_num,
                          'sort_by': 'id-desc', }).get('charges')

def get_all_recharge_charges_thread():
    master_charge_list = []
    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        for page_num in range(1,70):
            future = executor.submit(get_charges, page_num)
            futures.append(future)
            print('page',page_num)
        for future in futures:
            all_charges = future.result()
            if len(all_charges) == 0: break
            print(len(all_charges),end=' ')
            for charge in all_charges:
                master_charge_list.append((charge.get('customer_id'), charge))
    print('total charges', len(master_charge_list))
    #print('this is it ', master_charge_list)
    return master_charge_list

def get_all_customer_data(num, customer_order_list, customer_id, knack_sub_dict = {}, debug = True):

    if debug:
        customer_data = recharge.Customer.get(customer_id).get('customer')
        print(num, 'id', customer_data.get('id'), 'Fname->', customer_data.get('first_name'),
              'email->', customer_data.get('email'), 'num active sub->',
              customer_data.get('number_active_subscriptions'), 'num Sub->', customer_data.get('number_subscriptions'))

    recharge_week_dict = {}
    recharge_item_dict = {}
    order_dates = {}
    cancelled_subs = []
    # for x in recharge.Subscription.list({'customer_id': customer_id, 'include_onetimes': True}).get('subscriptions'):
    #     print('Sub', x) if debug else None
    #     if x.get('status') in 'CANCELLED':
    #         cancelled_subs.append(x.get('id'))

    #for x in recharge.Charge.list({'customer_id': customer_id, 'limit': 3, 'sort_by': 'id-desc'}).get('charges'):
    #print('this is customer orders list', customer_order_list)
    for customer_order in customer_order_list:
        print('id', customer_order.get('id'), 'charge', num, 'week ', recharge_week_dict) if debug else None
        valid_orders = ['SUCCESS', 'REFUNDED', 'PARTIALLY_REFUNDED']
        status = customer_order.get('status')
        tags = customer_order.get('tags')
        if status in valid_orders:
            week = get_batch_subscription_weeks(customer_order.get('processed_at'), tags)
            if order_dates.get('processed_at'):
                if (pendulum.parse(order_dates.get('processed_at')) >= pendulum.parse(customer_order.get('processed_at'))):
                    #print('processed', order_dates.get('processed_at'), x.get('processed_at'), )
                    order_dates['processed_at'] = order_dates.get('processed_at')
            else:
                order_dates['processed_at'] = customer_order.get('processed_at')
            print('success week', week,'dates', order_dates) if debug else None
        elif 'QUEUED' in status:
            week = get_batch_subscription_weeks(customer_order.get('scheduled_at'),tags)
            order_dates['scheduled_at'] = customer_order.get('scheduled_at')
            print('queue week', week) if debug else None
        elif 'ERROR' in status:
            week = [customer_order.get('error_type')]
            order_dates['scheduled_at'] = customer_order.get('updated_at')
            print('error week', week) if debug else None
        else:
            week = ['NEEDS REVIEW']
            order_dates['scheduled_at'] = customer_order.get('updated_at')
            print('Review week', week) if debug else None

        for items in customer_order.get('line_items'):
            #print('weekkkk',week)
            print('items-->',items) if debug else None
            #id = str(items.get('subscription_id')) + ';' + str(items.get('shopify_variant_id'))
            id = f'{items.get("subscription_id")};{items.get("shopify_variant_id")};{items.get("title")};{items.get("title")}'
            if 'PROCESSED' not in week:
                if recharge_week_dict.get(id):
                    #print('i found the id',id)
                    if week[0] not in recharge_week_dict[id]:
                        recharge_week_dict[id] = week + recharge_week_dict[id]

                        #print('i found the id here 2', id)
                        print('my week', week, order_dates) if debug else None
                else:
                    recharge_week_dict[id] = week
                    #print('but i did not found the id', id)
                    print('my other week', week, order_dates) if debug else None

    for dict_key, recharge_week in recharge_week_dict.items():
        #print('recharg week dict', recharge_week_dict)
        #print('dis dict',knack_sub_dict.get(key, {}))
        print('dic key', dict_key.split(';', maxsplit=2))
        sub_id, shop_variant, title = dict_key.split(';', maxsplit=2)
        key = f'{sub_id};{shop_variant}'
        #print(sub_id, shop_variant, title,key)
        if 'TEST' in title or 'Thanks' in title:
            continue
        if knack_sub_dict.get(key,{}):
            knack_order_week = knack_sub_dict.get(key, {}).get('week',[])
            print('this is order week', knack_order_week,'key data', knack_sub_dict.get(key,{}))
            if 'TGVG' in knack_order_week:
                print('SKIPPING', key,'recharge_week',recharge_week,'knack_order_week', knack_order_week) if debug else None
                knack_sub_dict.pop(key, 'Not Found')
                continue
            if len(recharge_week) > 1 and 'ON HOLD' in recharge_week:
                recharge_week.remove('ON HOLD')

            print('debug test', key, ' recharge->', recharge_week, 'knack_week->', knack_sub_dict.get(key), order_dates) if debug else None

            if knack_order_week != recharge_week:
                if 'ERROR CHECK' in knack_order_week or 'MANUAL EDIT' in knack_order_week:
                    knack_sub_dict.pop(key, 'Not Found')
                    continue
                print(f'Continue {knack_order_week}, not equal to {recharge_week}')
                customer_data = recharge.Customer.get(customer_id).get('customer')
                print(Back.BLUE,Fore.BLACK)
                print(customer_data.get('email'))
                # print(x, 'id', customer_data.get('id'), 'Fname->', customer_data.get('first_name'),
                #       'email->', customer_data.get('email'), 'num active sub->',
                #       customer_data.get('number_active_subscriptions'), 'num Sub->',
                #       customer_data.get('number_subscriptions'))
                print( key, ' recharge->', recharge_week, 'will overwrite knack_week->', knack_sub_dict.get(key),
                       'Match' if knack_order_week == recharge_week else 'Nope')
                if knack_sub_dict.get(key,{}).get('knack_order_id'):
                    if update_weeks:
                        update_week(recharge_week, knack_sub_dict.get(key,{}).get('knack_order_id'), order_dates)
                print(Style.RESET_ALL)

            knack_sub_dict.pop(key, 'Not Found')
        else:
            sub_id, shop_variant = key.split(';')
            new_key = f'{sub_id}SWAPPEDOUT;{shop_variant}'
            if knack_sub_dict.get(new_key, {}):
                continue
                knack_sub_dict.pop(key, 'Not Found')

            status = recharge.Subscription.get(sub_id).get('subscription', {}).get('status')
            print('\nstatus', status)
            if status and 'CANCELLED' in status:
                continue
            print(Back.YELLOW, Fore.BLACK,'\nKNACK ORDER MATCH NOT FOUND FOR', key, recharge_week, tags,Style.RESET_ALL)
            if 'PROCESSED' in week:
                print(Back.YELLOW,Fore.BLACK,'Order is Cancelled, Manual or PROCESSED',Style.RESET_ALL)
                #continue
            sub = get_sub_from_id(sub_id)
            if sub:
                print('It is a subscription')
                hook = sub
            else:
                print('It is a onetime')
                hook = get_onetime_from_id(sub_id)

            #print('this is the hook', hook,title)
            if hook.get('subscription'):
                hook['subscription']['status'] = 'ACTIVE'
                topic = 'subscription/updated'
                print('update that subscription',hook)
                if update_subscription:
                    SubscriptionUpdated(hook, topic)
            elif hook.get('onetime'):
                hook['onetime']['status'] = 'ACTIVE'
                topic = 'onetime/updated'
                print('update that onetime',hook)
                if update_subscription:
                    OnetimeUpdatedUpdated(hook, topic)
            else:
                print('The missing order ',new_key ,'The subscription Error', hook)
        #print(Style.RESET_ALL)


    return week

def manual_batch_run(all_customers, islive, debug):
    print('START', pendulum.now())
    os.system("say 'i am starting'")
    try:
        send_ifttt_hook('ENTIRE BATCH START', 'ORDERS BEGIN',f'WITH TIME {pendulum.now().to_datetime_string()} ORDERS')
    except:
        pass
    print('FINISH', pendulum.now())
    today = pendulum.now("US/Eastern")
    lastp, nextp = get_processing_dates_batch(today)
    all_recharge_charges = get_all_recharge_charges_thread()
    active_orders = get_batch_knack_orders()
    knack_sub_dict, knack_active_cust_list, shopify_orders = get_knack_subdict_cust_shoporders(active_orders)
    recharge_customer_list = get_active_recharge_customers()
    total_cust_list_knack_recharge = list(set(knack_active_cust_list + recharge_customer_list))
    print( 'knack-cust',len(knack_active_cust_list), 'recharge-cust',len(recharge_customer_list), 'total', len(total_cust_list_knack_recharge))
    print('knack-subscription_dict', len(knack_sub_dict), 'shopify-orders', len(shopify_orders))
    for orders in shopify_orders:
        if islive:
            date = orders.get('field_140').get('iso_timestamp')
            tags = 'First Shopify'
            week = get_batch_subscription_weeks(date, tags)
            if orders.get('field_350') != week:
                print('this is field_350', orders.get('field_350'))
                if 'TGVG' in orders.get('field_350') or 'MANUAL EDIT' in orders.get('field_350'):
                    continue
                print('id', orders.get('field_903'), date, orders.get('field_350'), 'will update to', week, 'knack_order_id', orders.get('id'))
                pendulum.now().to_atom_string()
                update_dates = {'processed_at': lastp.to_atom_string(), 'scheduled_at': nextp.to_atom_string()}
                if update_shopify:
                    update_week(week,orders.get('id'),update_dates)
            else:
                print('shopify orders match',orders.get('field_350'),'week', week,'date',date) if debug else None
    os.system("say 'shopify complete'")
    try:
        send_ifttt_hook('SHOPIFY', 'ORDERS COMPLETE',f'WITH LENGTH {len(shopify_orders)} ORDERS')
    except:
        pass
    print('Finished Shopify Orders', pendulum.now())
    print()

    if all_customers:
        customers_list = total_cust_list_knack_recharge
    else:
        customers_list = [72431620]

    x = 1
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        for customer_id in customers_list:
            if customer_id:
                customer_order_list = get_customer_charges(all_recharge_charges, int(customer_id))
                print(x, end=' ')
                #futures = [executor.submit(get_all_customer_data, x, customer_order_list, int(customer_id), knack_sub_dict, debug)]
                get_all_customer_data(x, customer_order_list, int(customer_id), knack_sub_dict, debug)
                #print(futures)
                x += 1
            else:
                print('Error: Customer ID not Found',customer_id)
        os.system("say 'Knack Checker complete'")
        try:
            send_ifttt_hook('RECHARGE KNACK CHECKER ', 'CHECKING COMPLETE', f'WITH LENGTH {len(customers_list)} CUSTOMERS AND {len(all_recharge_charges)} CHARGES')
        except:
            pass

    if islive:
        for key, abandonded_items in knack_sub_dict.items():
            if 'SWAPPED' in key or 'Thanks' in abandonded_items.get('title',''):
                continue
            print('\n-> abandoned items', key, abandonded_items)

            update_dates = {'processed_at': lastp.to_atom_string(), 'scheduled_at': nextp.to_atom_string()}
            if update_abandoned:
                update_week(['PROCESSED'], abandonded_items.get('knack_order_id'), update_dates)
        os.system("say 'ABANDONED KNACK ORDERS Complete'")
        try:
            send_ifttt_hook('ABANDONED KNACK ORDERS', 'ORDERS COMPLETE', f'WITH LENGTH {len(knack_sub_dict)} ORDERS')
        except:
            pass

    print(len(knack_sub_dict))
    try:
        send_ifttt_hook('ENTIRE BATCH FINISHED', 'ORDERS COMPLETE',f'WITH TIME {pendulum.now().to_datetime_string()} ORDERS')
    except:
        pass
    print('FINISH', pendulum.now())

#manual_batch_run(all_customers, islive,debug)

try:
    manual_batch_run(True, True, False)
except:
    pass

