from datetime import datetime, timedelta
import pandas as pd
from .connections import *

#FROM INVOICE NUMBER
def search_by_invoice(list_invoice_number):
    list_invoice_number = tuple(list_invoice_number) if len(list_invoice_number)>1 else f"('{list_invoice_number[0]}')"
    
    query = f"""select partners.name, invoices.number invoice_number, invoices.invoice_date, invoices.due_date, invoices.status,
    invoices.company_id, invoices.document_type_id, invoice_totals.grandTotalUnformatted
    from invoices 
    JOIN invoice_totals
    ON invoices.uuid = invoice_totals.invoice_id
    JOIN partners 
    ON partners.uuid = invoices.partner_id and partners.company_id = invoices.company_id
    
    where 
    invoices.company_id = '74e5d47e-5b78-4921-9058-054704d2ed22'
    and invoice_date >='2024-01-01'
    and invoices.number IN {list_invoice_number}
    and invoices.deleted_at is null
    and invoices.status in (0,3)
    order by invoice_date , due_date
    """

    print(query)
    data_invoice = MySQL.to_pull_data(query) 
    try:
        data_invoice['top'] = data_invoice.apply(lambda s: (s['due_date'] - s['invoice_date']).days, axis=1)
    except:
        data_invoice['top'] = None

    return data_invoice  

def search_payment(start_date:str, company_id:list):
    list_columns = [ 'created_at', 'updated_at', 'company_id',
        'external_id', 'buyer_name',
       'supplier_name', 'status', 
       'amount.buyer_fee_amount', 'amount.cashback_amount',
       'amount.discount_amount', 'amount.grand_total', 'amount.sub_total',
       'amount.supplier_fee_amount']
    
    query = f"""for i in payment_reconciliation_transactions
    filter i.company_id == '{company_id}'
    and i.created_at >= '{start_date}'
    return i"""
    prt =  ArangoDB.to_pull_data('paper_payment',query, batch_size = 1000000)
    
    if prt.empty:
        return pd.DataFrame(columns = list_columns)
    
    return prt[list_columns]

#FROM EXTERNAL ID
def search_by_external_id(list_external_id):

    list_columns = [ 'created_at', 'updated_at', 'company_id',
        'external_id', 'buyer_name',
       'supplier_name', 'status', 
       'amount.buyer_fee_amount', 'amount.cashback_amount',
       'amount.discount_amount', 'amount.grand_total', 'amount.sub_total',
       'amount.supplier_fee_amount']
    
    query = f"""for i in payment_reconciliation_transactions
    filter i.external_id IN {list_external_id}
    return i"""
    prt =  ArangoDB.to_pull_data('paper_payment',query, batch_size = 1000000)
    
    if prt.empty:
        return pd.DataFrame(columns = list_columns)
    
    return prt[list_columns]

def search_invoice(company_id, list_partner_name, start_date, end_date):
    query = f"""select partners.name, invoices.number invoice_number, invoices.invoice_date, invoices.deleted_at, invoices.due_date, invoices.status invoice_status,
    invoices.company_id, invoices.document_type_id, invoice_totals.grandTotalUnformatted
    from invoices 
    JOIN invoice_totals
    ON invoices.uuid = invoice_totals.invoice_id
    JOIN partners 
    ON partners.uuid = invoices.partner_id and partners.company_id = invoices.company_id

    where 
    invoices.company_id = '{company_id}' 
    and LOWER(partners.name) IN {tuple([i.lower() for i in list_partner_name])}
    and invoice_date >='{start_date}'
    and invoice_date <='{end_date}'
    and invoices.deleted_at is null
    and invoices.status in (0,3)
    order by invoice_date , due_date
    """

    print(query)
    data_invoice = MySQL.to_pull_data(query) 
    try:
        data_invoice['top'] = data_invoice.apply(lambda s: (s['due_date'] - s['invoice_date']).days, axis=1)
    except:
        data_invoice['top'] = None

    return data_invoice  


def process_recon(prt,data_invoice):
    #FULLMOON
    FOUND = []
    remarks = []
    for _,i in prt.iterrows():
        external_id = i['external_id']
        company_id = i['company_id']
        amount = i['amount.grand_total']
        with_wht = i['amount.grand_total'] * 1.0202 #tambah 2.02% as WHT
        buyer_name = i['buyer_name']
        payment_date = datetime.strptime(i['created_at'].split('T')[0], '%Y-%m-%d').date()

        invoices = data_invoice[(data_invoice['invoice_date'] <= payment_date )\
                                & (~data_invoice['invoice_number'].isin(remarks))\
                               & (data_invoice['name'].transform(lambda s: buyer_name.upper() in s.upper() or s.upper() in buyer_name.upper()))
                               & (data_invoice['company_id']==company_id)]
        status = ''
        for _, j in invoices.iterrows():
            invoice_number = j['invoice_number']
            invoice_date = j['invoice_date']
            grand_total = j['grandTotalUnformatted']
            top = j['top']
            ontime = 1 if (payment_date - invoice_date).days <= top else 0
            
            status = None
            for TOLERANCES in [2_000,5_000]:
            
                if grand_total == amount:
                    status = (f"""exactly match""")
                    break
                elif grand_total == with_wht:
                    status = (f"""exactly match with wht""")
                    break
                elif abs(grand_total - amount)<=TOLERANCES:
                    status = (f"""match with difference: {abs(grand_total - amount)}""")
                    break
                elif abs(grand_total - with_wht)<=TOLERANCES:
                    status = (f"""include wht match with difference: {abs(grand_total - with_wht)} """)
                    break
                
                
                #10K rules
                add_idr = 10_000
                if grand_total == (amount + add_idr):
                    status = (f"""match with add 10K""")
                    break
                elif grand_total == (with_wht + add_idr):
                    status = (f"""match with wht and add 10K""")
                    break
                elif abs(grand_total - (amount + add_idr)) <= TOLERANCES:
                    status = (f"""match with add 10K, with difference: {abs(grand_total - amount)}""")
                    break
                elif abs(grand_total - (with_wht + add_idr))<=TOLERANCES:
                    status = (f"""include wht and add 10K, with difference: {abs(grand_total - with_wht)} """)
                    break    
            if status:
                break                
                
                
        if status:
            FOUND.append({'company_id':company_id,
                          'buyer_name':buyer_name,
                          'top':top,
                          'ontime':ontime,
                          'payment_date':payment_date.strftime('%Y-%m-%d'),
                          'invoice_date':invoice_date.strftime('%Y-%m-%d'),
                          'external_id':external_id,
                          'invoice_number':invoice_number,
                          'payment_amount':amount,
                          'payment_amount_wht':with_wht,
                          'invoice_amount':grand_total,
                          'status':status
                         })

            remarks.append(invoice_number)
            
        else:
            FOUND.append({'company_id':company_id,
                          'buyer_name':buyer_name,
                          'top':None,
                          'ontime':None,
                          'payment_date':payment_date.strftime('%Y-%m-%d'),
                          'invoice_date':None,
                          'external_id':external_id,
                          'invoice_number':None,
                          'payment_amount':amount,
                          'payment_amount_wht':with_wht,
                          'invoice_amount':None,
                          'status':'not found'
                         })          
            
            

    return FOUND

def search_data(list_invoice_number:list = [] ,list_external_id:list = []):
    list_invoice_number = list_invoice_number if list_invoice_number else []
    list_external_id = list_external_id if list_external_id else []

    BQ = call_bq()

    #check if the invoice number has been reconciliated 
    # def search_by_invoice_number
    filter_invoice = ''
    filter_external_id = ''
    if list_invoice_number:
        filter_invoice = tuple(list_invoice_number) if len(list_invoice_number)>1 else f"('{list_invoice_number[0]}')"
        filter_invoice = f"AND invoice_number IN {filter_invoice}"

    if list_external_id:
        filter_external_id = tuple(list_external_id) if len(list_external_id)>1 else f"('{list_external_id[0]}')"
        if filter_invoice:
            filter_external_id = f"OR external_id IN {filter_external_id}"
        else:
            filter_external_id = f"AND external_id IN {filter_external_id}"

    query = f"""
    SELECT * FROM datascience_public.invoice_reconciliations
    WHERE 1=1  
    {filter_invoice}
    {filter_external_id}
    """
    print(query)
    data_in_bq = BQ.to_pull_data(query)
    external_id_not_found = list(set(list_external_id)-set(data_in_bq['external_id']))
    invoice_number_not_found = list(set(list_invoice_number)-set(data_in_bq['invoice_number']))


    #found all
    if not invoice_number_not_found and not external_id_not_found:
        return data_in_bq.to_dict(orient = 'records')

    global MySQL
    MySQL = call_mysql()
    global ArangoDB
    ArangoDB = call_arangodb()
    
    all_result = data_in_bq.to_dict(orient = 'records')
    # misalnya ada invoice_number yang tidak ketemu    
    if invoice_number_not_found:

        #cari invoice nya
        all_invoice = search_by_invoice(invoice_number_not_found)

        for company_id in all_invoice['company_id'].unique():

            #data per company_id
            data_invoice = all_invoice[all_invoice['company_id']==company_id]

            #get min created_at
            start_date = str(data_invoice['invoice_date'].min())

            #cari posibillity payment nya per company_id
            payment = search_payment(start_date, company_id)

            #analysis
            print('payment:', payment.shape[0], 'invoice:',data_invoice.shape[0])
            result = process_recon(payment,data_invoice)     
            
            result = [i for i in result if i['status']!='not found']

            if result:

                #save to BQ
                BQ.to_push_data(pd.DataFrame(result), 'datascience_public','invoice_reconciliations','append')

                all_result.extend(result)

        not_found = all_invoice[~all_invoice['invoice_number'].isin([i['invoice_number'] for i in all_result])].to_dict(orient = 'records')
        for i in not_found:
            all_result.append({'company_id':i['company_id'],
                          'buyer_name':i['name'],
                          'top':i['top'],
                          'ontime':None,
                          'payment_date':None,
                          'invoice_date':i['invoice_date'],
                          'external_id':None,
                          'invoice_number':i['invoice_number'],
                          'payment_amount':None,
                          'payment_amount_wht':None,
                          'invoice_amount':i['grandTotalUnformatted'],
                          'status':'not found'
                         })  
            
    if external_id_not_found:
        print('search external_id')
        pay = search_by_external_id(external_id_not_found)
        
        for company_id in pay['company_id'].unique():
            #data per company_id
            data_payment = pay[pay['company_id']==company_id]

            #get min created_at
            # Convert 'created_at' to datetime and find the minimum
            min_date = pd.to_datetime(pay['created_at']).min()
            max_date = pd.to_datetime(pay['created_at']).max()

            # Subtract 40 days
            start_date = (min_date - timedelta(days=60)).strftime('%Y-%m-%d')
            end_date = (max_date + timedelta(days=1)).strftime('%Y-%m-%d')

            list_partner_name = list(pay['buyer_name'].unique())
            for i in pay['buyer_name'].unique():

                if i.startswith('PT'):
                    #remove PT
                    list_partner_name.append(i.replace('PT ','').strip())
                else:
                    #ADD PT
                    list_partner_name.append(f"PT {i}".strip())    
                
            #cari posibillity invoice nya per company_id
            print('search invoice from', company_id,list_partner_name)
            data_invoice = search_invoice(company_id, list_partner_name, start_date, end_date)
            #analysis
            print('payment:', data_payment.shape[0], 'invoice:',data_invoice.shape[0])
            result = process_recon(data_payment,data_invoice)   
            if [i for i in result if i['status']!='not found']:
                #save to BQ
                BQ.to_push_data(pd.DataFrame(result), 'datascience_public','invoice_reconciliations','append')

            all_result.extend(result)        

        
    return all_result