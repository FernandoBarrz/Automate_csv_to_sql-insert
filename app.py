'''EXTRACT AND TRANSFORM excel data (csv) to sql inserts'''



raw_data = []
with open('input.csv', 'r') as input_data:
    for line in input_data:
        raw_data.append(line)

#? TABLE DATA
BASE_INDEX: str = 'OP/06557'
TABLE_NAME = 'stock_warehouse_orderpoint'
ACTIVE = 'true'
QTY_MULTIPLE = '1'
COMPANY_ID = '1'
LEAD_TYPE = 'supplier'
#? TABLE DATA

temp_next_index_count = int(BASE_INDEX.split('/')[1]) - 1

EQUAL_VALUES_ALM_X_PROD = {}

to_review_values = {}

# List of list - every item is an excel row
output_data = []


def generate_index():
    global temp_next_index_count
    next_index = f'OP/{str(temp_next_index_count + 1).zfill(5)}'
    temp_next_index_count += 1 
    return next_index


def clean_data():
    '''Format raw data'''
    global to_review_values
    for index, row in enumerate(raw_data):
        *_, almacen, _, c_articulo, stock_min, stock_max = row.strip('\n').split(',')
        temp_match = (almacen.strip().replace(' ', ''), c_articulo.replace('-', '').replace(' ', '').replace('/', ''))
        print(temp_match)
        #print(temp_match.__repr__())
        temp_index = generate_index()
        if temp_match in EQUAL_VALUES_ALM_X_PROD:
            to_review_values[temp_match] = f"Clave producto: {c_articulo} --> Number line at input.csv: {index}"

        else:
            EQUAL_VALUES_ALM_X_PROD[temp_match] = 1
            

        output_data.append([temp_index, almacen, c_articulo, stock_min, stock_max])



def create_query_insert():
    clean_data()
    with open('output_inserts.sql', 'w+') as temp:
        temp.write(f'INSERT INTO {TABLE_NAME} (name, active, warehouse_id, location_id, product_id ,product_min_qty, product_max_qty, qty_multiple, company_id, lead_type)\n')
        temp.write('VALUES\n')
        for value in output_data:
            temp_index, almacen, c_articulo, stock_min, stock_max = value
            temp.write(f"('{temp_index}', {ACTIVE}, (select id from stock_warehouse where name = '{almacen}'), (select id from stock_location where name = '{almacen}'), (select id from product_product where default_code = '{c_articulo}'), {stock_min}, {stock_max}, {QTY_MULTIPLE}, {COMPANY_ID}, '{LEAD_TYPE}'),\n")
        temp.write(';')



create_query_insert()


if len(to_review_values) > 0:
    print(f"There is/are ({len(to_review_values)}) repeated value(s).\n")
    for k, v in to_review_values.items():
        print(f'The repeated values are: {k} ---> Review at: {v}\n')
else:
    print("There are no repeated values")

