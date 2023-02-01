# f1 = open('url_input.txt','r').read().splitlines()
# f2 = open('error.txt','r').read().splitlines()

# todo_list = list(set(f1)-set(f2))

# f3 = open('c.txt','w')
# for i in todo_list:
#     f3.write(i+'\n')

import pymysql
from tqdm import tqdm

conn = pymysql.connect(host='172.30.1.51', user='root', password='vision9551', db='kisti_crawl')
cursor = conn.cursor()

url_list = open('error.txt','r').read().splitlines()

for url in tqdm(url_list):
    sql = f'''delete from amz_category_with_asin where url = "{url}"'''
    cursor.execute(sql)
    conn.commit()