from sqlalchemy import create_engine
import pymysql
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import pandas as pd
import crawl_amz
import time

db_connection_str = 'mysql+pymysql://root:vision9551@172.30.1.51/temp'
db_connection = create_engine(db_connection_str)
conn = db_connection.connect()

db = pymysql.connect(host='172.30.1.51', user='root', password='vision9551', db='kisti_crawl', charset='utf8')
cursor = db.cursor()

URL_ADDRESS = "https://www.amazon.com/"
URL_LIST = open("url_input.txt", "r", encoding="utf-8").read().splitlines()

options = Options()
driver = webdriver.Firefox(options=options,executable_path="./geckodriver.exe")

for url in URL_LIST:
    asin_list = list()

    cursor.execute(f"select product_key from amz_product_info where url = '{url}'")
    rows = cursor.fetchall()
    product_key_list = list()
    for row in rows:
        product_key_list.append(row[0])
    # print(product_key_list)

    cursor.execute(f"select asin from amz_category_with_asin where url = '{url}'")
    rows = cursor.fetchall()
    asin_list = list()
    for row in rows:
        asin_list.append(row[0])
    # print(asin_list)

    todo_asin_list = list(set(asin_list) - set(product_key_list))
    recycle_asin_list = list(set(asin_list) - set(todo_asin_list))
    """
        [todo_asin_list] asin코드 참조하여 크롤링해야하는 목록
    """
    print(len(todo_asin_list))
    for todo_asin in todo_asin_list:
        cursor.execute(f"select * from amz_category_with_asin where url = '{url}' and asin = '{todo_asin}'")
        rows = cursor.fetchall()
        row_list = list()
        for row in rows:
            row_dict = dict()
            row_dict['url'] = row[0]
            row_dict['product_idx'] = row[1]
            row_dict['asin'] = row[2]
            row_dict['create_date'] = row[3]
            row_list.append(row_dict)
        # print(row_list)

        for row_dict in row_list:
            URL_SUFFIX = "/dp/"+ row_dict['asin']
            # amazon서버 불러오기
            driver.get(URL_ADDRESS + URL_SUFFIX)
            driver.implicitly_wait(10)
            time.sleep(0.5)
            product_detail = dict()
            product_feature =dict()
            product_info = dict()
            reviewDict = dict()
            feature_rating = dict()
            review_keyword = dict()
            body_content = list()
            
            crawl_amz.goToDetailPage(cursor,driver,row_dict['url'],row_dict['asin'],row_dict['create_date'],row_dict['product_idx'],product_detail,product_feature,product_info,reviewDict,feature_rating,review_keyword,body_content)



    """
        [recycle_asin_list] 기존 DB에서 where문으로 조회해서 가져와야하는 목록
    """
    print(len(recycle_asin_list))
    new_product_info_list = list()

    for recycle_asin in recycle_asin_list:
        """
            amz_category_wirh_asin
        """
        cursor.execute(f"select * from amz_category_with_asin where url = '{url}' and asin = '{recycle_asin}'")
        amz_cateogory_with_asin_db = cursor.fetchone()

        """
            amz_product_info
        """
        cursor.execute(f"select * from amz_product_info where url = '{url}' and product_key = '{recycle_asin}'")
        amz_product_info_db = cursor.fetchone()

        new_product_info_dict = dict()
        new_product_info_dict['url'] = amz_cateogory_with_asin_db[0]
        new_product_info_dict['product_key'] = amz_cateogory_with_asin_db[2]
        new_product_info_dict['product_idx'] = int(amz_cateogory_with_asin_db[1])
        new_product_info_dict['create_date'] = amz_cateogory_with_asin_db[3]
        new_product_info_dict['level1'] = amz_product_info_db[4]
        new_product_info_dict['level2'] = amz_product_info_db[5]
        new_product_info_dict['level3'] = amz_product_info_db[6]
        new_product_info_dict['level4'] = amz_product_info_db[7]
        new_product_info_dict['product_name'] = amz_product_info_db[8]
        new_product_info_dict['product_price'] = amz_product_info_db[9]
        new_product_info_dict['review_score'] = amz_product_info_db[10]
        new_product_info_dict['review_number'] = amz_product_info_db[11]
        new_product_info_dict['5star'] = amz_product_info_db[12]
        new_product_info_dict['4star'] = amz_product_info_db[13]
        new_product_info_dict['3star'] = amz_product_info_db[14]
        new_product_info_dict['2star'] = amz_product_info_db[15]
        new_product_info_dict['1star'] = amz_product_info_db[16]
        new_product_info_list.append(new_product_info_dict)
        
        
        # amz_feature_rating        
        cursor.execute(f"insert into temp.amz_feature_rating select * from kisti_crawl.amz_feature_rating where url = '{url}' and product_key = '{recycle_asin}';")
        
        # amz_product_detail        
        cursor.execute(f"insert into temp.amz_product_detail select * from kisti_crawl.amz_product_detail where url = '{url}' and product_key = '{recycle_asin}';")

        # amz_product_feature        
        cursor.execute(f"insert into temp.amz_product_feature select * from kisti_crawl.amz_product_feature where url = '{url}' and product_key = '{recycle_asin}';")

        # amz_review_keyword        
        cursor.execute(f"insert into temp.amz_review_keyword select * from kisti_crawl.amz_review_keyword where url = '{url}' and product_key = '{recycle_asin}';")
        
        # amz_body_content
        cursor.execute(f"insert into temp.amz_body_content select * from kisti_crawl.amz_body_content where url = '{url}' and product_key = '{recycle_asin}';")
        db.commit()

    new_product_info_df = pd.DataFrame(new_product_info_list)
    new_product_info_df.to_sql(name='amz_product_info',con=db_connection, if_exists='append', index=False)