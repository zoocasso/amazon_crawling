import pymysql

conn = pymysql.connect(host='139.150.82.178', user='root', password='vision9551', db='kisti_crawl')

url_list = open('temp.txt','r').read().splitlines()

cursor = conn.cursor()
for url in url_list:
    print(f"""{url_list.index(url)+1} / {len(url_list)}""")
    for table in ["amz_body_content_tb","amz_feature_rating_tb","amz_product_detail_tb","amz_product_feature_tb","amz_product_info_tb","amz_review_keyword_tb"]:
        try:
            sql_1 = f"""SELECT create_date FROM {table}
            WHERE url = '{url}'
            GROUP BY create_date
            ORDER BY create_date ASC
            LIMIT 1;"""
            cursor.execute(sql_1)
            a = cursor.fetchone()[0]
            
            sql_2 = f"""delete FROM {table} WHERE url = '{url}' and create_date = '{a}'\n"""
            cursor.execute(sql_2)
            b = cursor.fetchall()

            conn.commit()
        except:
            pass