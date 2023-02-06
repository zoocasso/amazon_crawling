from sqlalchemy import create_engine
import pymysql
import pandas as pd
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from bs4 import BeautifulSoup
from datetime import datetime
import time
import re

db_connection_str = 'mysql+pymysql://root:vision9551@172.30.1.51/temp'
db_connection = create_engine(db_connection_str)
conn = db_connection.connect()

db = pymysql.connect(host='172.30.1.51', user='root', password='vision9551', db='temp', charset='utf8')
cursor = db.cursor()

create_date = str(datetime.now()).split(' ')[0].strip()

def insert_db(url,asin,product_order,product_info,product_detail,product_feature,feature_rating,review_keyword,body_content):
    cursor.execute(f"select * from amz_category where url = '{url}'")
    rows = cursor.fetchall()
    level_list = list()
    for row in rows:
        level_list.append(row[0])
        level_list.append(row[1])
        level_list.append(row[2])
        level_list.append(row[3])

    try:
        product_info_list = list()
        product_info_dict = dict()
        product_info_dict['url'] = url
        product_info_dict["product_key"] = asin
        product_info_dict["product_idx"] =product_order
        product_info_dict["create_date"] = create_date
        product_info_dict["level1"] = level_list[0]
        product_info_dict["level2"] = level_list[1]
        product_info_dict["level3"] = level_list[2]
        product_info_dict["level4"] = level_list[3]
        product_info_dict["product_name"] = product_info["Product_name"]
        product_info_dict["product_price"] = product_info["Product_price"]
        product_info_dict["review_score"] = product_info["totalRatingStar"]
        product_info_dict["review_number"] = product_info["totalReviewCount"]
        product_info_dict["5star"] = product_info["5star"]
        product_info_dict["4star"] = product_info["4star"]
        product_info_dict["3star"] = product_info["3star"]
        product_info_dict["2star"] = product_info["2star"]
        product_info_dict["1star"] = product_info["1star"]
        
        product_info_list.append(product_info_dict)
        product_info_df = pd.DataFrame(product_info_list)
        product_info_df.to_sql(name='amz_product_info',con=db_connection, if_exists='append', index=False)
    except:
        pass

    try:
        index_1 = 1
        product_detail_list=list()
        for key in product_detail:
            product_detail_dict = dict()
            product_detail_dict['url'] = url
            product_detail_dict["product_key"] = asin
            product_detail_dict["product_idx"] = index_1
            product_detail_dict["create_date"] = create_date
            product_detail_dict["detail_table_key"] = key
            product_detail_dict["detail_table_value"] = product_detail[key]
            product_detail_list.append(product_detail_dict)
            index_1 += 1
        
        product_detail_df = pd.DataFrame(product_detail_list)
        product_detail_df.to_sql(name='amz_product_detail',con=db_connection, if_exists='append', index=False)
    except:
        pass

    try:
        index_2 = 1
        feature_rating_list = list()
        for key in feature_rating:
            feature_rating_dict = dict()
            feature_rating_dict['url'] = url
            feature_rating_dict["product_key"] = asin
            feature_rating_dict["product_idx"] = index_2
            feature_rating_dict["create_date"] = create_date
            feature_rating_dict["feature_title"] = key
            feature_rating_dict["feature_rating"] = feature_rating[key]
            feature_rating_list.append(feature_rating_dict)
            index_2 += 1
        
        feature_rating_df = pd.DataFrame(feature_rating_list)
        feature_rating_df.to_sql(name='amz_feature_rating',con=db_connection, if_exists='append', index=False)
    except:
        pass

    try:
        index_3 = 1
        product_feature_list = list()
        for key in product_feature:
            product_feature_dict = dict()
            product_feature_dict['url'] = url
            product_feature_dict["product_key"] = asin
            product_feature_dict["product_idx"] = index_3
            product_feature_dict["create_date"] = create_date
            product_feature_dict["feature_list_content"] = product_feature[key]
            product_feature_list.append(product_feature_dict)
            index_3 += 1
        
        product_feature_df = pd.DataFrame(product_feature_list)
        product_feature_df.to_sql(name='amz_product_feature',con=db_connection, if_exists='append', index=False)
    except:
        pass

    try:
        index_4 = 1
        review_keyword_list = list()
        for key in review_keyword:
            review_keyword_dict = dict()
            review_keyword_dict['url'] = url
            review_keyword_dict["product_key"] = asin
            review_keyword_dict["product_idx"] = index_4
            review_keyword_dict["create_date"] = create_date
            review_keyword_dict["keyword"] = review_keyword[key]
            review_keyword_list.append(review_keyword_dict)
            index_4 += 1
        
        review_keyword_df = pd.DataFrame(review_keyword_list)
        review_keyword_df.to_sql(name='amz_review_keyword',con=db_connection, if_exists='append', index=False)
    except:
        pass

    try:
        body_content_list = list()
        body_content_dict = dict()
        body_content_dict['url'] = url
        body_content_dict["product_key"] = asin
        body_content_dict["create_date"] = create_date
        body_content_dict["body_content"] = str(body_content)
        body_content_list.append(body_content_dict)
        
        body_content_df = pd.DataFrame(body_content_list)
        body_content_df.to_sql(name='amz_body_content',con=db_connection, if_exists='append', index=False)
    except:
        pass

# RatingStar
def getRatingStar(product_info, soup):
    try:
        ratingStar_html = soup.find('span',{'data-hook':'rating-out-of-text'})
        ratingStar = ratingStar_html.get_text().split(' ')[0].strip()
        product_info["totalRatingStar"] = float(ratingStar)
    except:
        product_info["totalRatingStar"] = None

# ReviewCount
def getReviewCount(product_info, soup):
    try:
        totalReviewCount_html = soup.find('div',{'data-hook':'total-review-count'}).find('span')
        totalReviewCount_str = totalReviewCount_html.get_text().strip()
        totalReviewCount_number = re.sub(r'[^0-9]', '', totalReviewCount_str)
        product_info["totalReviewCount"] = int(totalReviewCount_number)
    except:
        product_info["totalReviewCount"] = None

# EachStarPercent
def getEachStarPercent(product_info, soup):
    try:
        eachStarPercent_list = list()
        eachStarPercent_html = soup.find('table',{'id':'histogramTable'}).find('tbody').find('tr').find('td',{'class':'a-text-right'}).find('span',{'class':'a-size-base'}).find_all('a')
        for eachStarPercent_str in eachStarPercent_html:
            eachStarPercent_list.append(eachStarPercent_str.get_text().strip())
        star_index = 5
        for eachStarPercent_str in eachStarPercent_list:
            eachStarPercent_number = re.sub(r'[^0-9]', '', eachStarPercent_str)
            product_info[f'{star_index}star']= int(eachStarPercent_number)
            star_index -= 1
    except:
        product_info['5star'] = None
        product_info['4star'] = None
        product_info['3star'] = None
        product_info['2star'] = None
        product_info['1star'] = None

# "Product Description" / "From the manufacturer"
def getDescription_text(body_content,soup):
    try:
        description_text = soup.find('div',{'class':'aplus-v2','class':'desktop','class':'celwidget'}).find_all('div',{'class':'celwidget', 'class':'aplus-module'})
        for list_index in description_text:
            body_content.append(list_index.get_text().strip().replace("  ","").replace("\n",""))
    except:
        pass
    try:
        description_text_2 = soup.find('div',{'id':'productDescription'})
        body_content.append(description_text_2.get_text().strip().replace("  ","").replace("\n",""))
    except:
        pass

    body_content = str(list(filter(None, body_content)))

# Category
def getCategory(product_info, soup):
    index = 1
    categories_str = soup.find_all('a',{'class':'a-color-tertiary'})
    for category in categories_str:
        product_info[f'Level_{index}'] = category.get_text().strip().replace("'","`")
        index += 1

#  Name & Price
def getNameAndPrice(product_info, soup):
    productName_str = soup.find('span',{'id':'productTitle'}).get_text().strip().replace("'","`")
    
    try:
        product_info["Product_name"] = productName_str
    except:
        product_info["Product_name"] = None

    try:
        productPrice = soup.find('span',{'class':'a-offscreen'}).get_text().strip().lstrip("$")
        product_info["Product_price"] = float(productPrice)
    except:
        product_info["Product_price"] = None

# Featurebullets
def getFeaturebullets(product_feature, soup):
    featurePage = soup.find('div',{'id':'feature-bullets'}).find('ul',{'class':'a-unordered-list','class':'a-vertical'})
    featurePage_elements = featurePage.select("li > span")
    index = 1
    for feature in featurePage_elements:
        product_feature[f'Feature_{index}'] = feature.get_text().strip().replace("'","`")
        index += 1

# AttrRatingStar
def getAttrRatingStar(feature_rating, soup):
    attrRatingTable = soup.find('div',{'id':'cr-dp-summarization-attributes'}).find_all("div",{"data-hook":"cr-summarization-attribute"})
    for attrRatingList in attrRatingTable:
        attrRatingTitle = attrRatingList.find('span',{'class':'a-size-base','class':'a-color-base'}).get_text().strip().replace("'","`")
        attrRatingScore = attrRatingList.find('span',{'class':'a-size-base','class':'a-color-tertiary'}).get_text().strip()
        feature_rating[attrRatingTitle] = float(attrRatingScore)

def getReviewKeyword(review_keyword, soup):
    reviewKeywords = soup.find('div',{'id':'cr-lighthut-1-'})
    reviewKeyword_html = reviewKeywords.find_all('span',{'class':'cr-lighthouse-term'})
    index = 1
    for reviewKeyword_str in reviewKeyword_html:
        reviewKeyword = reviewKeyword_str.get_text().strip()
        review_keyword[f"ReviewKeyword_{index}"] = reviewKeyword
        index += 1
        
def getDetailList(product_detail, soup):
    detailListSource = soup.find('div',{'id':'detailBullets_feature_div'}).find('span',{'class':'a-list-item'}).find_all('span')
    detailTableDict_key_list = list()
    detailTableDict_value_list = list()
    for spanList in detailListSource:
        if detailListSource.index(spanList) % 2 == 0:
            detailTableDict_key_list.append(spanList.get_text().replace("  ","").replace("\n","").replace(":","").replace("‏","").replace("‎","").strip())
        else:
            detailTableDict_value_list.append(spanList.get_text().replace("  ","").replace("\n","").strip())
    for key, value in zip(detailTableDict_key_list, detailTableDict_value_list):
        product_detail[key]=value

def getDetailTable(product_detail, soup):
    detailTableSource = soup.find('table',{'class':'a-keyvalue', 'class':'prodDetTable'}).find('tbody').find_all('tr')
    # print(detailTableSource)
    for index in detailTableSource:
        title = index.find("th").get_text().strip().replace("'","`")
        description = index.find("td").get_text().replace("  ","").replace("\n","").strip().replace("'","`")
        product_detail[title] = description

# 상품 정보페이지로 넘어가는 함수
def goToDetailPage(url,asin,product_order,product_detail,product_feature,product_info,reviewDict,feature_rating,review_keyword,body_content):
    # 사이트 스크롤해서 html 불러오기
    for i in [10,5,10/3,10/4,2,10/6,10/7,10/8,10/9,1]: # 페이지 10등분
        driver.execute_script(f"window.scrollTo(0,document.body.scrollHeight/{i})")
        time.sleep(0.5)

    # BeautifulSoup
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')
    try:
        # Category
        getCategory(product_info, soup)
    except:
        # print(1)
        pass
    try:
        # Name & Price
        getNameAndPrice(product_info, soup)
    except:
        # print(2)
        pass  
    try:
        # DetailTable
        getDetailList(product_detail, soup)
    except:
        # print(3)
        pass
    try:
        # DetailTableSource
        getDetailTable(product_detail, soup)
    except:
        # print(4)
        pass
    try:
        # Featurebullets
        getFeaturebullets(product_feature, soup)
    except:
        # print(5)
        pass
    try:
        # AttrRatingStar
        getAttrRatingStar(feature_rating, soup)
    except:
        # print(6)
        pass
    try:
        # ReviewKeyword
        getReviewKeyword(review_keyword, soup)
    except:
        # print(7)
        pass
    try:
        # RatingStar
        getRatingStar(product_info, soup)
    except:
        # print(8)
        pass
    try:
        # ReviewCount
        getReviewCount(product_info, soup)
    except:
        # print(9)
        pass
    try:    
        # EachStarPercent
        getEachStarPercent(product_info, soup)
    except:
        # print(10)
        pass
    try:
        # "Product Description" / "From the manufacturer"
        getDescription_text(body_content,soup)
    except:
        # print(11)
        pass

    insert_db(url,asin,product_order,product_info,product_detail,product_feature,feature_rating,review_keyword,body_content)


if __name__ =='__main__':
    """
        input_list && web driver 설정
    """
    URL_ADDRESS = "https://www.amazon.com/"
    URL_LIST = open("url_input.txt", "r", encoding="utf-8").read().splitlines()

    options = Options()
    driver = webdriver.Firefox(options=options,executable_path="./geckodriver.exe")
    driver.set_window_size(1920, 1080)

    """
        zip코드 입력하는 함수
    """
    # ziasin_code (Selenium)
    driver.get(URL_ADDRESS)
    time.sleep(1)
    driver.find_element("id","nav-global-location-popover-link").click()
    time.sleep(1)

    driver.find_element("id","GLUXZipUpdateInput").send_keys("98101")
    driver.find_element("id","GLUXZipUpdate").click()
    time.sleep(1)

    """
        URL별 크롤링
    """ 
    for url in URL_LIST:
        print(url)
        asin_list = list()
        product_order = 1
        # 카테고리 페이즈 접속
        driver.get(url)
        while(True):
            # 사이트 스크롤해서 html 불러오기
            for i in [10,5,10/3,10/4,2,10/6,10/7,10/7,10/8,10/8]:
                driver.execute_script(f"window.scrollTo(0,document.body.scrollHeight/{i})")
                time.sleep(1)
            # Selenium && BeautifulSoup
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')

            # 상품을 지정하는 div를 list형식으로 저장
            contents = soup.find_all('div',{'class':'zg-grid-general-faceout'})

            # 상품의 URL_suffix와 상품의 고유 key값을 dict로 묶어서 list로 저장
            content_URLkey_list = list()
            for index in contents:
                content_URLkey_dict = dict()
                asin_list.append(index.find('div',{'class':'p13n-sc-uncoverable-faceout'})['id'].strip())
                
            # 다음페이지로 넘기는 함수
            nextPageURL = soup.find('li',{'class':'a-last'}).find('a')
            if nextPageURL != None:
                URL = URL_ADDRESS + str(nextPageURL['href'].strip())
                driver.get(URL)
            else:
                print("[COMPLETE]")
                break

        """
            URL내의 asin코드별 크롤링
        """
        for asin in asin_list:
            URL_SUFFIX = "/dp/"+ asin
            # amazon서버 불러오기
            try:
                driver.get(URL_ADDRESS + URL_SUFFIX)
            except:
                driver.refresh()

            driver.implicitly_wait(10)
            time.sleep(0.5)
            product_detail = dict()
            product_feature =dict()
            product_info = dict()
            reviewDict = dict()
            feature_rating = dict()
            review_keyword = dict()
            body_content = list()
            # try:
            goToDetailPage(url,asin,product_order,product_detail,product_feature,product_info,reviewDict,feature_rating,review_keyword,body_content)
            product_order += 1
            # except:
            #     pass
        db.commit()
        db.close()