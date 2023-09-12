import scrapy
from scrapy.crawler import CrawlerProcess
import requests
import zipfile
import mysql.connector
from bs4 import BeautifulSoup
import json
import os
import re
from urllib.request import urlretrieve
import shutil
import mysql.connector
import time

def download_image(url, save_path):
    response = requests.get(url)
    response.raise_for_status()  # Check for any errors

    with open(save_path, 'wb') as file:
        file.write(response.content)

    print(f"The image has been downloaded and saved as {save_path}")

def create_zip(file_list, zip_name):
    with zipfile.ZipFile(zip_name, 'w') as zip_file:
        for file in file_list:
            zip_file.write(file)
def save_image_to_database(image_path):
    # Read the image file as binary data
    with open(image_path, 'rb') as file:
        image_data = file.read()
        return image_data


def connection_db():
    db_host = "sql353.your-server.de"
    db_user = "finyourd_1"
    db_password = "iXXXskuJ7V3As2ix"
    db_name = "finyou_rd"

    # Connect to the database
    db = mysql.connector.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name
    )
    cursor = db.cursor()
    return [cursor,db]
def download_pdf(url, save_path,headers=None):
    if headers!=None:
        response = requests.get(url,headers=headers)
    else:
        response = requests.get(url)

    with open(save_path, 'wb') as file:
        file.write(response.content)

# Get a cursor object to interact with the database



class Scraper(scrapy.Spider):
    name = "Scraper"


    def start_requests(self):
        website_links = open('website-links.txt','r')
        website_links_read = [v.strip() for v in website_links.readlines()]
        for links in website_links_read:
            if 'www.ilb.de' in links:
                headers = {
                    'Accept': '*/*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Connection': 'keep-alive',
                    # 'Cookie': 'klaro=%7B%22klaro%22%3Atrue%2C%22apache%22%3Atrue%2C%22friendlyCaptcha%22%3Atrue%2C%22googleAnalytics%22%3Atrue%2C%22youtube%22%3Atrue%2C%22twitter%22%3Atrue%2C%22googleMaps%22%3Atrue%7D; JSESSIONID=207581ABEC59CB40FED3A3F730044E4F; _gid=GA1.2.124465491.1686980459; _gat_UA-110967658-2=1; _ga=GA1.1.1269499403.1686980459; _ga_5NCZFPR9D0=GS1.1.1686980458.1.1.1686981967.0.0.0',
                    'Referer': 'https://www.ilb.de/de/service/foerderfinder/foerderfinder.html?zielgruppe=Private%2520Unternehmen',
                    'Sec-Fetch-Dest': 'empty',
                    'Sec-Fetch-Mode': 'cors',
                    'Sec-Fetch-Site': 'same-origin',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                    'X-Requested-With': 'XMLHttpRequest',
                    'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                }
                yield scrapy.Request(url=links,headers=headers,callback=self.ilb_parser)
            if 'www.ib-sachsen-anhalt'  in links:
                headers = {
                    'authority': 'www.ib-sachsen-anhalt.de',
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                    'accept-language': 'en-US,en;q=0.9',
                    'cache-control': 'max-age=0',
                    # 'cookie': '_pk_id.1.76e8=d90823ea36d1b4bb.1687190564.; _pk_ses.1.76e8=1; CookieConsent={stamp:%27UumF4k/KL0UGyS/UuUxY4IsGrDM4jzshAL+7K/kpm4QEd1sH0veBfA==%27%2Cnecessary:true%2Cpreferences:true%2Cstatistics:true%2Cmarketing:true%2Cmethod:%27explicit%27%2Cver:1%2Cutc:1687287902728%2Cregion:%27pk%27}',
                    'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'document',
                    'sec-fetch-mode': 'navigate',
                    'sec-fetch-site': 'none',
                    'sec-fetch-user': '?1',
                    'upgrade-insecure-requests': '1',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
                }
                yield scrapy.Request(url=links, headers=headers,callback=self.parse_ib_sachsen)
            if 'www.berlin.de' in links:
                yield scrapy.Request(url=links,callback=self.transparenzdatenbank_parse)
            if 'www.ibb.de' in links:
                yield scrapy.Request(url=links, callback=self.ibb_parse)


    def ilb_parser(self, response):
        headers = {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            # 'Cookie': 'klaro=%7B%22klaro%22%3Atrue%2C%22apache%22%3Atrue%2C%22friendlyCaptcha%22%3Atrue%2C%22googleAnalytics%22%3Atrue%2C%22youtube%22%3Atrue%2C%22twitter%22%3Atrue%2C%22googleMaps%22%3Atrue%7D; JSESSIONID=207581ABEC59CB40FED3A3F730044E4F; _gid=GA1.2.124465491.1686980459; _gat_UA-110967658-2=1; _ga=GA1.1.1269499403.1686980459; _ga_5NCZFPR9D0=GS1.1.1686980458.1.1.1686981967.0.0.0',
            'Referer': 'https://www.ilb.de/de/service/foerderfinder/foerderfinder.html?zielgruppe=Private%2520Unternehmen',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest',
            'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }
        if 'https://www.ilb.de' in response.url:
            group = response.url.split('zielgruppe=')[-1].replace('%2520', "%20")

            try:
                pagination_pages =int(int(response.css('div.pagination-pages ::text').get().split('von')[-1].strip())/5)
            except:
                pagination_pages = 1
            response_json = requests.get(
                f'https://www.ilb.de/sp/sp/subsidy_finder?start=1&rows=5&q=*:*&f[find_zielgruppe]={group}&_=1686981966953',
                headers=headers,
            )
            json_values = json.loads(response_json.text)
            try:
                for loop in json_values['docs']:
                    nex_page_url = loop['url']
                    yield scrapy.Request(url='https://www.ilb.de' + nex_page_url, callback=self.ilb_grant,
                                         headers=headers,meta ={'target':group.replace('%20'," ").replace('%C3%A4',"ä").replace('%C3%96',"Ö")})
            except:
                pass


            for pagination_loop in range(1,pagination_pages):

                response_json = requests.get(
                    f'https://www.ilb.de/sp/sp/subsidy_finder?start={pagination_loop*5}&rows=5&q=*:*&f[find_zielgruppe]={group}&_=1686981966953',
                    headers=headers,
                )
                json_values = json.loads(response_json.text)
                try:
                    for loop in json_values['docs']:
                        nex_page_url = loop['url']
                        yield scrapy.Request(url='https://www.ilb.de'+nex_page_url,callback=self.ilb_grant,headers=headers,meta ={'target':group})
                except:
                    pass

    def ilb_grant(self,response):
        returntype  = connection_db()
        cursor = returntype[0]
        db = returntype[1]

        item = dict()
        try:
            item['grant_name'] = response.css('div.intro h1 ::text').get()
        except:
            item['grant_name'] = ""
        try:
            item['grant_url'] = response.url
        except:
            item['grant_url'] = ""

        item['bank_name'] = 'Investitionsbank des Landes Brandenburg'
        item['target'] = response.meta['target'].replace('%20'," ")

        try:
            item['introduction'] = ' '.join(response.css('div.group.group--fp.group--outline ::text').extract()).strip().replace('\t'," ").replace('\n ',"").replace("  "," ")
        except:
            item['introduction'] = ""

        soup = BeautifulSoup(response.text, 'html.parser')
        main_content = soup.select_one("div#scrollNavItems").prettify()
        html = main_content

        try:
            item['html'] = soup.select_one("div#scrollNavItems").prettify()
        except:
            item['html'] = ""
        try:
            item['text'] = "\n".join(scrapy.Selector(text=html).css('::text').getall()).strip().replace('\n',' ')
        except:
            item['text'] = ""



        for facts in response.css('div.group.group--fp'):
            if ' '.join(facts.css('div.group__title ::text').extract()).strip() == 'Aktuelle Meldungen':
                try:
                    item['info_box'] = ' '.join(facts.css('::text').extract()).strip().replace('\t'," ").replace('\n ',"").replace("  "," ")
                except:
                    item['info_box'] = ""
            if ' '.join(facts.css('div.group__title ::text').extract()).strip() == 'Ziel des Programms':
                try:
                    item['short_fact'] = ' '.join(facts.css('::text').extract()).strip().replace('\t', " ").replace('\n ',"").replace("  ", " ")
                except:
                    item['short_fact'] = ""
                a=1
            if ' '.join(facts.css('div.group__title ::text').extract()).strip() == 'Wer, was und wie wird gefördert':
                all_data = ' '.join(facts.css('::text').extract()).strip().replace('\t'," ").replace('\n ',"").replace("  "," ")
                # for who
                all_headings = facts.css('p strong ::text').extract()
                for index,heading_loop in enumerate(all_headings):
                    if heading_loop == 'Wer wird gefördert?':
                        try:
                            item['who'] = all_data.split(heading_loop)[-1].split(all_headings[index+1])[0].strip()
                        except:
                            item['who'] = all_data.split(heading_loop)[-1].split(heading_loop)[-1].strip()

                    if heading_loop == 'Was wird gefördert?':
                        try:
                            item['what'] = all_data.split(heading_loop)[-1].split(all_headings[index+1])[0].strip()
                        except:
                            item['what'] = all_data.split(heading_loop)[-1].split(heading_loop)[-1].strip()
                    if heading_loop == 'Wie wird gefördert?':
                        try:
                            item['how'] = all_data.split(heading_loop)[-1].split(all_headings[index+1])[0].strip()
                        except:
                            item['how'] = all_data.split(heading_loop)[-1].split(heading_loop)[-1].strip()

            if ' '.join(facts.css('div.group__title ::text').extract()).strip() == 'Ablauf / Verfahren':
                try:
                    item['application'] = ' '.join(facts.css('::text').extract()).strip().replace('\t'," ").replace('\n ',"").replace("  "," ")
                except:
                    item['application'] = ""
                a=11
            if ' '.join(facts.css('div.group__title ::text').extract()).strip() == 'Was ist noch zu beachten':
                try:
                    item['what_else'] = ' '.join(facts.css('::text').extract()).strip().replace('\t'," ").replace('\n ',"").replace("  "," ")
                except:
                    item['what_else'] = ""

        # pdfs
        documents = response.css('p.item a[rel="external"] ::attr(href)').extract()
        program_dir = item['grant_name']
        program_dir = program_dir.replace('/',"")
        print(program_dir)
        try:
            os.makedirs(program_dir, exist_ok=True)
        except:
            a=1
            pass
        item['directory_name'] = program_dir
        file_names = []
        for loop in documents:
            doc_name = loop.split('/')[-1]
            doc_path = os.path.join(program_dir, doc_name)
            # urlretrieve('https://www.ilb.de', doc_path)
            try:
                download_pdf('https://www.ilb.de'+loop,doc_path)
                file_names.append(doc_name)
            except:
                try:
                    download_pdf('https://www.ilb.de'+loop,os.getcwd()+'/'+program_dir+"/"+doc_name[-70:])
                    file_names.append(doc_name)
                except:
                    a=1
                    pass

        item['file_names'] =  ','.join(file_names)

        zip_path = 'archive' + ".zip"
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as archive:
            for root, dirs, files in os.walk(program_dir):
                for file in files:
                    archive.write(os.path.join(root, file), file)
        # remove the dir
        shutil.rmtree(program_dir)
        time.sleep(3)

        with open(zip_path, 'rb') as zip_file:
            zip_data = zip_file.read()

        os.remove(zip_path)

        for fills in ["grant_name","introduction","info_box","short_facts","who","what","how","conditions","application","what_else","text"]:
            if fills  not in list(item.keys()):
                item[fills] = ""
        read_query = f"SELECT grant_url FROM finyou_rd.ilb_grants where grant_url='{item['grant_url']}'"

        # Execute the select query
        try:
            cursor.execute(read_query)
        except:
            returntype  = connection_db()
            cursor = returntype[0]
            db = returntype[1]
            cursor.execute(read_query)
            pass

        # Fetch all rows from the result set
        result = cursor.fetchall()

        if len(result) < 1:
            query = """INSERT INTO finyou_rd.ilb_grants (
            grant_url,
            bank,
            target,
            html,
            text,
            grant_name,
            introduction,
            info_box,
            short_facts,
            who,
            what,
            how,
            conditions,
            application,
            what_else,
            pdf_zips,
            file_names          
            ) VALUES ( %s,%s, %s,%s, %s, %s, %s,%s, %s, %s, %s, %s,%s,%s,%s,%s,%s)
                                    """
            cursor.execute(query, (item['grant_url'],item['bank_name'],item['target'],item['html'],item['text'],item['grant_name'],item['introduction'],item['info_box'],item['short_facts'],item['who'],item['what'],item['how'],item['conditions'],item['application'],item['what_else'],zip_data,item['file_names']))

            # Commit the changes to the database
            db.commit()
            a=1
        else:
            # Define the update query
            query = """UPDATE finyou_rd.ilb_grants SET
                        bank = %s,
                        target = %s,
                        html = %s,
                        text = %s,
                        grant_name = %s,
                        introduction = %s,
                        info_box = %s,
                        short_facts = %s,
                        who = %s,
                        what = %s,
                        how = %s,
                        conditions = %s,
                        application = %s,
                        what_else = %s,
                        pdf_zips = %s,
                        file_names = %s                        
                      WHERE grant_url = %s"""
            # Define the values to be updated
            values = (item['bank_name'],item['target'],item['html'], item['text'], item['grant_name'],
                item['introduction'], item['info_box'], item['short_facts'], item['who'], item['what'], item['how'],
                item['conditions'], item['application'], item['what_else'], zip_data, item['file_names'],item['grant_url']
            )

            # Execute the update query with the values
            cursor.execute(query, values)

            # Commit the changes to the database
            db.commit()


    def parse_ib_sachsen(self,response):
        headers = {
            'authority': 'www.ib-sachsen-anhalt.de',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'max-age=0',
            # 'cookie': '_pk_id.1.76e8=d90823ea36d1b4bb.1687190564.; _pk_ses.1.76e8=1; CookieConsent={stamp:%27UumF4k/KL0UGyS/UuUxY4IsGrDM4jzshAL+7K/kpm4QEd1sH0veBfA==%27%2Cnecessary:true%2Cpreferences:true%2Cstatistics:true%2Cmarketing:true%2Cmethod:%27explicit%27%2Cver:1%2Cutc:1687287902728%2Cregion:%27pk%27}',
            'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        }
        for looop in response.css(
                'ul.mobileMenu__items.background-white ul.mobileMenu__items.background-white li a ::attr(href)').extract():
            yield scrapy.Request(url='https://www.ib-sachsen-anhalt.de' + looop, headers=headers,callback=self.ib_sachsen_grant)
    def ib_sachsen_grant(self,response):
        headers = {
            'authority': 'www.ib-sachsen-anhalt.de',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'max-age=0',
            # 'cookie': '_pk_id.1.76e8=d90823ea36d1b4bb.1687190564.; _pk_ses.1.76e8=1; CookieConsent={stamp:%27UumF4k/KL0UGyS/UuUxY4IsGrDM4jzshAL+7K/kpm4QEd1sH0veBfA==%27%2Cnecessary:true%2Cpreferences:true%2Cstatistics:true%2Cmarketing:true%2Cmethod:%27explicit%27%2Cver:1%2Cutc:1687287902728%2Cregion:%27pk%27}',
            'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        }
        item = dict()
        item['grant_url']  = response.url
        try:
            item['grant_name'] = ' '.join(response.css('div.singlepost_headline ::text').extract()).strip().replace('\n',"").replace('\t',"")
        except:
            item['grant_name'] = ''

        item['bank_name'] = 'Sachsen-Anhalt'
        try:
            item['target'] = response.url.split('/')[-3].strip()
        except:
            item['target'] = ""

        soup = BeautifulSoup(response.text, 'html.parser')
        html = soup.select_one("div.column.medium-6.large-8").prettify()

        try:
            item['html'] = html
        except:
            item['html'] = ""
        try:
            item['text'] = "\n".join(scrapy.Selector(text=html).css('::text').getall()).strip().replace('\n', ' ').replace("            "," ").replace("    "," ")
        except:
            item['text'] = ""


        documents = response.css('div.downloads_accordion a ::attr(href)').extract()
        program_dir = item['grant_name']
        program_dir = program_dir.replace('/', "").replace('|',"").replace(' ',"-")
        print(program_dir)
        try:
            os.makedirs(program_dir, exist_ok=True)
        except:
            a = 1
            pass
        item['directory_name'] = program_dir
        file_names = []
        for loop in documents:
            doc_name = loop.split('/')[-1]
            doc_path = os.path.join(program_dir, doc_name)
            try:
                download_pdf('https://www.ib-sachsen-anhalt.de' + loop, doc_path,headers)
                file_names.append(doc_name)
            except:
                try:
                    download_pdf('https://www.ib-sachsen-anhalt.de' + loop, os.getcwd() + '/' + program_dir + "/" + doc_name[-70:],headers)
                    file_names.append(doc_name)
                except:
                    a = 1
                    pass

        item['file_names'] = ','.join(file_names)

        zip_path = 'archive' + ".zip"
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as archive:
            for root, dirs, files in os.walk(program_dir):
                for file in files:
                    archive.write(os.path.join(root, file), file)
        # remove the dir
        shutil.rmtree(program_dir)
        time.sleep(3)

        with open(zip_path, 'rb') as zip_file:
            zip_data = zip_file.read()

        os.remove(zip_path)


        connection_values = connection_db()
        cursor = connection_values[0]
        db = connection_values[1]
        read_query = f"SELECT grant_url FROM finyou_rd.ib_sachsen where grant_url='{item['grant_url']}'"

        # Execute the select query
        try:
            cursor.execute(read_query)
        except:
            returntype = connection_db()
            cursor = returntype[0]
            db = returntype[1]
            cursor.execute(read_query)
            pass
        result = cursor.fetchall()

        if len(result) < 1:
            query = """INSERT INTO finyou_rd.ib_sachsen (
            grant_url,
            bank,
            target,
            html,
            text,
            grant_name,            
            pdf_zips,
            file_names          
            ) VALUES ( %s,%s, %s,%s, %s, %s, %s,%s)
                                    """
            cursor.execute(query, (item['grant_url'],item['bank_name'],item['target'],item['html'],item['text'],item['grant_name'],zip_data,item['file_names']))

            # Commit the changes to the database
            db.commit()
            cursor.close()
            db.close()
            a=1
        else:
            # Define the update query
            query = """UPDATE finyou_rd.ib_sachsen SET
                        bank = %s,
                        target = %s,
                        html = %s,
                        text = %s,
                        grant_name = %s,
                        pdf_zips = %s,
                        file_names = %s                        
                      WHERE grant_url = %s"""
            # Define the values to be updated
            values = (item['bank_name'],item['target'],item['html'], item['text'], item['grant_name'],
                 zip_data, item['file_names'],item['grant_url']
            )

            # Execute the update query with the values
            cursor.execute(query, values)

            # Commit the changes to the database
            db.commit()
            cursor.close()
            db.close()

    def transparenzdatenbank_parse(self,response):
        for company in response.css('.modul-searchitems > div'):

            info = {}
            info['company_name'] = company.css('h2::text').get()

            tmp = company.css('div').get()
            start = tmp.find('</strong>') + len('</strong>')
            end = tmp.find('<div>', start)
            info['hrb_number'] = tmp[start:end].strip()

            print('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx  ', info['hrb_number'], ' xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx')
            link = company.css('h3.title a::attr(href)').get()

            update = company.css('div.well::text').extract()
            if update:
                update = update[1].strip()
                info['last_update'] = update

            if len(company.css('.img-logo')) > 0:
                info['transparent'] = True
            else:
                info['transparent'] = False

            # remove if you go for all companies
            # if info['hrb_number'] == 'hrb_126923':
            final_link = 'https://www.berlin.de/buergeraktiv/informieren/transparenz/transparenzdatenbank/' + link

            yield scrapy.Request(url=final_link, callback=self.transparenzdatenbank_grant, meta=info)

        nextpage = 'https://www.berlin.de/buergeraktiv/informieren/transparenz/transparenzdatenbank/' + response.css(
            'li.pager-item-next a::attr(href)').get()[1:]
        if nextpage:
            yield scrapy.Request(url=nextpage, callback=self.transparenzdatenbank_parse)

    def transparenzdatenbank_grant(self,response):
        connection_values = connection_db()
        cursor = connection_values[0]
        db = connection_values[1]
        info = response.meta
        info['url'] = response.url
        try:
            a = response.css('h4.subtitle::text').get()
            try:
                info['Bezirk']= [v.strip() for v in a.split(' | ')][0]
                info['Gesellschaftsform'] = [v.strip() for v in a.split(' | ')][0]
            except:
                info['Bezirk'] = ""
                info['Gesellschaftsform'] = ""


            for text_loop in response.css('.list--tablelist div.cell'):
                try:
                    text = text_loop.css('p::text').get().strip().split(':')
                except:
                    a = 1
                    continue
                if len(text) > 1 and text[-1] != "":
                    key = text[0].strip()
                    value = text[1].strip()
                    if key == 'Größenklasse':
                        text = ' '.join(text_loop.css('p ::text').extract()).strip().split(':')
                        key = text[0].strip()
                        value = text[1].strip()
                    if 'Tarifgebunden' in key:
                        try:
                            state_value = key.split('(')[-1].replace(')', "")
                            info['Tarifgebunden'] = state_value
                        except:
                            state_value = ""
                            info['Tarifgebunden'] = state_value

                        key = 'Tarifgebunden_text'

                    info[key.replace(' ', '_').replace('(', "").replace(")", "")] = value
                if len(text) > 1 and text[-1] == "":
                    key = text[0]
                    if key == 'Hauptzuwendungsgeber':
                        value = '\n'.join([v.strip().split(':')[-1] for v in text_loop.css('ul li ::text').extract()])
                        info['Name_Sitz'] = value
                        continue
                    if key == 'Entscheidungsträger':
                        value = '\n'.join([v.strip().split(':')[-1] for v in text_loop.css('ul li ::text').extract()])
                        info['Name_Funktion'] = value
                        continue
                    if key == 'Angaben zur Personalstruktur':
                        for again_loop in text_loop.css('ul li'):
                            key = again_loop.css(' ::text').get().strip()[:-1]
                            if key != 'Datei':
                                value = again_loop.css('span ::text').get().strip()
                                info['Angaben_zur_Personalstruktur_' + key.replace(" ", "_")] = value
                        continue
                    if key == 'Mittelherkunft':
                        for again_loop in text_loop.css('ul li'):
                            key = again_loop.css(' ::text').get().strip()[:-1]
                            if key != 'Datei':
                                try:
                                    value = again_loop.css('span ::text').get().strip()
                                except:
                                    value = ""

                                info['Mittelherkunft_' + key.replace(" ", "_")] = value
                            if key == 'Datei':
                                for index, pdf_loop in enumerate(text_loop.css('ul li a.link--download')):
                                    pdf_url = pdf_loop.css(' ::attr(href)').get()
                                    download_image(pdf_url, os.getcwd() + '/document.pdf')
                                    info[f'Mittelherkunft_pdf_{index + 1}'] = save_image_to_database(
                                        os.getcwd() + '/document.pdf')
                                    os.remove(os.getcwd() + '/document.pdf')
                                    time.sleep(3)

                        continue
                    if key == 'Satzung oder Gesellschaftervertrag':
                        for index, pdf_loop in enumerate(text_loop.css('ul li a.link--download')):
                            pdf_url = pdf_loop.css(' ::attr(href)').get()
                            download_image(pdf_url, os.getcwd() + '/document.pdf')
                            info[f'Satzung_oder_Gesellschaftervertrag_pdf_{index + 1}'] = save_image_to_database(
                                os.getcwd() + '/document.pdf')
                            os.remove(os.getcwd() + '/document.pdf')
                            time.sleep(3)

                    if key == 'Tätigkeitsbericht':
                        for index, pdf_loop in enumerate(text_loop.css('ul li a.link--download')):
                            pdf_url = pdf_loop.css(' ::attr(href)').get()
                            download_image(pdf_url, os.getcwd() + '/document.pdf')
                            info[f'Tätigkeitsbericht_pdf_{index + 1}'] = save_image_to_database(
                                os.getcwd() + '/document.pdf')
                            os.remove(os.getcwd() + '/document.pdf')
                            time.sleep(3)
                    if key == 'Mittelverwendung':
                        for index, pdf_loop in enumerate(text_loop.css('ul li a.link--download')):
                            pdf_url = pdf_loop.css(' ::attr(href)').get()
                            download_image(pdf_url, os.getcwd() + '/document.pdf')
                            info[f'Mittelverwendung_pdf_{index + 1}'] = save_image_to_database(
                                os.getcwd() + '/document.pdf')
                            os.remove(os.getcwd() + '/document.pdf')
                            time.sleep(3)




                    else:
                        value = ""
                        info[key] = value

            link = response.css('a.more::attr(href)').get()

            info["street"] = response.css('.address.loc span.street::text').get()
            try:
                zipcode = response.css('.address.loc span.city::text').get().split(' ')
                info['code'] = zipcode[0]
            except:
                zipcode = ""
                info['code'] = ""

            try:
                info['city'] = response.css('.address.loc span.city::text').get().replace(zipcode[0], '').strip()
            except:
                info['city'] = ""

            try:
                info["phone"] = response.css('.tel::text').get().replace('Telefon:', '').strip()
            except:
                info["phone"] = ""

            try:
                info["fax"] = response.css('.fax::text').get().replace('Fax:', '').strip()
            except:
                info["fax"] = ""

            try:
                info["email"] = response.css('.email a::text').get().strip()
            except:
                info["email"] = ""
            try:
                info['title'] = response.css('h2.title ::text').get().strip()
            except:
                info['title'] = ""

            info['description'] = response.css('div.html5-section.body.block p ::text').get()
            for loop in response.css('div.html5-section.body'):
                if len(loop.css('h3').extract()) > 0:
                    info['organization_presentation'] = loop.css('h3 ::text').get() + '\n' + loop.css('p ::text').get()
            try:
                info["website"] = response.css('a[title="Link zur Webseite"]::attr(href)').get()
            except:
                info["website"] = ""

            info['logo'] = response.css('img[title="Logo der Organisation"] ::attr(src)').get()
            if info['logo'] != None:
                download_image(info['logo'], os.getcwd() + '/image.jpg')
                info['image_data_binary'] = save_image_to_database(os.getcwd() + '/image.jpg')
                os.remove(os.getcwd() + '/image.jpg')
                time.sleep(3)

            read_query = f"SELECT * FROM finyou_rd.transparenzregister where url='{info['url']}'"

            # Execute the select query
            try:
                cursor.execute(read_query)
            except:
                returntype = connection_db()
                cursor = returntype[0]
                db = returntype[1]
                cursor.execute(read_query)
                pass
            result = cursor.fetchall()
            if len(result) < 1:

                query = """
                            INSERT INTO finyou_rd.transparenzregister (url,company_name, hrb_number, last_update, transparent, depth, download_timeout,
                                download_slot, download_latency, Bezirk, Gesellschaftsform, Stiftung, Fördernd,
                                Verbandsmitgliedschaften, Behindertengerecht, Zielgruppen, Bereich, Gemeinnützigkeit,
                                Datum_Gemeinnützigkeitsbescheinigung, Hauptsitz, Gründungsjahr, Name_Funktion,
                                Tarifgebunden_Ja, Angaben_zur_Personalstruktur_Berichtsjahr,
                                Angaben_zur_Personalstruktur_Anzahl_Hauptamtliche,
                                Angaben_zur_Personalstruktur_Anzahl_Honorarkräfte, Angaben_zur_Personalstruktur_Anzahl_GfB,
                                Angaben_zur_Personalstruktur_Anzahl_Freiwilligendienstleistende,
                                Angaben_zur_Personalstruktur_Anzahl_ehrenamtliche_Mitarbeiter, Mittelherkunft_Berichtsjahr,
                                Mittelherkunft_Gesamteinnahmen, Mittelherkunft_davon, Mittelherkunft_öffentliche_Zuwendungen,
                                Mittelherkunft_Spenden_und_Mitgliedsbeiträge, Name_Sitz, street, code, city, phone, fax,
                                email, title, description, organization_presentation, website,Größenklasse,Anzahl_der_Beschäftigen,Tarifgebunden,Tarifgebunden_text,logo,satzung_oder_Gesellschaftervertrag_pdf_1,satzung_oder_Gesellschaftervertrag_pdf_2,
                                Tätigkeitsbericht_pdf_1,Mittelverwendung_pdf_1,Mittelherkunft_pdf_1
                            ) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,
                            %s, %s, %s, %s  ,%s ,%s,%s,%s,%s,%s,%s,%s,%s,%s
                            )
                        """
                for inser_temp in ['company_name', 'hrb_number', 'last_update', 'transparent', 'depth', 'download_timeout',
                                   'download_slot', 'download_latency', 'Bezirk', 'Gesellschaftsform', 'Stiftung',
                                   'Fördernd', 'Verbandsmitgliedschaften', 'Behindertengerecht', 'Zielgruppen', 'Bereich',
                                   'Gemeinnützigkeit', 'Datum_Gemeinnützigkeitsbescheinigung', 'Hauptsitz', 'Gründungsjahr',
                                   'Name_Funktion', 'Tarifgebunden_Ja', 'Angaben_zur_Personalstruktur_Berichtsjahr',
                                   'Angaben_zur_Personalstruktur_Anzahl_Hauptamtliche',
                                   'Angaben_zur_Personalstruktur_Anzahl_Honorarkräfte',
                                   'Angaben_zur_Personalstruktur_Anzahl_GfB',
                                   'Angaben_zur_Personalstruktur_Anzahl_Freiwilligendienstleistende',
                                   'Angaben_zur_Personalstruktur_Anzahl_ehrenamtliche_Mitarbeiter',
                                   'Mittelherkunft_Berichtsjahr', 'Mittelherkunft_Gesamteinnahmen', 'Mittelherkunft_davon',
                                   'Mittelherkunft_öffentliche_Zuwendungen', 'Mittelherkunft_Spenden_und_Mitgliedsbeiträge',
                                   'Name_Sitz', 'street', 'code', 'city', 'phone', 'fax', 'email', 'title', 'description',
                                   'organization_presentation', 'website', 'Tarifgebunden', 'Tarifgebunden_text',
                                   'Größenklasse', 'Anzahl_der_Beschäftigen']:
                    if inser_temp not in list(info.keys()):
                        info[inser_temp] = ''
                for blobs in ["image_data_binary", "logo", "Satzung_oder_Gesellschaftervertrag_pdf_1",
                              "Satzung_oder_Gesellschaftervertrag_pdf_2", "Tätigkeitsbericht_pdf_1",
                              "Mittelverwendung_pdf_1", "Mittelherkunft_pdf_1","company_name","hrb_number","last_update","transparent","depth","download_timeout","download_slot",
                              "download_latency","Bezirk","Gesellschaftsform","Stiftung","Fördernd","Verbandsmitgliedschaften","Behindertengerecht","Zielgruppen","Bereich","Gemeinnützigkeit","Datum_Gemeinnützigkeitsbescheinigung", "Hauptsitz", "Gründungsjahr",
                               "Name_Funktion", "Tarifgebunden_Ja", "Angaben_zur_Personalstruktur_Berichtsjahr",
                              "Angaben_zur_Personalstruktur_Anzahl_Hauptamtliche",
                            "Angaben_zur_Personalstruktur_Anzahl_Honorarkräfte",
                            "Angaben_zur_Personalstruktur_Anzahl_GfB",
                            "Angaben_zur_Personalstruktur_Anzahl_Freiwilligendienstleistende",
                            "Angaben_zur_Personalstruktur_Anzahl_ehrenamtliche_Mitarbeiter",
                            "Mittelherkunft_Berichtsjahr","Mittelherkunft_Gesamteinnahmen", "Mittelherkunft_davon",
                            "Mittelherkunft_öffentliche_Zuwendungen", "Mittelherkunft_Spenden_und_Mitgliedsbeiträge",
                            "Name_Sitz", "street", "code", "city", "phone", "fax",
                            "email",
                            "title", "description", "organization_presentation", "website",
                            'Größenklasse', 'Anzahl_der_Beschäftigen',
                            'Tarifgebunden', 'Tarifgebunden_text',
                            'image_data_binary', 'Satzung_oder_Gesellschaftervertrag_pdf_1',
                            'Satzung_oder_Gesellschaftervertrag_pdf_2', 'Tätigkeitsbericht_pdf_1',
                            'Mittelverwendung_pdf_1', 'Mittelherkunft_pdf_1']:

                    if blobs not in list(info.keys()):
                        pdf_data = b''
                        info[blobs] = pdf_data


                cursor.execute(query, (info['url'],
                    info["company_name"], info["hrb_number"], info["last_update"], info["transparent"], info["depth"],
                    info["download_timeout"], info["download_slot"], info["download_latency"], info["Bezirk"],
                    info["Gesellschaftsform"], info["Stiftung"], info["Fördernd"], info["Verbandsmitgliedschaften"],
                    info["Behindertengerecht"], info["Zielgruppen"], info["Bereich"], info["Gemeinnützigkeit"],
                    info["Datum_Gemeinnützigkeitsbescheinigung"], info["Hauptsitz"], info["Gründungsjahr"],
                    info["Name_Funktion"], info["Tarifgebunden_Ja"], info["Angaben_zur_Personalstruktur_Berichtsjahr"],
                    info["Angaben_zur_Personalstruktur_Anzahl_Hauptamtliche"],
                    info["Angaben_zur_Personalstruktur_Anzahl_Honorarkräfte"],
                    info["Angaben_zur_Personalstruktur_Anzahl_GfB"],
                    info["Angaben_zur_Personalstruktur_Anzahl_Freiwilligendienstleistende"],
                    info["Angaben_zur_Personalstruktur_Anzahl_ehrenamtliche_Mitarbeiter"],
                    info["Mittelherkunft_Berichtsjahr"],
                    info["Mittelherkunft_Gesamteinnahmen"], info["Mittelherkunft_davon"],
                    info["Mittelherkunft_öffentliche_Zuwendungen"], info["Mittelherkunft_Spenden_und_Mitgliedsbeiträge"],
                    info["Name_Sitz"], info["street"], info["code"], info["city"], info["phone"], info["fax"],
                    info["email"],
                    info["title"], info["description"], info["organization_presentation"], info["website"],
                    info['Größenklasse'], info['Anzahl_der_Beschäftigen'],
                    info['Tarifgebunden'], info['Tarifgebunden_text'],
                    info['image_data_binary'], info['Satzung_oder_Gesellschaftervertrag_pdf_1'],
                    info['Satzung_oder_Gesellschaftervertrag_pdf_2'], info['Tätigkeitsbericht_pdf_1'],
                    info['Mittelverwendung_pdf_1'], info['Mittelherkunft_pdf_1']))

                # Commit the changes to the database
                db.commit()

                a = 1
            else:
                for inser_temp in ["image_data_binary", "logo", "Satzung_oder_Gesellschaftervertrag_pdf_1",
                                   "Satzung_oder_Gesellschaftervertrag_pdf_2", "Tätigkeitsbericht_pdf_1",
                                   "Mittelverwendung_pdf_1", "Mittelherkunft_pdf_1", "company_name", "hrb_number",
                                   "last_update", "transparent", "depth", "download_timeout", "download_slot",
                                   "download_latency", "Bezirk", "Gesellschaftsform", "Stiftung", "Fördernd",
                                   "Verbandsmitgliedschaften", "Behindertengerecht", "Zielgruppen", "Bereich",
                                   "Gemeinnützigkeit", "Datum_Gemeinnützigkeitsbescheinigung", "Hauptsitz",
                                   "Gründungsjahr",
                                   "Name_Funktion", "Tarifgebunden_Ja", "Angaben_zur_Personalstruktur_Berichtsjahr",
                                   "Angaben_zur_Personalstruktur_Anzahl_Hauptamtliche",
                                   "Angaben_zur_Personalstruktur_Anzahl_Honorarkräfte",
                                   "Angaben_zur_Personalstruktur_Anzahl_GfB",
                                   "Angaben_zur_Personalstruktur_Anzahl_Freiwilligendienstleistende",
                                   "Angaben_zur_Personalstruktur_Anzahl_ehrenamtliche_Mitarbeiter",
                                   "Mittelherkunft_Berichtsjahr", "Mittelherkunft_Gesamteinnahmen",
                                   "Mittelherkunft_davon",
                                   "Mittelherkunft_öffentliche_Zuwendungen",
                                   "Mittelherkunft_Spenden_und_Mitgliedsbeiträge",
                                   "Name_Sitz", "street", "code", "city", "phone", "fax",
                                   "email",
                                   "title", "description", "organization_presentation", "website",
                                   'Größenklasse', 'Anzahl_der_Beschäftigen',
                                   'Tarifgebunden', 'Tarifgebunden_text',
                                   'image_data_binary', 'Satzung_oder_Gesellschaftervertrag_pdf_1',
                                   'Satzung_oder_Gesellschaftervertrag_pdf_2', 'Tätigkeitsbericht_pdf_1',
                                   'Mittelverwendung_pdf_1', 'Mittelherkunft_pdf_1']:
                    if inser_temp not in list(info.keys()):
                        info[inser_temp] = ''
                query = """UPDATE finyou_rd.transparenzregister SET 
                                                company_name = %s, hrb_number=%s, last_update=%s, transparent=%s, depth=%s, download_timeout=%s,
                                                download_slot=%s, download_latency=%s, Bezirk=%s, Gesellschaftsform=%s, Stiftung=%s, Fördernd=%s,
                                                Verbandsmitgliedschaften=%s, Behindertengerecht=%s, Zielgruppen=%s, Bereich=%s, Gemeinnützigkeit=%s,
                                                Datum_Gemeinnützigkeitsbescheinigung=%s, Hauptsitz=%s, Gründungsjahr=%s, Name_Funktion=%s,
                                                Tarifgebunden_Ja=%s, Angaben_zur_Personalstruktur_Berichtsjahr=%s,
                                                Angaben_zur_Personalstruktur_Anzahl_Hauptamtliche=%s,
                                                Angaben_zur_Personalstruktur_Anzahl_Honorarkräfte=%s, Angaben_zur_Personalstruktur_Anzahl_GfB=%s,
                                                Angaben_zur_Personalstruktur_Anzahl_Freiwilligendienstleistende=%s,
                                                Angaben_zur_Personalstruktur_Anzahl_ehrenamtliche_Mitarbeiter=%s, Mittelherkunft_Berichtsjahr=%s,
                                                Mittelherkunft_Gesamteinnahmen=%s, Mittelherkunft_davon=%s, Mittelherkunft_öffentliche_Zuwendungen=%s,
                                                Mittelherkunft_Spenden_und_Mitgliedsbeiträge=%s, Name_Sitz=%s, street=%s, code=%s, city=%s, phone=%s, fax=%s,
                                                email=%s, title=%s, description=%s, organization_presentation=%s, website=%s,Größenklasse=%s,Anzahl_der_Beschäftigen=%s,Tarifgebunden=%s,Tarifgebunden_text=%s,logo=%s,satzung_oder_Gesellschaftervertrag_pdf_1=%s,satzung_oder_Gesellschaftervertrag_pdf_2=%s,
                                                Tätigkeitsbericht_pdf_1=%s,Mittelverwendung_pdf_1=%s,Mittelherkunft_pdf_1=%s where ul= %s
                                        """
                values = (
                    info["company_name"], info["hrb_number"], info["last_update"], info["transparent"], info["depth"],
                    info["download_timeout"], info["download_slot"], info["download_latency"], info["Bezirk"],
                    info["Gesellschaftsform"], info["Stiftung"], info["Fördernd"], info["Verbandsmitgliedschaften"],
                    info["Behindertengerecht"], info["Zielgruppen"], info["Bereich"], info["Gemeinnützigkeit"],
                    info["Datum_Gemeinnützigkeitsbescheinigung"], info["Hauptsitz"], info["Gründungsjahr"],
                    info["Name_Funktion"], info["Tarifgebunden_Ja"], info["Angaben_zur_Personalstruktur_Berichtsjahr"],
                    info["Angaben_zur_Personalstruktur_Anzahl_Hauptamtliche"],
                    info["Angaben_zur_Personalstruktur_Anzahl_Honorarkräfte"],
                    info["Angaben_zur_Personalstruktur_Anzahl_GfB"],
                    info["Angaben_zur_Personalstruktur_Anzahl_Freiwilligendienstleistende"],
                    info["Angaben_zur_Personalstruktur_Anzahl_ehrenamtliche_Mitarbeiter"],
                    info["Mittelherkunft_Berichtsjahr"],
                    info["Mittelherkunft_Gesamteinnahmen"], info["Mittelherkunft_davon"],
                    info["Mittelherkunft_öffentliche_Zuwendungen"], info["Mittelherkunft_Spenden_und_Mitgliedsbeiträge"],
                    info["Name_Sitz"], info["street"], info["code"], info["city"], info["phone"], info["fax"],
                    info["email"],
                    info["title"], info["description"], info["organization_presentation"], info["website"],
                    info['Größenklasse'], info['Anzahl_der_Beschäftigen'],
                    info['Tarifgebunden'], info['Tarifgebunden_text'],
                    info['image_data_binary'], info['Satzung_oder_Gesellschaftervertrag_pdf_1'],
                    info['Satzung_oder_Gesellschaftervertrag_pdf_2'], info['Tätigkeitsbericht_pdf_1'],
                    info['Mittelverwendung_pdf_1'], info['Mittelherkunft_pdf_1'],info['url'])
                cursor.execute(query, values)

                # Commit the changes to the database
                db.commit()
                cursor.close()
                db.close()



        except Exception as e:
            print(e)
            for inser_temp in ["image_data_binary", "logo", "Satzung_oder_Gesellschaftervertrag_pdf_1",
                              "Satzung_oder_Gesellschaftervertrag_pdf_2", "Tätigkeitsbericht_pdf_1",
                              "Mittelverwendung_pdf_1", "Mittelherkunft_pdf_1","company_name","hrb_number","last_update","transparent","depth","download_timeout","download_slot",
                              "download_latency","Bezirk","Gesellschaftsform","Stiftung","Fördernd","Verbandsmitgliedschaften","Behindertengerecht","Zielgruppen","Bereich","Gemeinnützigkeit","Datum_Gemeinnützigkeitsbescheinigung", "Hauptsitz", "Gründungsjahr",
                               "Name_Funktion", "Tarifgebunden_Ja", "Angaben_zur_Personalstruktur_Berichtsjahr",
                              "Angaben_zur_Personalstruktur_Anzahl_Hauptamtliche",
                            "Angaben_zur_Personalstruktur_Anzahl_Honorarkräfte",
                            "Angaben_zur_Personalstruktur_Anzahl_GfB",
                            "Angaben_zur_Personalstruktur_Anzahl_Freiwilligendienstleistende",
                            "Angaben_zur_Personalstruktur_Anzahl_ehrenamtliche_Mitarbeiter",
                            "Mittelherkunft_Berichtsjahr","Mittelherkunft_Gesamteinnahmen", "Mittelherkunft_davon",
                            "Mittelherkunft_öffentliche_Zuwendungen", "Mittelherkunft_Spenden_und_Mitgliedsbeiträge",
                            "Name_Sitz", "street", "code", "city", "phone", "fax",
                            "email",
                            "title", "description", "organization_presentation", "website",
                            'Größenklasse', 'Anzahl_der_Beschäftigen',
                            'Tarifgebunden', 'Tarifgebunden_text',
                            'image_data_binary', 'Satzung_oder_Gesellschaftervertrag_pdf_1',
                            'Satzung_oder_Gesellschaftervertrag_pdf_2', 'Tätigkeitsbericht_pdf_1',
                            'Mittelverwendung_pdf_1', 'Mittelherkunft_pdf_1']:
                if inser_temp not in list(info.keys()):
                    info[inser_temp] = ''
            for blobs in ["image_data_binary", "logo", "Satzung_oder_Gesellschaftervertrag_pdf_1",
                          "Satzung_oder_Gesellschaftervertrag_pdf_2", "Tätigkeitsbericht_pdf_1",
                          "Mittelverwendung_pdf_1", "Mittelherkunft_pdf_1"]:
                if blobs not in list(info.keys()):
                    pdf_data = b''
                    info[blobs] = pdf_data
            query = """INSERT INTO finyou_rd.transparenzregister (url,company_name, hrb_number, last_update, transparent, depth, download_timeout,
                                        download_slot, download_latency, Bezirk, Gesellschaftsform, Stiftung, Fördernd,
                                        Verbandsmitgliedschaften, Behindertengerecht, Zielgruppen, Bereich, Gemeinnützigkeit,
                                        Datum_Gemeinnützigkeitsbescheinigung, Hauptsitz, Gründungsjahr, Name_Funktion,
                                        Tarifgebunden_Ja, Angaben_zur_Personalstruktur_Berichtsjahr,
                                        Angaben_zur_Personalstruktur_Anzahl_Hauptamtliche,
                                        Angaben_zur_Personalstruktur_Anzahl_Honorarkräfte, Angaben_zur_Personalstruktur_Anzahl_GfB,
                                        Angaben_zur_Personalstruktur_Anzahl_Freiwilligendienstleistende,
                                        Angaben_zur_Personalstruktur_Anzahl_ehrenamtliche_Mitarbeiter, Mittelherkunft_Berichtsjahr,
                                        Mittelherkunft_Gesamteinnahmen, Mittelherkunft_davon, Mittelherkunft_öffentliche_Zuwendungen,
                                        Mittelherkunft_Spenden_und_Mitgliedsbeiträge, Name_Sitz, street, code, city, phone, fax,
                                        email, title, description, organization_presentation, website,Größenklasse,Anzahl_der_Beschäftigen,Tarifgebunden,Tarifgebunden_text,logo,satzung_oder_Gesellschaftervertrag_pdf_1,satzung_oder_Gesellschaftervertrag_pdf_2,
                                        Tätigkeitsbericht_pdf_1,Mittelverwendung_pdf_1,Mittelherkunft_pdf_1
                                    ) VALUES ( %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,
                                    %s, %s, %s, %s  ,%s ,%s,%s,%s,%s,%s,%s,%s,%s,%s
                                    )
                                """
            try:
                cursor.execute(query, (info['url'],
                    info["company_name"], info["hrb_number"], info["last_update"], info["transparent"], info["depth"],
                    info["download_timeout"], info["download_slot"], info["download_latency"], info["Bezirk"],
                    info["Gesellschaftsform"], info["Stiftung"], info["Fördernd"], info["Verbandsmitgliedschaften"],
                    info["Behindertengerecht"], info["Zielgruppen"], info["Bereich"], info["Gemeinnützigkeit"],
                    info["Datum_Gemeinnützigkeitsbescheinigung"], info["Hauptsitz"], info["Gründungsjahr"],
                    info["Name_Funktion"], info["Tarifgebunden_Ja"], info["Angaben_zur_Personalstruktur_Berichtsjahr"],
                    info["Angaben_zur_Personalstruktur_Anzahl_Hauptamtliche"],
                    info["Angaben_zur_Personalstruktur_Anzahl_Honorarkräfte"],
                    info["Angaben_zur_Personalstruktur_Anzahl_GfB"],
                    info["Angaben_zur_Personalstruktur_Anzahl_Freiwilligendienstleistende"],
                    info["Angaben_zur_Personalstruktur_Anzahl_ehrenamtliche_Mitarbeiter"],
                    info["Mittelherkunft_Berichtsjahr"],
                    info["Mittelherkunft_Gesamteinnahmen"], info["Mittelherkunft_davon"],
                    info["Mittelherkunft_öffentliche_Zuwendungen"], info["Mittelherkunft_Spenden_und_Mitgliedsbeiträge"],
                    info["Name_Sitz"], info["street"], info["code"], info["city"], info["phone"], info["fax"],
                    info["email"],
                    info["title"], info["description"], info["organization_presentation"], info["website"],
                    info['Größenklasse'], info['Anzahl_der_Beschäftigen'],
                    info['Tarifgebunden'], info['Tarifgebunden_text'],
                    info['image_data_binary'], info['Satzung_oder_Gesellschaftervertrag_pdf_1'],
                    info['Satzung_oder_Gesellschaftervertrag_pdf_2'], info['Tätigkeitsbericht_pdf_1'],
                    info['Mittelverwendung_pdf_1'], info['Mittelherkunft_pdf_1']))


                # Commit the changes to the database
                db.commit()
                cursor.close()
                db.close()
            except Exception as E:
                a=1
                pass

    def ibb_parse(self,response):
        grant_type = ""
        for grants_koopa in ["wirtschaftsfoerderung", "arbeitsmarktfoerderung", "immobilienfoerderung"]:
            if grants_koopa in response.url:
                grant_type = grants_koopa

        for loop in response.css('section.contentModule.fundingProgramList article a ::attr(href)').extract():
            if loop != '#':
                yield scrapy.Request(url='https://www.ibb.de' + loop, callback=self.ibb_grant,
                                     meta={"grant_type": grant_type})

    def ibb_grant(self,response):
        connection_values = connection_db()
        cursor = connection_values[0]
        db = connection_values[1]
        item = dict()
        item['grant_type'] = response.meta['grant_type']
        try:
            item['grant_name'] = response.css('header#top h1 ::text').get()
        except:
            item['grant_name'] = ""
        try:
            item['introduction'] = response.css('header section.contentModule p ::text').get()
        except:
            item['introduction'] = ""
        try:
            name_response = response.css('article.detailPage section.contentModule.infoModule.info p ::text').get()
            if 'hieß zuvor' in name_response:
                item['info_box'] = name_response.split('hieß zuvor')[-1]

        except:
            item['info_box'] = ""

        item['grant_url'] = response.url
        page_intro = response.css(".col-md-16.pageIntro.pdfContent").get()
        main_content = response.css(".col-md-16.mainContent").get()
        merged_html = page_intro + main_content
        text = scrapy.Selector(text=merged_html).xpath("//text()").extract()
        text = " ".join(text)
        html = merged_html
        try:
            item['html'] = html
        except:
            item['html'] = ""

        try:
            item['text'] = text
        except:
            item['text'] = ""

        try:
            item['short_facts'] = " , ".join(response.css('div#auf-einen-blick ul li p ::text').extract())

        except:
            item['short_facts'] = ""

        for koop in response.css('div.panel-group>article'):
            if koop.css('a[rel="noopener noreferrer"] span ::text').get().strip() == 'Wer wird gefördert?':
                try:
                    item['who'] = "\n".join(koop.css('div.panel-collapse.collapse section ::text').extract()).strip()
                except:
                    item['who'] = ""

            if koop.css('a[rel="noopener noreferrer"] span ::text').get().strip() == 'Was wird gefördert?':
                try:
                    item['what'] = "\n".join(koop.css('div.panel-collapse.collapse section ::text').extract()).strip()
                except:
                    item['what'] = ""
            if koop.css('a[rel="noopener noreferrer"] span ::text').get().strip() == 'Wie wird gefördert?':
                try:
                    item['how'] = "\n".join(koop.css('div.panel-collapse.collapse section ::text').extract()).strip()
                except:
                    item['how'] = ""
            if koop.css('a[rel="noopener noreferrer"] span ::text').get().strip() == 'Zu welchen Konditionen?':
                try:
                    item['conditions'] = "\n".join(
                        koop.css('div.panel-collapse.collapse section ::text').extract()).strip()
                except:
                    item['conditions'] = ""

            if koop.css('a[rel="noopener noreferrer"] span ::text').get().strip() == 'Wie verläuft die Antragstellung?':
                try:
                    item['application'] = "\n".join(
                        koop.css('div.panel-collapse.collapse section ::text').extract()).strip()
                except:
                    item['application'] = ""

            if koop.css(
                    'a[rel="noopener noreferrer"] span ::text').get().strip() == 'Was gibt es sonst noch zu beachten?':
                try:
                    item['what_else'] = "\n".join(
                        koop.css('div.panel-collapse.collapse section ::text').extract()).strip()
                except:
                    item['what_else'] = ""

        zip_file_names = []
        program_dir = item['grant_name']
        program_dir = program_dir.replace('/', "").replace('|', "").replace(' ', "-")
        print(program_dir)
        try:
            os.makedirs(program_dir, exist_ok=True)
        except:
            a = 1
            pass
        for index, pdfs in enumerate(response.css(
                'section.contentModule.moduleDownload.pdfContentExclude ul li a[data-event-action="Download"] ::attr(href)').extract()):
            download_pdf('https://www.ibb.de' + pdfs, os.getcwd()+'/'+program_dir+"/"+pdfs.split('/')[-1].strip())
            zip_file_names.append(pdfs.split('/')[-1].strip())

        if len(zip_file_names) > 0:

            zip_path = "archive" + ".zip"
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as archive:
                for root, dirs, files in os.walk(program_dir):
                    for file in files:
                        archive.write(os.path.join(root, file), file)
            with open(zip_path, 'rb') as zip_file:
                zip_data = zip_file.read()

            os.remove(zip_path)
            file_names = ','.join(zip_file_names)


        else:
            zip_data = b""
            file_names = ""

        # remove the dir
        shutil.rmtree(os.getcwd() + '/' + program_dir)
        time.sleep(3)



        for fills in ["grant_name", "introduction", "info_box", "short_facts", "who", "what", "how", "conditions",
                      "application", "what_else", "text"]:
            if fills not in list(item.keys()):
                item[fills] = ""

        read_query = f"SELECT * FROM finyou_rd.ibb_grants where grant_url='{item['grant_url']}'"

        # Execute the select query
        cursor.execute(read_query)

        # Fetch all rows from the result set
        result = cursor.fetchall()

        if len(result) < 1:
            query = """INSERT INTO finyou_rd.ibb_grants (
                    grant_url,
                    grant_type,
                    html,
                    text,
                    grant_name,
                    introduction,
                    info_box,
                    short_facts,
                    who,
                    what,
                    how,
                    conditions,
                    application,
                    what_else,
                    pdf_zips,
                    file_names
                    ) VALUES ( %s,%s, %s,%s, %s, %s, %s,%s, %s, %s, %s, %s,%s,%s,%s,%s)
                                            """
            cursor.execute(query, (
            item['grant_url'], item['grant_type'], item['html'], item['text'], item['grant_name'], item['introduction'],
            item['info_box'], item['short_facts'], item['who'], item['what'], item['how'], item['conditions'],
            item['application'], item['what_else'], zip_data, file_names))

            # Commit the changes to the database
            db.commit()
            a = 1
        else:
            # Define the update query
            query = """UPDATE finyou_rd.ibb_grants SET
                                grant_type = %s,
                                html = %s,
                                text = %s,
                                grant_name = %s,
                                introduction = %s,
                                info_box = %s,
                                short_facts = %s,
                                who = %s,
                                what = %s,
                                how = %s,
                                conditions = %s,
                                application = %s,
                                what_else = %s,
                                pdf_zips = %s,
                                file_names = %s
                              WHERE grant_url = %s"""
            # Define the values to be updated
            values = (item['grant_type'], item['html'], item['text'], item['grant_name'],
                      item['introduction'], item['info_box'], item['short_facts'], item['who'], item['what'],
                      item['how'],
                      item['conditions'], item['application'], item['what_else'], zip_data, file_names,
                      item['grant_url']
                      )

            # Execute the update query with the values
            cursor.execute(query, values)

            # Commit the changes to the database
            db.commit()
            a = 1


process = CrawlerProcess()
process.crawl(Scraper)
process.start()
b=1

