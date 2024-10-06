import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup, Comment
import json
import re
import traceback
import motor.motor_asyncio
import os
from pymongo import UpdateOne
from bson import ObjectId
from googletrans import Translator  # 번역을 위해 추가
import logging
import time  # 딜레이를 위해 추가

# 로깅 설정
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# 몽고DB 클라이언트 생성
client = motor.motor_asyncio.AsyncIOMotorClient(
    'mongodb+srv://qortlgus100:TWPdnSfVbQmCJ5UR@return-plus-web.u5cgr.mongodb.net/?retryWrites=true&w=majority&appName=return-plus-web')
db = client['return-plus']
collection = db['news']

# 번역기 인스턴스 생성
translator = Translator()


def translate_text(text, src='ko', dest='en', retries=3, delay=2):
    """
    텍스트를 번역하는 동기 함수. 오류 발생 시 재시도.
    """
    for attempt in range(retries):
        try:
            return translator.translate(text, src=src, dest=dest).text
        except Exception as e:
            logging.warning(f"번역 시도 {attempt+1}/{retries} 실패: {e}")
            time.sleep(delay)
    logging.error(f"번역 실패: {text}")
    return None


async def crawl_naver_news_list(keyword):
    base_url = f"https://search.naver.com/search.naver?where=news&query={keyword}&sm=tab_opt&sort=1&photo=0&field=0&pd=1&ds=&de=&docid=&related=0&mynews=0&office_type=0&office_section_code=0&news_office_checked=&nso=so%3Add%2Cp%3A1w&is_sug_officeid=0&office_category=0&service_area=0"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(base_url)

        previous_height = None
        while True:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(1)
            current_height = await page.evaluate("document.body.scrollHeight")

            if previous_height == current_height:
                break
            previous_height = current_height

        page_content = await page.content()
        soup = BeautifulSoup(page_content, 'html.parser')
        news_links = soup.select('a.info')
        naver_links = [link['href']
                       for link in news_links if 'news.naver.com' in link.get('href', '')]

        await browser.close()

    return naver_links


async def save_articles_bulk(articles):
    operations = []
    for article in articles:
        operations.append(
            UpdateOne(
                {"url": article['url']},
                {"$set": article},
                upsert=True
            )
        )
    try:
        if operations:
            result = await collection.bulk_write(operations)
            logging.info(f"새로 삽입된 문서 수: {result.upserted_count}")
            logging.info(f"업데이트된 문서 수: {result.modified_count}")
    except Exception as e:
        logging.error(f"Bulk write 중 오류 발생: {e}")


async def crawl_naver_news_details(sem, browser, url):
    async with sem:
        logging.info(f"크롤링 중: {url}")
        article = {}
        try:
            page = await browser.new_page()
            await page.goto(url)
            await page.wait_for_load_state('networkidle')
            await asyncio.sleep(1)

            page_content = await page.content()
            soup = BeautifulSoup(page_content, 'html.parser')

            # 댓글 제거
            for element in soup(text=lambda text: isinstance(text, Comment)):
                element.extract()

            # 제목 추출
            title_element = soup.select_one(
                'h2.media_end_head_headline') or soup.select_one('h2#title_area')
            if title_element:
                title = title_element.get_text(strip=True)
                title = re.sub(r'\[.*?\]', '', title).strip()
            else:
                title = None
            logging.info(f"제목: {title}")

            # 내용 추출
            content_element = soup.find(
                'article', id='dic_area') or soup.find('div', id='dic_area')
            if content_element:
                for div in content_element.find_all('div', attrs=lambda attr: attr):
                    div.decompose()

                unwanted_classes = ['u_word_dic']
                for span in content_element.find_all('span', class_=unwanted_classes):
                    span.decompose()

                unwanted_tags = ['em']
                for tag in unwanted_tags:
                    for element in content_element.find_all(tag):
                        element.decompose()

                content = content_element.get_text(separator=' ', strip=True)
                content = re.sub(r'\[.*?\]', '', content)
                content = re.sub(r'\s+', ' ', content).strip()
            else:
                content = None
            logging.info(f"내용: {content}")

            # 언론사 추출
            press_element = soup.select_one('img.media_end_head_top_logo_img') or soup.select_one(
                'a.media_end_head_top_logo img')
            press = press_element['alt'] if press_element and press_element.has_attr(
                'alt') else None
            logging.info(f"언론사: {press}")

            # 날짜 추출
            date_element = soup.select_one('span._ARTICLE_DATE_TIME') or soup.select_one(
                'span.media_end_head_info_datestamp_time')
            date = date_element['data-date-time'] if date_element and date_element.has_attr(
                'data-date-time') else None
            logging.info(f"날짜: {date}")

            # 이미지 URL 추출
            image_element = soup.select_one('meta[property="og:image"]')
            if image_element and image_element.has_attr('content'):
                image = image_element['content']
            else:
                image_element = soup.select_one('span.end_photo_org img') or soup.select_one(
                    'img#img1') or soup.select_one('figure img')
                image = image_element['src'] if image_element and image_element.has_attr(
                    'src') else None
            logging.info(f"이미지 URL: {image}")

            if title and content:
                # 번역을 위한 이벤트 루프 가져오기
                loop = asyncio.get_running_loop()

                # 제목 번역 (비동기 실행)
                title_en = await loop.run_in_executor(None, translate_text, title) if title else None
                # 번역 요청 사이에 딜레이 추가
                await asyncio.sleep(1)
                # 내용 번역 (비동기 실행)
                content_en = await loop.run_in_executor(None, translate_text, content) if content else None
                # 번역 요청 사이에 딜레이 추가
                await asyncio.sleep(1)
                # 언론사 번역 (비동기 실행)
                press_en = await loop.run_in_executor(None, translate_text, press) if press else None

                article = {
                    "title": title,
                    "content": content,
                    "press": press,
                    "date": date,
                    "image": image,
                    "url": url,
                    "title_en": title_en,
                    "content_en": content_en,
                    "press_en": press_en ,
                    "tags" : []
                }
                return article
            else:
                logging.warning(f"필수 정보 누락으로 스킵: {url}")
                return None
        except Exception as e:
            logging.error(f"Error occurred while processing {url}: {e}")
            traceback.print_exc()
            return None
        finally:
            await page.close()


def default_serializer(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    raise TypeError(f'Type {obj.__class__.__name__} is not JSON serializable')


async def main():
    # 고유 인덱스 생성
    await collection.create_index("url", unique=True)

    keyword = "토큰증권"
    naver_links = await crawl_naver_news_list(keyword)
    logging.info(f"크롤링할 기사 수: {len(naver_links)}")

    sem = asyncio.Semaphore(5)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        tasks = []
        for link in naver_links:
            task = asyncio.create_task(
                crawl_naver_news_details(sem, browser, link))
            tasks.append(task)
        results = await asyncio.gather(*tasks)

        await browser.close()

    articles_to_save = [article for article in results if article]

    if articles_to_save:
        await save_articles_bulk(articles_to_save)

        # 특정 폴더에 JSON 파일 저장
        folder_name = "네이버"  # 원하는 폴더 이름으로 변경 가능
        os.makedirs(folder_name, exist_ok=True)  # 폴더가 없으면 생성

        file_name = f"{keyword}.json"
        file_path = os.path.join(folder_name, file_name)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(articles_to_save, f, ensure_ascii=False,
                      indent=4, default=default_serializer)
        logging.info(f"기사 정보가 {file_path} 파일로 저장되었습니다.")
    else:
        logging.info("크롤링된 기사가 없습니다.")

if __name__ == "__main__":
    asyncio.run(main())
