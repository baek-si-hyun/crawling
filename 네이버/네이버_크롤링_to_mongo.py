import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup, Comment
import json
import re
import traceback
import motor.motor_asyncio  # Motor 라이브러리 임포트
from pymongo.errors import DuplicateKeyError  # 중복 키 에러 처리
from bson import ObjectId  # ObjectId 임포트

# 몽고DB 클라이언트 생성
client = motor.motor_asyncio.AsyncIOMotorClient(
    'mongodb+srv://qortlgus100:TWPdnSfVbQmCJ5UR@return-plus-web.u5cgr.mongodb.net/?retryWrites=true&w=majority&appName=return-plus-web')
db = client['return-plus']  # 데이터베이스 이름 설정
collection = db['news']  # 컬렉션 이름 설정

articles = []  # 전역 변수로 articles 리스트를 선언


async def crawl_naver_news_list(keyword):
    base_url = f"https://search.naver.com/search.naver?where=news&query={keyword}&sm=tab_opt&sort=1&photo=0&field=0&pd=1&ds=&de=&docid=&related=0&mynews=0&office_type=0&office_section_code=0&news_office_checked=&nso=so%3Add%2Cp%3A1w&is_sug_officeid=0&office_category=0&service_area=0"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(base_url)

        previous_height = None
        while True:
            # Scroll to the bottom
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(1)  # Wait for page to load
            current_height = await page.evaluate("document.body.scrollHeight")

            # Break if no more content loads
            if previous_height == current_height:
                break
            previous_height = current_height

        # Extract HTML source
        page_content = await page.content()
        soup = BeautifulSoup(page_content, 'html.parser')
        news_links = soup.select('a.info')
        naver_links = [link['href']
                       for link in news_links if 'news.naver.com' in link['href']]

        await browser.close()

    return naver_links


async def save_to_mongodb(article):
    try:
        # 딕셔너리 복사본 생성
        article_copy = article.copy()
        result = await collection.insert_one(article_copy)
        print(f"기사 저장 완료: {result.inserted_id}")
    except DuplicateKeyError:
        print(f"이미 존재하는 기사입니다: {article['url']}")
    except Exception as e:
        print(f"몽고DB 저장 중 오류 발생: {e}")


async def crawl_naver_news_details(sem, browser, url):
    async with sem:
        print(f"크롤링 중: {url}")
        article = {}
        try:
            page = await browser.new_page()
            await page.goto(url)
            # 페이지 로딩 대기
            await page.wait_for_load_state('networkidle')
            await asyncio.sleep(1)  # 추가 대기 시간

            page_content = await page.content()
            soup = BeautifulSoup(page_content, 'html.parser')

            # 주석 제거
            for element in soup(text=lambda text: isinstance(text, Comment)):
                element.extract()

            # 제목 추출
            title_element = soup.select_one(
                'h2.media_end_head_headline') or soup.select_one('h2#title_area')
            if title_element:
                title = title_element.get_text(strip=True)
                # 대괄호와 그 안의 내용 제거
                title = re.sub(r'\[.*?\]', '', title)
                title = title.strip()
            else:
                title = None
            print(f"제목: {title}")

            # 기사 내용 전체 추출
            content_element = soup.find(
                'article', id='dic_area') or soup.find('div', id='dic_area')
            if content_element:
                # 1. 속성이 있는 <div> 태그 제거
                for div in content_element.find_all('div', attrs=lambda attr: attr):
                    div.decompose()

                # 2. 기타 불필요한 요소 제거 (예: 특정 클래스)
                unwanted_classes = ['u_word_dic']  # 필요에 따라 추가
                for span in content_element.find_all('span', class_=unwanted_classes):
                    span.decompose()

                # 3. 기타 불필요한 태그 제거 (예: <em>)
                unwanted_tags = ['em']
                for tag in unwanted_tags:
                    for element in content_element.find_all(tag):
                        element.decompose()

                # 4. 전체 텍스트 추출
                content = content_element.get_text(separator=' ', strip=True)

                # 5. 대괄호와 그 안의 내용 제거
                content = re.sub(r'\[.*?\]', '', content)

                # 6. 불필요한 공백 제거
                content = re.sub(r'\s+', ' ', content).strip()
            else:
                content = None
            print(f"내용: {content}")

            # 언론사 추출
            press_element = soup.select_one('img.media_end_head_top_logo_img') or soup.select_one(
                'a.media_end_head_top_logo img')
            press = press_element['alt'] if press_element else None
            print(f"언론사: {press}")

            # 날짜 추출
            date_element = soup.select_one('span._ARTICLE_DATE_TIME') or soup.select_one(
                'span.media_end_head_info_datestamp_time')
            date = date_element['data-date-time'] if date_element and 'data-date-time' in date_element.attrs else None
            print(f"날짜: {date}")

            # 이미지 URL 추출
            image_element = soup.select_one('meta[property="og:image"]')
            if image_element and 'content' in image_element.attrs:
                image = image_element['content']
            else:
                image_element = soup.select_one('span.end_photo_org img') or soup.select_one(
                    'img#img1') or soup.select_one('figure img')
                image = image_element['src'] if image_element else None
            print(f"이미지 URL: {image}")

            # 필수 필드(title, content)가 있는 경우에만 article 생성
            if title and content:
                article = {
                    "title": title,
                    "content": content,
                    "press": press,
                    "date": date,
                    "image": image,
                    "url": url
                }
                # 데이터를 몽고DB에 저장
                await save_to_mongodb(article)
                return article
            else:
                print(f"필수 정보 누락으로 스킵: {url}")
                return None
        except Exception as e:
            print(f"Error occurred while processing {url}: {e}")
            traceback.print_exc()  # 스택 트레이스 출력
            return None
        finally:
            await page.close()


def default_serializer(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    raise TypeError(f'Type {obj.__class__.__name__} is not JSON serializable')


async def main():
    # 인덱스 생성 (url 필드를 고유 인덱스로 설정)
    await collection.create_index("url", unique=True)

    keyword = "토큰증권"  # 키워드를 직접 입력하거나 원하는 방식으로 변경하세요
    naver_links = await crawl_naver_news_list(keyword)
    print(f"크롤링할 기사 수: {len(naver_links)}")

    # 동시 실행 작업 수 제한 (예: 5)
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

    for article in results:
        if article:
            # '_id' 필드 제거
            article.pop('_id', None)
            articles.append(article)

    if articles:
        # JSON 파일로 변환
        file_name = f"{keyword}.json"
        with open(file_name, 'w', encoding='utf-8') as f:
            json.dump(articles, f, ensure_ascii=False,
                      indent=4, default=default_serializer)
        print(f"기사 정보가 {file_name} 파일로 저장되었습니다.")
    else:
        print("크롤링된 기사가 없습니다.")

if __name__ == "__main__":
    asyncio.run(main())
