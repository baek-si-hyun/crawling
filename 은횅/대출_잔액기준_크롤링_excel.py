import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import pandas as pd
import logging
import datetime
import os
from openpyxl.utils import get_column_letter

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DESIRED_HEADERS = ['대출 종류', '은행', '구분', '금리']

async def crawl_kfb_loan_rates():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        url = 'https://portal.kfb.or.kr/compare/loan_household_new.php'
        logging.info(f"페이지 접속: {url}")
        await page.goto(url, timeout=1000000)

        # 페이지가 완전히 로드될 때까지 대기
        await page.wait_for_load_state('networkidle')

        # 모든 은행 선택 체크박스 클릭
        try:
            await page.check('input#BankAll')
            logging.info("모든 은행 선택 체크박스 클릭 완료")
        except Exception as e:
            logging.error(f"체크박스 클릭 실패: {e}")

        # 공시기준 선택 (잔액기준)
        try:
            await page.check('input#select_new_balance_2')
            logging.info("공시기준 '잔액기준' 선택 완료")
        except Exception as e:
            logging.error(f"공시기준 선택 실패: {e}")

        # 상세구분 선택 (대출금리 상세보기)
        try:
            await page.check('input#all_show_1')
            logging.info("상세구분 '대출금리 상세보기' 선택 완료")
        except Exception as e:
            logging.error(f"상세구분 선택 실패: {e}")

        # 대출 종류별 선택자
        loan_types = {
            '분할상환방식 주택담보대출': '#opt_1_1',
            '일시상환방식 주택담보대출': '#opt_1_2',
            '일반신용대출': '#opt_1_3',
            '신용한도대출(마이너스대출)': '#opt_1_4',
            '전세자금대출': '#opt_1_5',
            '가계대출': '#opt_1_6'
        }

        all_data = []

        # 각 대출종류를 순회하며 데이터 수집
        for loan_label, loan_selector in loan_types.items():
            # 대출 종류별 데이터 수집 리스트 초기화
            collected_data = []

            try:
                # 새로운 대출종류 선택
                await page.evaluate(f"document.querySelector('{loan_selector}').checked = true;")
                logging.info(f"대출종류 '{loan_label}' 선택 완료")
            except Exception as e:
                logging.error(f"대출종류 '{loan_label}' 선택 실패: {e}")
                continue

            # 검색 버튼 클릭
            try:
                await page.click('div.btnArea .btn a[href^="Javascript:LoanHouseholdNewSearch_cur();"]')
                logging.info("검색 버튼 클릭 완료")
                await asyncio.sleep(2)
            except Exception as e:
                logging.error(f"검색 버튼 클릭 실패: {e}")
                continue

            # 검색 결과 로드 대기
            try:
                await page.wait_for_selector('table.resultList_ty02 tbody tr', timeout=60000)
                logging.info(f"검색 결과 테이블 로드 완료 for 대출종류 '{loan_label}'")
            except Exception as e:
                logging.error(f"검색 결과 테이블 로드 실패 for 대출종류 '{loan_label}': {e}")
                continue

            # 페이지 콘텐츠 가져오기
            page_content = await page.content()
            soup = BeautifulSoup(page_content, 'html.parser')

            # 결과 테이블 찾기
            table = soup.find('table', class_='resultList_ty02')
            if not table:
                logging.error(f"결과 테이블을 찾을 수 없습니다 for 대출종류 '{loan_label}'")
                continue

            # 데이터 행 추출
            data_rows = table.find_all('tr')
            bank_name = None

            for tr in data_rows:
                cells = [td.get_text(strip=True) for td in tr.find_all('td') if td.get_text(strip=True)]

                # 은행 이름이 포함된 행 처리
                if len(cells) == 3:
                    bank_name = cells[0].replace('\xa0', ' ')
                    rate_type = cells[1]
                    rate_value = cells[2]
                elif len(cells) == 2 and bank_name:
                    rate_type = cells[0]
                    rate_value = cells[1]
                else:
                    # cells가 2나 3이 아닌 경우 건너뜁니다
                    continue
                
                # 데이터 수집
                collected_data.append({
                    '대출 종류': loan_label,
                    '은행': bank_name,
                    '구분': rate_type,
                    '금리': rate_value
                })

            logging.info(f"대출종류 '{loan_label}'의 데이터 수집 완료: {len(collected_data)} rows")
            all_data.extend(collected_data)

            # 다음 대출종류를 선택하기 전에 현재 선택된 대출종류를 해제
            try:
                await page.evaluate(f"document.querySelector('{loan_selector}').checked = false;")
                logging.info(f"대출종류 '{loan_label}' 해제 완료")
            except Exception as e:
                logging.error(f"대출종류 '{loan_label}' 해제 실패: {e}")

        logging.info(f"총 데이터 수집 완료: {len(all_data)} rows")

        # 브라우저 닫기
        await browser.close()

        # DataFrame 생성 및 컬럼 순서 맞추기
        df = pd.DataFrame(all_data)
        df = df[DESIRED_HEADERS]

        # 엑셀 파일 저장
        now = datetime.datetime.now()
        timestamp = now.strftime("%Y%m%d_%H%M%S")
        file_name = f"대출금리_잔액기준_{timestamp}.xlsx"
        file_path = os.path.join(os.getcwd(), file_name)

        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, header=True, sheet_name='종합대출금리')

            # 각 열의 최대 길이를 기반으로 열 너비 조정
            worksheet = writer.sheets['종합대출금리']
            for col in worksheet.columns:
                max_length = max(len(str(cell.value)) for cell in col if cell.value)
                column_letter = get_column_letter(col[0].column)
                worksheet.column_dimensions[column_letter].width = max_length + 2

        logging.info(f"엑셀 파일로 저장 완료: {file_path}")

if __name__ == "__main__":
    asyncio.run(crawl_kfb_loan_rates())