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

# 신규취급액 기준의 헤더 설정
DESIRED_HEADERS_DETAILED = [
    '대출 종류', '은행', '구분', '1000~951점', '950~901점', '900~851점', '850~801점',
    '800~751점', '750~701점', '700~651점', '650~601점', '600점이하', '평균신용점수', 'CB회사명', '참고사항'
]

# 잔액기준의 헤더 설정
DESIRED_HEADERS_BALANCE = ['대출 종류', '은행', '구분', '금리']

async def crawl_kfb_loan_rates():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        url = 'https://portal.kfb.or.kr/compare/loan_household_new.php'
        logging.info(f"페이지 접속: {url}")
        await page.goto(url, timeout=1000000)

        # 대기 및 공통 요소 선택
        await page.wait_for_load_state('networkidle')
        await page.check('input#BankAll')
        await page.check('input#all_show_1')

        # 신규취급액 기준 크롤링 및 엑셀 생성 로직
        async def collect_data_detailed(page, loan_types, headers, basis_selector, filename_prefix):
            collected_data = []

            try:
                await page.check(basis_selector)
                logging.info(f"공시기준 선택 완료: {basis_selector}")
            except Exception as e:
                logging.error(f"공시기준 선택 실패: {e}")
                return

            last_average_score = ""
            last_cb_company = ""

            for loan_label, loan_selector in loan_types.items():
                try:
                    await page.evaluate(f"document.querySelector('{loan_selector}').checked = true;")
                    await page.click('div.btnArea .btn a[href^="Javascript:LoanHouseholdNewSearch_cur();"]')
                    await asyncio.sleep(3)
                    logging.info(f"대출종류 '{loan_label}' 검색 완료")
                except Exception as e:
                    logging.error(f"대출종류 '{loan_label}' 선택 및 검색 실패: {e}")
                    continue

                page_content = await page.content()
                soup = BeautifulSoup(page_content, 'html.parser')
                table = soup.find('table', class_='resultList_ty02')

                if not table:
                    logging.error(f"결과 테이블을 찾을 수 없습니다 for 대출종류 '{loan_label}'")
                    continue

                data_rows = table.find_all('tr')
                bank_name = None

                for tr in data_rows:
                    tds = tr.find_all('td')
                    if not tds:
                        continue

                    if tds[0].has_attr('rowspan'):
                        bank_name = tds[0].get_text(strip=True)
                        cells = [td.get_text(strip=True) for td in tds[1:]]
                    else:
                        cells = [td.get_text(strip=True) for td in tds]

                    rate_type = cells[0] if len(cells) > 0 else ''
                    rates = cells[1:11] if len(cells) > 10 else [''] * 10
                    additional_info = cells[11:] if len(cells) > 11 else []

                    average_credit_score = additional_info[0] if len(additional_info) > 0 else last_average_score
                    cb_company = additional_info[1] if len(additional_info) > 1 else last_cb_company
                    remarks = additional_info[2] if len(additional_info) > 2 else ''

                    last_average_score = average_credit_score
                    last_cb_company = cb_company

                    collected_data.append({
                        '대출 종류': loan_label,
                        '은행': bank_name if bank_name else '',
                        '구분': rate_type,
                        **{f'{score}': rate for score, rate in zip(headers[3:13], rates)},
                        '평균신용점수': average_credit_score,
                        'CB회사명': cb_company,
                        '참고사항': remarks
                    })

                logging.info(f"대출종류 '{loan_label}'의 데이터 수집 완료")

                await page.evaluate(f"document.querySelector('{loan_selector}').checked = false;")

            df = pd.DataFrame(collected_data)
            df = df[headers]
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name = f"{filename_prefix}_{timestamp}.xlsx"
            df.to_excel(file_name, index=False)
            logging.info(f"신규취급액 기준 엑셀 파일로 저장 완료: {file_name}")

        # 잔액기준 크롤링 및 엑셀 생성 로직
        async def collect_data_balance(page, loan_types, headers, basis_selector, filename_prefix):
            collected_data = []

            try:
                await page.check(basis_selector)
                logging.info(f"공시기준 선택 완료: {basis_selector}")
            except Exception as e:
                logging.error(f"공시기준 선택 실패: {e}")
                return

            for loan_label, loan_selector in loan_types.items():
                try:
                    await page.evaluate(f"document.querySelector('{loan_selector}').checked = true;")
                    await page.click('div.btnArea .btn a[href^="Javascript:LoanHouseholdNewSearch_cur();"]')
                    await asyncio.sleep(2)
                    logging.info(f"대출종류 '{loan_label}' 검색 완료")
                except Exception as e:
                    logging.error(f"대출종류 '{loan_label}' 선택 및 검색 실패: {e}")
                    continue

                page_content = await page.content()
                soup = BeautifulSoup(page_content, 'html.parser')
                table = soup.find('table', class_='resultList_ty02')

                if not table:
                    logging.error(f"결과 테이블을 찾을 수 없습니다 for 대출종류 '{loan_label}'")
                    continue

                data_rows = table.find_all('tr')
                bank_name = None

                for tr in data_rows:
                    cells = [td.get_text(strip=True) for td in tr.find_all('td') if td.get_text(strip=True)]

                    if len(cells) == 3:
                        bank_name = cells[0].replace('\xa0', ' ')
                        rate_type = cells[1]
                        rate_value = cells[2]
                    elif len(cells) == 2 and bank_name:
                        rate_type = cells[0]
                        rate_value = cells[1]
                    else:
                        continue

                    collected_data.append({
                        '대출 종류': loan_label,
                        '은행': bank_name,
                        '구분': rate_type,
                        '금리': rate_value
                    })

                logging.info(f"대출종류 '{loan_label}'의 데이터 수집 완료: {len(collected_data)} rows")

                await page.evaluate(f"document.querySelector('{loan_selector}').checked = false;")

            df = pd.DataFrame(collected_data)
            df = df[headers]
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name = f"{filename_prefix}_{timestamp}.xlsx"
            file_path = os.path.join(os.getcwd(), file_name)

            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, header=True, sheet_name='잔액기준대출금리')

                worksheet = writer.sheets['잔액기준대출금리']
                for col in worksheet.columns:
                    max_length = max(len(str(cell.value)) for cell in col if cell.value)
                    column_letter = get_column_letter(col[0].column)
                    worksheet.column_dimensions[column_letter].width = max_length + 2

            logging.info(f"잔액기준 엑셀 파일로 저장 완료: {file_path}")

        # 대출 종류별 선택자 정의
        loan_types = {
            '분할상환방식 주택담보대출': '#opt_1_1',
            '일시상환방식 주택담보대출': '#opt_1_2',
            '일반신용대출': '#opt_1_3',
            '신용한도대출(마이너스대출)': '#opt_1_4',
            '전세자금대출': '#opt_1_5',
            '가계대출': '#opt_1_6'
        }

        # 신규취급액 기준 데이터 수집
        await collect_data_detailed(page, loan_types, DESIRED_HEADERS_DETAILED, 'input#select_new_balance_1', "대출금리_신규취급액기준")

        # 잔액 기준 데이터 수집
        await collect_data_balance(page, loan_types, DESIRED_HEADERS_BALANCE, 'input#select_new_balance_2', "대출금리_잔액기준")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(crawl_kfb_loan_rates())