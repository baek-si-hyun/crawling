import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import pandas as pd
import logging
import re
import datetime
import os
from openpyxl import load_workbook

# 로깅 설정 (DEBUG 레벨로 변경)
logging.basicConfig(level=logging.INFO,  # INFO 레벨로 설정
                    format='%(asctime)s - %(levelname)s - %(message)s')


async def crawl_kfb_deposit():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)  # headless=False로 설정하면 브라우저가 표시됩니다.
        context = await browser.new_context()
        page = await context.new_page()

        url = 'https://portal.kfb.or.kr/compare/receiving_deposit_3.php'
        logging.info(f"페이지 접속: {url}")
        await page.goto(url, timeout=1000000)  

        # 페이지가 완전히 로드될 때까지 대기
        await page.wait_for_load_state('networkidle')

        # 1. 모든 은행 선택 체크박스 클릭 (id="AllCheck")
        try:
            await page.check('input#AllCheck')
            logging.info("모든 은행 선택 체크박스 클릭 완료")
        except Exception as e:
            logging.error(f"체크박스 클릭 실패: {e}")

        # 2. 만기 옵션 추출: 모든 만기 옵션을 리스트로 가져오기
        # 페이지 콘텐츠 가져오기
        page_content_initial = await page.content()
        soup_initial = BeautifulSoup(page_content_initial, 'html.parser')
        expiration_inputs = soup_initial.find_all('input', id=re.compile(r'^EXPIRATION'))
        maturity_options = []

        for input_elem in expiration_inputs:
            input_id = input_elem.get('id')
            # 해당 input에 연결된 label 찾기
            label = soup_initial.find('label', attrs={'for': input_id})
            if label:
                maturity_label = label.get_text(separator=' ', strip=True)
            else:
                # label이 없으면 value 속성 사용
                maturity_label = input_elem.get('value', 'Unknown Maturity')
            maturity_options.append({
                'id': input_id,
                'label': maturity_label
            })

        logging.info(f"만기 옵션 목록: {maturity_options}")

        # 3. 이자 계산방식 정의
        interest_types = {
            '단리': 'input#InterestType1',
            '복리': 'input#InterestType2'
        }

        # 4. 데이터 수집 리스트 초기화
        collected_data = []

        # 5. 이자 계산방식과 만기 옵션을 순회하며 데이터 크롤링
        for interest_label, interest_selector in interest_types.items():
            # 이자 계산방식 선택
            try:
                # 기존 선택을 해제할 수 없으므로, 단순히 원하는 라디오 버튼을 선택
                await page.check(interest_selector)
                logging.info(f"이자 계산방식 '{interest_label}' 선택 완료")
            except Exception as e:
                logging.error(f"이자 계산방식 '{interest_label}' 선택 실패: {e}")
                continue  # 다음 이자 계산방식으로 넘어감

            # 각 만기 옵션 순회
            for maturity in maturity_options:
                maturity_id = maturity['id']
                maturity_label = maturity['label']

                try:
                    # 만기 옵션 선택 (라디오 버튼은 자동으로 이전 선택 해제됨)
                    await page.check(f'input#{maturity_id}')
                    logging.info(f"만기 옵션 '{maturity_label}' 선택 완료")
                except Exception as e:
                    logging.error(f"만기 옵션 '{maturity_label}' 선택 실패: {e}")
                    continue  # 다음 만기 옵션으로 넘어감

                # 6. 정렬방식 설정
                try:
                    # 정렬방식 드롭다운에서 'BASIC_INTEREST' 선택 (value="BASIC_INTEREST")
                    await page.select_option('select#InterestMonth', 'BASIC_INTEREST')
                    logging.info(f"정렬방식 InterestMonth 선택 완료: BASIC_INTEREST")
                except Exception as e:
                    logging.error(f"InterestMonth 선택 실패: {e}")

                try:
                    # 정렬방식 드롭다운에서 '내림차순으로 정렬' 선택 (value="DESC")
                    await page.select_option('select#OrderByType', 'DESC')
                    logging.info("정렬방식 OrderByType 선택 완료: DESC")
                except Exception as e:
                    logging.error(f"OrderByType 선택 실패: {e}")

                # 7. 검색 버튼 클릭
                try:
                    # '검색' 버튼 클릭
                    await page.click('div.btnArea > span.btn > a:has-text("검색")')
                    logging.info(f"검색 버튼 클릭 완료 for 이자 계산방식 '{interest_label}', 만기 '{maturity_label}'")
                except Exception as e:
                    logging.error(f"검색 버튼 클릭 실패 for 이자 계산방식 '{interest_label}', 만기 '{maturity_label}': {e}")
                    continue  # 다음 만기 옵션으로 넘어감

                # 8. 검색 결과 로드 대기
                try:
                    await page.wait_for_selector('table.resultList_ty02', timeout=60000)  # 60초 타임아웃
                    logging.info(f"검색 결과 로드 완료 for 이자 계산방식 '{interest_label}', 만기 '{maturity_label}'")
                except Exception as e:
                    logging.error(f"검색 결과 로드 실패 for 이자 계산방식 '{interest_label}', 만기 '{maturity_label}': {e}")
                    continue  # 다음 만기 옵션으로 넘어감

                # 9. 페이지 콘텐츠 가져오기
                page_content = await page.content()
                soup = BeautifulSoup(page_content, 'html.parser')

                # 10. 결과 테이블 찾기
                table = soup.find('table', class_='resultList_ty02')
                if not table:
                    logging.error(f"결과 테이블을 찾을 수 없습니다 for 이자 계산방식 '{interest_label}', 만기 '{maturity_label}'")
                    continue  # 다음 만기 옵션으로 넘어감

                # 11. 테이블 헤더 추출
                header_row = table.find('tr')
                headers = []
                for th in header_row.find_all('th'):
                    header_text = th.get_text(separator=' ', strip=True)
                    headers.append(header_text)
                logging.info(f"테이블 헤더: {headers}")

                # 12. 테이블 데이터 추출
                rows = table.find_all('tr')[1:]  # 첫 번째 행은 헤더이므로 제외
                if not rows:
                    logging.info(f"이자 계산방식 '{interest_label}', 만기 '{maturity_label}'에 대한 데이터가 없습니다.")
                    continue

                for tr in rows:
                    cols = tr.find_all('td')
                    if len(cols) < 6:
                        row_html = tr.prettify()
                        logging.info(f"Skipped row HTML: {row_html}")  # DEBUG 레벨 대신 INFO로 변경
                        continue  # 다음 row로 넘어감

                    # 주요 데이터 추출
                    bank_name = cols[0].get_text(separator=' ', strip=True).replace('\xa0', ' ') or "없음"
                    product_link = cols[1].find('a')
                    if product_link:
                        product_name = product_link.get_text(separator=' ', strip=True) or "없음"
                    else:
                        product_name = "없음"
                        logging.warning(f"상품명에 'a' 태그가 없습니다. 은행: {bank_name}")

                    basic_rate = cols[2].get_text(separator=' ', strip=True) or "없음"
                    max_rate = cols[3].get_text(separator=' ', strip=True) or "없음"
                    # '전월취급평균금리'는 6번째 <td>로 가정 (인덱스 5)
                    if len(cols) >= 6:
                        average_rate = cols[5].get_text(separator=' ', strip=True) or "없음"
                    else:
                        average_rate = "없음"

                    # 세부 정보 tr 찾기
                    tr_detail = tr.find_next_sibling('tr', id='Goods_Text_TR')
                    if tr_detail:
                        div_details = tr_detail.find('div', class_='openTxt02')
                        if div_details:
                            detail_dict = {}
                            uls = div_details.find_all('ul')
                            for ul in uls:
                                lis = ul.find_all('li')
                                if len(lis) >= 2:
                                    key = lis[0].get_text(strip=True)
                                    value = lis[1].get_text(separator=' ', strip=True)
                                    detail_dict[key] = value

                            # 세부 정보 매핑
                            last_provided_date = detail_dict.get('은행 최종제공일', '없음')
                            post_maturity_rate = detail_dict.get('만기 후 금리', '없음')
                            subscription_method = detail_dict.get('가입방법', '없음')
                            preferential_conditions = detail_dict.get('우대조건', '없음')
                            subscription_restrictions = detail_dict.get('가입 제한조건', '없음')
                            target_audience = detail_dict.get('가입대상', '없음')
                            additional_notes = detail_dict.get('기타 유의사항', '없음')
                            maximum_limit = detail_dict.get('최고한도', '없음')
                        else:
                            last_provided_date = post_maturity_rate = subscription_method = preferential_conditions = subscription_restrictions = target_audience = additional_notes = maximum_limit = "없음"
                    else:
                        last_provided_date = post_maturity_rate = subscription_method = preferential_conditions = subscription_restrictions = target_audience = additional_notes = maximum_limit = "없음"

                    # 데이터 수집
                    collected_data.append({
                        '은행': bank_name,
                        '상품명': product_name,
                        '기본금리': basic_rate,
                        '최고금리': max_rate,
                        '전월취급평균금리': average_rate,
                        '은행 최종제공일': last_provided_date,
                        '만기 후 금리': post_maturity_rate,
                        '가입 방법': subscription_method,
                        '우대 조건': preferential_conditions,
                        '가입 제한조건': subscription_restrictions,
                        '가입대상': target_audience,
                        '기타 유의사항': additional_notes,
                        '최고한도': maximum_limit,
                        '이자계산방식': interest_label,
                        '만기': maturity_label
                    })

                logging.info(f"이자 계산방식 '{interest_label}', 만기 '{maturity_label}'의 데이터 수집 완료")

    # 브라우저 닫기 및 데이터 저장을 루프 밖으로 이동
        # 13. 중복 데이터 제거
        df = pd.DataFrame(collected_data)
        initial_count = len(df)
        df.drop_duplicates(inplace=True)
        final_count = len(df)
        logging.info(f"중복 제거 전: {initial_count}개, 중복 제거 후: {final_count}개")

        if final_count == 0:
            logging.warning("수집된 데이터가 없습니다. 엑셀 파일을 생성하지 않습니다.")
            await browser.close()
            return

        # 14. 엑셀 컬럼 정렬
        excel_columns = [
            '은행',
            '상품명',
            '기본금리',
            '최고금리',
            '전월취급평균금리',
            '은행 최종제공일',
            '만기 후 금리',
            '가입 방법',
            '우대 조건',
            '가입 제한조건',
            '가입대상',
            '기타 유의사항',
            '최고한도',
            '이자계산방식',
            '만기'
        ]
        # 컬럼이 모두 존재하는지 확인
        missing_columns = set(excel_columns) - set(df.columns)
        if missing_columns:
            logging.warning(f"누락된 컬럼이 있습니다: {missing_columns}")
            # 누락된 컬럼을 '없음'으로 채우기
            for col in missing_columns:
                df[col] = "없음"

        df = df[excel_columns]

        # 15. 엑셀 파일로 저장 및 열 너비 자동 조정
        # datetime 모듈을 사용하여 현재 날짜와 시간을 포맷
        now = datetime.datetime.now()
        timestamp = now.strftime("%Y%m%d_%H%M%S")
        file_name = f"예금_{timestamp}.xlsx"
        file_path = os.path.join(os.getcwd(), file_name)  # 현재 작업 디렉토리에 저장
        try:
            # ExcelWriter를 사용하여 엑셀 파일 작성
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Sheet1')
                workbook = writer.book
                worksheet = writer.sheets['Sheet1']

                # 각 열의 최대 길이를 기반으로 열 너비 조정
                for column_cells in worksheet.columns:
                    max_length = 0
                    column = column_cells[0].column_letter  # 열의 알파벳

                    for cell in column_cells:
                        try:
                            cell_length = len(str(cell.value))
                            if cell_length > max_length:
                                max_length = cell_length
                        except:
                            pass

                    adjusted_width = (max_length + 2)  # 여유 공간 추가
                    worksheet.column_dimensions[column].width = adjusted_width

            logging.info(f"엑셀 파일로 저장 완료: {file_path}")
        except Exception as e:
            logging.error(f"엑셀 파일 저장 실패: {e}")

        # 16. 브라우저 닫기
        await browser.close()


if __name__ == "__main__":
    asyncio.run(crawl_kfb_deposit())