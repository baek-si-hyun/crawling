import os
import time
import calendar
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# -----------------------------------
# (1) 사용자 입력: 시도코드, 시군구코드, 연도 등
# -----------------------------------
print("예시) '11000' => 서울특별시, '11680' => 강남구 등")
user_sido_code = input("시도 코드를 입력하세요 (예: 11000): ")
user_sgg_code = input("시군구 코드를 입력하세요 (예: 11680): ")
year_str = input("연도를 입력하세요 (예: 2024): ")

try:
    year = int(year_str)
except ValueError:
    print("잘못된 연도 입력. 기본값 2024로 진행합니다.")
    year = 2024

# -----------------------------------
# (2) 다운로드 폴더 경로 설정
#     - 현재 작업 경로(os.getcwd()) 아래 "부동산" 폴더
#     - 그 아래 "년도_시군구코드" 형태 폴더
# -----------------------------------
base_dir = os.path.join(os.getcwd(), "부동산")          # 예: /.../부동산
if not os.path.exists(base_dir):
    os.makedirs(base_dir)

download_dir_name = f"{year}_{user_sgg_code}"         # 예: "2024_11680"
download_dir = os.path.join(base_dir, download_dir_name)
if not os.path.exists(download_dir):
    os.makedirs(download_dir)

# -----------------------------------
# (3) 크롬 옵션 및 드라이버 설정
# -----------------------------------
options = webdriver.ChromeOptions()

prefs = {
    "download.default_directory": download_dir,  # 다운로드 폴더 지정
    "download.prompt_for_download": False,       
    "directory_upgrade": True,
    "safebrowsing.enabled": True
}
options.add_experimental_option("prefs", prefs)

driver = webdriver.Chrome(options=options)

# -----------------------------------
# (4) 페이지 접속
# -----------------------------------
url = "https://rt.molit.go.kr/pt/xls/xls.do?mobileAt="
driver.get(url)
driver.maximize_window()

wait = WebDriverWait(driver, 10)

# -----------------------------------
# (5) 토지 탭 클릭 (fnThingChange('G'))
# -----------------------------------
land_tab = wait.until(EC.element_to_be_clickable((By.ID, "xlsTab7")))
land_tab.click()

# -----------------------------------
# (6) 시도/시군구 선택
# -----------------------------------
sido_select_element = wait.until(EC.presence_of_element_located((By.ID, "srhSidoCd")))
sido_select = Select(sido_select_element)
sido_select.select_by_value(user_sido_code)

# 시군구 목록 로딩 대기
time.sleep(1)

sgg_select_element = wait.until(EC.presence_of_element_located((By.ID, "srhSggCd")))
sgg_select = Select(sgg_select_element)
sgg_select.select_by_value(user_sgg_code)

# -----------------------------------
# (7) 날짜 설정(해당 연도 1월~12월)
# -----------------------------------
for month in range(1, 13):
    _, last_day = calendar.monthrange(year, month)
    from_date = f"{year}-{month:02d}-01"
    to_date = f"{year}-{month:02d}-{last_day:02d}"

    from_input = wait.until(EC.presence_of_element_located((By.ID, "srhFromDt")))
    from_input.clear()
    from_input.send_keys(from_date)

    to_input = wait.until(EC.presence_of_element_located((By.ID, "srhToDt")))
    to_input.clear()
    to_input.send_keys(to_date)

    # 엑셀 다운 버튼 클릭
    excel_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@onclick='fnExcelDown()']")))
    excel_btn.click()

    # (간단 대기) 실제로는 다운로드 완료 체크 루프가 더 안전
    time.sleep(5)

# -----------------------------------
# (8) 크롬 종료
# -----------------------------------
driver.quit()

# -----------------------------------
# (9) 다운로드 파일 확인 후, 병합
#     - "지분구분" 열에서 '지분'인 행 제거
# -----------------------------------
excel_files = [f for f in os.listdir(download_dir) if f.endswith((".xls", ".xlsx"))]
all_dataframes = []

for file in excel_files:
    file_path = os.path.join(download_dir, file)
    try:
        # 엑셀 읽기 (13번째 행을 헤더, 14번째 행부터 실제 데이터)
        df = pd.read_excel(file_path, engine="openpyxl", skiprows=12, header=0)
    except:
        df = pd.read_excel(file_path, skiprows=12, header=0)

    # '지분구분' 열에 '지분'값이 있으면 해당 행 제거
    if '지분구분' in df.columns:
        df = df[df['지분구분'] != '지분']

    all_dataframes.append(df)

# -----------------------------------
# (10) 병합 및 최종 파일 저장
# -----------------------------------
if all_dataframes:
    merged_df = pd.concat(all_dataframes, ignore_index=True)

    final_excel_name = f"merged_data_{year}_{user_sgg_code}.xlsx"
    final_excel_path = os.path.join(download_dir, final_excel_name)

    merged_df.to_excel(final_excel_path, index=False)
    print(f"모든 파일을 병합하여 '{final_excel_path}' 로 저장했습니다.")
else:
    print("병합할 엑셀 파일이 없습니다.")