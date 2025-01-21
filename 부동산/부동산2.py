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

# ----------------------------------------
# (A) 서울특별시 자치구명 -> 시군구코드 매핑
# ----------------------------------------
gu_code_map = {
    "강남구": "11680",
    "강동구": "11740",
    "강북구": "11305",
    "강서구": "11500",
    "관악구": "11620",
    "광진구": "11215",
    "구로구": "11530",
    "금천구": "11545",
    "노원구": "11350",
    "도봉구": "11320",
    "동대문구": "11230",
    "동작구": "11590",
    "마포구": "11440",
    "서대문구": "11410",
    "서초구": "11650",
    "성동구": "11200",
    "성북구": "11290",
    "송파구": "11710",
    "양천구": "11470",
    "영등포구": "11560",
    "용산구": "11170",
    "은평구": "11380",
    "종로구": "11110",
    "중구":   "11140",
    "중랑구": "11260",
}

def get_sgg_code(sgg_value: str) -> str:
    """
    예: '서울특별시 동작구 흑석동'처럼 구 이름이 들어있는 문자열(sgg_value)에서
    gu_code_map 딕셔너리를 참조해 시군구 코드를 리턴.
    어떤 구 이름도 매칭 안 되면 "" 반환.
    """
    if not isinstance(sgg_value, str):
        return ""  # NaN 등이 들어있으면 빈 문자열
    for gu_name, code in gu_code_map.items():
        if gu_name in sgg_value:
            return code
    return ""

# ----------------------------------------
# (B) 사용자 입력 (시도코드, 시군구코드, 연도)
# ----------------------------------------
print("예시) '11000' => 서울특별시, '11680' => 강남구 등")
user_sido_code = input("시도 코드를 입력하세요 (예: 11000): ")
user_sgg_code = input("시군구 코드를 입력하세요 (전체면 엔터): ")
year_str = input("연도를 입력하세요 (예: 2024): ")

try:
    year = int(year_str)
except ValueError:
    print("잘못된 연도 입력. 기본값 2024로 진행합니다.")
    year = 2024

# ----------------------------------------
# (C) 다운로드 폴더 설정
# ----------------------------------------
base_dir = os.path.join(os.getcwd(), "부동산")
if not os.path.exists(base_dir):
    os.makedirs(base_dir)

folder_suffix = user_sgg_code if user_sgg_code else "ALL"
download_dir_name = f"{year}_{folder_suffix}"
download_dir = os.path.join(base_dir, download_dir_name)
if not os.path.exists(download_dir):
    os.makedirs(download_dir)

# ----------------------------------------
# (D) 크롬 드라이버 설정
# ----------------------------------------
options = webdriver.ChromeOptions()
prefs = {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "directory_upgrade": True,
    "safebrowsing.enabled": True
}
options.add_experimental_option("prefs", prefs)

driver = webdriver.Chrome(options=options)

# ----------------------------------------
# (E) 대상 페이지 접속 & 토지 탭 클릭
# ----------------------------------------
url = "https://rt.molit.go.kr/pt/xls/xls.do?mobileAt="
driver.get(url)
driver.maximize_window()

wait = WebDriverWait(driver, 10)
land_tab = wait.until(EC.element_to_be_clickable((By.ID, "xlsTab7")))
land_tab.click()

# ----------------------------------------
# (F) 시도/시군구 선택
# ----------------------------------------
sido_select_element = wait.until(EC.presence_of_element_located((By.ID, "srhSidoCd")))
sido_select = Select(sido_select_element)
sido_select.select_by_value(user_sido_code)

time.sleep(1)
sgg_select_element = wait.until(EC.presence_of_element_located((By.ID, "srhSggCd")))
sgg_select = Select(sgg_select_element)
sgg_select.select_by_value(user_sgg_code)

# ----------------------------------------
# (G) 1~12월 날짜 반복하여 엑셀 다운로드
# ----------------------------------------
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

    excel_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@onclick='fnExcelDown()']")))
    excel_btn.click()

    time.sleep(5)

driver.quit()

# ----------------------------------------
# (I) 다운로드된 엑셀 파일 병합 + '지분구분'='지분' 제거
# ----------------------------------------
excel_files = [f for f in os.listdir(download_dir) if f.endswith((".xls", ".xlsx"))]
all_dataframes = []

for file in excel_files:
    file_path = os.path.join(download_dir, file)
    try:
        df = pd.read_excel(file_path, engine="openpyxl", skiprows=12, header=0)
    except:
        df = pd.read_excel(file_path, skiprows=12, header=0)

    if '지분구분' in df.columns:
        df = df[df['지분구분'] != '지분']

    all_dataframes.append(df)

if not all_dataframes:
    print("병합할 엑셀 파일이 없습니다.")
    exit()

merged_df = pd.concat(all_dataframes, ignore_index=True)

# ----------------------------------------
# (J) "시군구" 열로부터 시군구 코드 매핑
# ----------------------------------------
if "시군구" in merged_df.columns:
    merged_df["시군구 코드"] = merged_df["시군구"].apply(get_sgg_code)
else:
    print("주의: '시군구' 열이 존재하지 않습니다. 구 코드를 매핑할 수 없습니다.")

# ----------------------------------------
# (K) 'NO' 열 삭제 + '시군구 코드' 열을 첫 번째 열로 이동
# ----------------------------------------
if "NO" in merged_df.columns:
    merged_df.drop(columns=["NO"], inplace=True)

# 열 순서 재배열: "시군구 코드"가 맨 앞으로 오도록
cols = list(merged_df.columns)
if "시군구 코드" in cols:
    # "시군구 코드"를 맨 앞으로 가져오고, 나머지는 그대로 순서 유지
    new_order = ["시군구 코드"] + [c for c in cols if c != "시군구 코드"]
    merged_df = merged_df[new_order]

# ----------------------------------------
# (L) 최종 엑셀로 저장
# ----------------------------------------
final_excel_name = f"merged_data_{year}_{folder_suffix}.xlsx"
final_excel_path = os.path.join(download_dir, final_excel_name)
merged_df.to_excel(final_excel_path, index=False)

print(f"모든 파일을 병합하여 '{final_excel_path}' 로 저장했습니다.")