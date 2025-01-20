#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import time
import calendar
from datetime import date, datetime

import warnings
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

###############################################################################
# (A) 드라이버 생성 함수
###############################################################################
def get_chrome_driver(download_dir: str) -> webdriver.Chrome:
    """
    주어진 download_dir로 파일을 자동으로 다운로드하는 크롬 드라이버를 생성하여 반환
    """
    chrome_options = Options()
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": download_dir,      # 다운로드 경로
        "download.prompt_for_download": False,           # 다운로드 확인 창 비활성화
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })
    # 필요하다면 headless 모드
    # chrome_options.add_argument("--headless")

    driver = webdriver.Chrome(options=chrome_options)
    return driver

###############################################################################
# (B) 단일 월 다운로드 함수 (순차 처리)
###############################################################################
def download_excel_for_month(month: int, year: int, download_dir: str) -> None:
    """
    1) 해당 연도(year), 월(month)에 맞춰 (1일 ~ 말일) 기간을 설정
    2) 아파트 탭 클릭 -> 날짜 입력 -> EXCEL 다운 클릭
    3) 다운로드된 새로운 파일(.xlsx/.xls) 발견 시까지 대기
    4) 파일 크기 변동이 없으면 안정화된 것으로 보고, 원하는 새 이름으로 rename
    """
    from selenium.webdriver.common.by import By

    # 1) 시작일, 종료일 문자열 준비 (예: 2024-01-01 ~ 2024-01-31)
    last_day = calendar.monthrange(year, month)[1]
    start_date_str = f"{year}-{month:02d}-01"
    end_date_str   = f"{year}-{month:02d}-{last_day:02d}"

    # 2) ChromeDriver 생성
    driver = get_chrome_driver(download_dir)
    try:
        # 사이트 접속
        url = "https://rt.molit.go.kr/pt/xls/xls.do?&mobileAt="
        driver.get(url)
        time.sleep(2)

        # 아파트 탭 클릭 (id="xlsTab1")
        apt_tab_selector = "#xlsTab1"
        driver.find_element(By.CSS_SELECTOR, apt_tab_selector).click()
        time.sleep(1)

        # 날짜 입력
        from_date_ele = driver.find_element(By.ID, "srhFromDt")
        driver.execute_script("arguments[0].value = '';", from_date_ele)  # 초기화
        from_date_ele.send_keys(start_date_str)

        to_date_ele = driver.find_element(By.ID, "srhToDt")
        driver.execute_script("arguments[0].value = '';", to_date_ele)    # 초기화
        to_date_ele.send_keys(end_date_str)

        time.sleep(1)

        # 다운로드 버튼 클릭 전, 기존 파일 목록 저장
        before_files = set(os.listdir(download_dir))

        # EXCEL 다운 버튼 클릭
        excel_down_button = driver.find_element(By.XPATH, "//button[contains(text(), 'EXCEL 다운')]")
        excel_down_button.click()

        # 3) 다운로드된 새로운 파일(.xlsx/.xls) 추적
        downloaded_file = None
        max_wait_sec = 60
        waited = 0

        while waited < max_wait_sec:
            time.sleep(1)
            waited += 1

            # 새로 생긴 파일(이전과 달라진 파일)을 찾는다
            current_files = set(os.listdir(download_dir))
            new_files = current_files - before_files

            # 확장자가 xlsx 또는 xls
            new_xl_files = [f for f in new_files if f.lower().endswith((".xlsx", ".xls"))]
            if new_xl_files:
                # 첫 번째(혹은 유일한) 새 엑셀 파일
                downloaded_file = new_xl_files[0]
                break
        
        if not downloaded_file:
            print(f"[{start_date_str} ~ {end_date_str}] 파일이 다운로드되지 않아 종료합니다.")
            return

        # 4) 파일 안정화(크기 변동이 없어질 때까지 대기)
        file_path = os.path.join(download_dir, downloaded_file)
        stable = False
        waited = 0
        while not stable and waited < max_wait_sec:
            size_now = os.path.getsize(file_path)
            time.sleep(1)
            waited += 1
            size_next = os.path.getsize(file_path)
            if size_now == size_next:
                stable = True

        if not stable:
            print(f"[{start_date_str} ~ {end_date_str}] 파일이 끝까지 다운로드되지 않았습니다.")
            return

        # 원하는 새 이름으로 변경
        new_name = f"apartment_{start_date_str}_{end_date_str}.xlsx"
        new_path = os.path.join(download_dir, new_name)
        os.rename(file_path, new_path)
        print(f"[{start_date_str} ~ {end_date_str}] 다운로드 및 파일명 변경 완료 -> {new_name}")

    except Exception as e:
        print(f"[오류] {year}-{month} 다운로드 중 오류: {e}")
    finally:
        driver.quit()

###############################################################################
# (C) 모든 월(1~12) 순차 처리
###############################################################################
def download_all_months(year: int, download_dir: str):
    """
    1~12월까지 순서대로 (병렬X) download_excel_for_month() 실행
    """
    for m in range(1, 13):
        download_excel_for_month(m, year, download_dir)

###############################################################################
# (D) 엑셀 파일 병합 - skiprows=12, header=0 (13행이 컬럼)
###############################################################################
def combine_excels(download_dir: str, output_path: str):
    """
    download_dir 안의 .xls/.xlsx 파일을,
    '13행을 컬럼 헤더'로, '14행부터 데이터'로 인식하여 읽고 병합.
    - 파일별 크기, DataFrame shape 로그를 남겨, 어느 파일에서 문제가 발생했는지 파악.
    """
    excel_files = [
        f for f in os.listdir(download_dir)
        if f.lower().endswith((".xls", ".xlsx"))
    ]
    if not excel_files:
        print("[주의] 병합할 엑셀 파일이 없습니다.")
        return

    all_dfs = []
    print(f"[정보] 병합 대상: {len(excel_files)}개 엑셀 파일")

    for idx, fname in enumerate(excel_files, start=1):
        file_path = os.path.join(download_dir, fname)
        file_size = os.path.getsize(file_path)
        print(f"\n[{idx}/{len(excel_files)}] 파일명: '{fname}' (크기: {file_size} bytes)")

        try:
            # (중요) 12행을 스킵, 다음 줄(13행)을 컬럼명, 14행부터 데이터
            df = pd.read_excel(
                file_path,
                skiprows=12,  # 1~12행 무시
                header=0,     # 그 다음 줄(13행)을 컬럼 헤더
                engine="openpyxl"
            )
            print(f" └─ 읽기 완료: shape={df.shape} (행, 열)")
            all_dfs.append(df)

        except Exception as e:
            print(f" └─ [오류] '{fname}' 읽기 실패: {e}")
            # 실패한 파일을 건너뛰고 계속 진행
            continue

    # all_dfs가 비어있으면 중단
    if not all_dfs:
        print("[주의] 모든 파일 읽기에 실패했거나 데이터가 없습니다.")
        return

    # 병합 시도
    try:
        final_df = pd.concat(all_dfs, ignore_index=True)
    except Exception as e:
        print(f"[오류] DataFrame 병합 중 오류 발생: {e}")
        return

    print(f"\n[정보] 최종 병합 DataFrame: shape={final_df.shape}")

    # 결과 저장
    try:
        final_df.to_excel(output_path, index=False)
        print(f"[완료] 병합 결과: {output_path}")
    except Exception as e:
        print(f"[오류] 엑셀 저장 중 예외 발생: {e}")

###############################################################################
# (E) 메인 실행부
###############################################################################
if __name__ == "__main__":
    # 필요하다면 openpyxl 경고 무시 (중복 경고 방지)
    # warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

    # 현재 .py 파일이 위치한 폴더 기준으로 "downloads" 폴더 지정
    base_dir = os.path.dirname(os.path.abspath(__file__))
    download_directory = os.path.join(base_dir, "downloads")
    os.makedirs(download_directory, exist_ok=True)

    # 1) 특정 연도 데이터(예: 2024년) 다운로드
    year = 2024
    download_all_months(year, download_directory)

    # 2) 다운로드된 모든 엑셀 파일 병합
    combined_file_path = os.path.join(download_directory, "combined.xlsx")
    combine_excels(download_dir=download_directory, output_path=combined_file_path)