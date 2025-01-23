import os
import json
import time
import asyncio
import math
import httpx
import xmltodict
from collections import defaultdict

# year_month 를 2022년도로 변경 (예시)
# year_month = ['202401', '202402', '202403', '202404', '202405',
#               '202406', '202407', '202408', '202409', '202410', '202411', '202412']
# year_month = ['202301', '202302', '202303', '202304', '202305', '202306', '202307', '202308', '202309', '202310', '202311', '202312']
year_month = ['202309']

# land_code = [
#     '51150', '51820', '51170', '51230', '51210', '51800', '51830', '51750', '51130', '51810', '51770', '51780', '51110', '51190', '51760', '51720', '51790', '51730'
# ]
land_code = [
    "11710",
]
########################
# 1) 네트워크 호출 & 로깅
########################


async def fetch_and_log(client, url, params):
    print(f"[REQUEST] GET {url}, params={params}")
    resp = await client.get(url, params=params)
    print(
        f"[RESPONSE] status={resp.status_code}, content[:100]={resp.text[:100]}...")
    resp.raise_for_status()  # HTTP 에러코드면 예외
    return resp

########################
# 2) totalCount 조회
########################


async def get_total_count(lawd_cd: str, deal_ymd: str, service_key: str) -> int:
    url = "http://apis.data.go.kr/1613000/RTMSDataSvcLandTrade/getRTMSDataSvcLandTrade"
    params = {
        "serviceKey": service_key,
        "LAWD_CD": lawd_cd,
        "DEAL_YMD": deal_ymd,
        "pageNo": 1,
        "numOfRows": 1
    }

    async with httpx.AsyncClient() as client:
        resp = await fetch_and_log(client, url, params)
    data = xmltodict.parse(resp.text)

    total_count_str = (
        data.get("response", {})
            .get("body", {})
            .get("totalCount", "0")
    )

    try:
        total_count = int(total_count_str)
        print(
            f"[INFO] get_total_count => (lawd_cd={lawd_cd}, deal_ymd={deal_ymd}) => total_count={total_count}")
    except ValueError:
        print(
            f"[WARNING] total_count_str='{total_count_str}' is not an integer. Setting to 0.")
        total_count = 0

    return total_count

########################
# 3) 페이지별 데이터 조회
########################


async def fetch_page_data(client, url, params):
    print(f"[REQUEST_PAGE] GET {url}, params={params}")
    resp = await client.get(url, params=params)
    print(
        f"[RESPONSE_PAGE] status={resp.status_code}, content[:100]={resp.text[:100]}...")
    resp.raise_for_status()
    return resp

########################
# 4) save_land_info
########################


async def save_land_info():
    daily_limit = 950
    calls_today = 0
    num_of_rows = 100

    # service_key = "WXYY8pEqPloaplBKg1cO1/KNrYYKl/jzBkU/95Wee64b+tO/P6QN+GLGhxTP8AWhu7EjYiGzyK96r+xhlT7TkA=="
    # service_key = "Is8OYneAatrDOxbNCsmisrDM2Mr5/I3QgOj/KFDEio44kM4+xjseKNqDsO7eFpdc8OdJt+UkShitVzwFjXrexQ=="
    # service_key = "6mTwQ17ctTHOQN6Ts3CqM6xJVv2tW6zcbJSExg0k3TZfH/l/OvKS8l40/5AjS3HajEyC0xFy5jOrgQ4euTUOLQ=="
    service_key = "S72G8Ah5C4YY+lk9nnNgrPUr+XtW9RIKhiRbEh6qze0mxXoUIMphwZkBDSEdDJ2JynvKreDlEOXCeAfPsFZM9g=="
    base_url = "http://apis.data.go.kr/1613000/RTMSDataSvcLandTrade/getRTMSDataSvcLandTrade"

    all_data = []

    async with httpx.AsyncClient() as client:
        for code in land_code:
            for ymd in year_month:

                # (1) total_count
                try:
                    total_count = await get_total_count(code, ymd, service_key)
                except httpx.HTTPStatusError as e:
                    error_info = {
                        "lawd_cd": code,
                        "deal_ymd": ymd,
                        "error_type": "HTTPStatusError_totalCount",
                        "status_code": e.response.status_code,
                        "response_text": e.response.text[:200],
                        "items": [],
                    }
                    all_data.append(error_info)
                    print(
                        f"[HTTPStatusError_totalCount] code={code}, ymd={ymd}, status={e.response.status_code}")
                    continue
                except httpx.RequestError as e:
                    error_info = {
                        "lawd_cd": code,
                        "deal_ymd": ymd,
                        "error_type": "RequestError_totalCount",
                        "error_message": str(e),
                        "items": []
                    }
                    all_data.append(error_info)
                    print(
                        f"[RequestError_totalCount] code={code}, ymd={ymd}, error={e}")
                    continue
                except Exception as e:
                    error_info = {
                        "lawd_cd": code,
                        "deal_ymd": ymd,
                        "error_type": "Exception_totalCount",
                        "error_message": str(e),
                        "items": []
                    }
                    all_data.append(error_info)
                    print(
                        f"[Exception_totalCount] code={code}, ymd={ymd}, error={e}")
                    continue

                if total_count == 0:
                    all_data.append({
                        "lawd_cd": code,
                        "deal_ymd": ymd,
                        "total_count": 0,
                        "items": []
                    })
                    continue

                # (2) 페이지별 조회
                total_pages = math.ceil(total_count / num_of_rows)
                items_collected = []

                for page in range(1, total_pages + 1):
                    if calls_today >= daily_limit:
                        return {
                            "status": "stopped",
                            "message": "Daily limit reached. Processing paused.",
                            "calls_used_today": calls_today,
                            "partial_data": all_data
                        }

                    params = {
                        "serviceKey": service_key,
                        "LAWD_CD": code,
                        "DEAL_YMD": ymd,
                        "pageNo": page,
                        "numOfRows": num_of_rows,
                    }

                    try:
                        print(
                            f"[INFO] Fetching page={page}/{total_pages}, lawd_cd={code}, deal_ymd={ymd}")
                        resp = await fetch_page_data(client, base_url, params)
                        calls_today += 1
                        await asyncio.sleep(1)

                        data = xmltodict.parse(resp.text)
                        items = (
                            data.get("response", {})
                            .get("body", {})
                            .get("items", {})
                            .get("item", [])
                        )
                        if isinstance(items, dict):
                            items = [items]

                        print(f"[INFO] Got {len(items)} items on this page.")

                        for item in items:
                            row_dict = {
                                "sggCd": item.get("sggCd"),
                                "sggNm": item.get("sggNm"),
                                "umdNm": item.get("umdNm"),
                                "jimok": item.get("jimok"),
                                "jibun": item.get("jibun"),
                                "dealAmount": item.get("dealAmount"),
                                "dealArea": item.get("dealArea"),
                                "dealingGbn": item.get("dealingGbn"),
                                "landUse": item.get("landUse"),
                                "shareDealingType": item.get("shareDealingType"),
                                "cdealType": item.get("cdealType"),
                                "cdealDay": item.get("cdealDay"),
                                "estateAgentSggNm": item.get("estateAgentSggNm"),
                                "dealYear": item.get("dealYear"),
                                "dealMonth": item.get("dealMonth"),
                                "dealDay": item.get("dealDay"),
                            }
                            items_collected.append(row_dict)

                    except httpx.HTTPStatusError as e:
                        print(
                            f"[HTTPStatusError_page] code={code}, ymd={ymd}, page={page}, status={e.response.status_code}")
                        all_data.append({
                            "lawd_cd": code,
                            "deal_ymd": ymd,
                            "error_type": "HTTPStatusError_in_page_loop",
                            "status_code": e.response.status_code,
                            "page": page,
                            "items_collected_so_far": items_collected,
                        })
                        break
                    except httpx.RequestError as e:
                        print(
                            f"[RequestError_page] code={code}, ymd={ymd}, page={page}, error={e}")
                        all_data.append({
                            "lawd_cd": code,
                            "deal_ymd": ymd,
                            "error_type": "RequestError_in_page_loop",
                            "error": str(e),
                            "page": page,
                            "items_collected_so_far": items_collected,
                        })
                        break
                    except Exception as e:
                        print(
                            f"[Exception_page] code={code}, ymd={ymd}, page={page}, error={e}")
                        all_data.append({
                            "lawd_cd": code,
                            "deal_ymd": ymd,
                            "error_type": "Exception_in_page_loop",
                            "error": str(e),
                            "page": page,
                            "items_collected_so_far": items_collected,
                        })
                        break
                else:
                    all_data.append({
                        "lawd_cd": code,
                        "deal_ymd": ymd,
                        "total_count": total_count,
                        "items": items_collected
                    })

    return {
        "status": "success",
        "calls_used_today": calls_today,
        "data": all_data
    }

########################
# 5) main
########################


async def main():
    result = await save_land_info()

    if result.get("status") == "success":
        print(">>> [save_land_info] SUCCESS!")
        data_list = result["data"]

        # code별로 묶음
        grouped = defaultdict(list)
        for row in data_list:
            code = row["lawd_cd"]
            grouped[code].append(row)

        folder_name = "land_info_results"
        os.makedirs(folder_name, exist_ok=True)

        for code, new_items in grouped.items():
            file_path = os.path.join(folder_name, f"{code}.json")

            # 1) 만약 기존 파일이 있으면 로드
            if os.path.exists(file_path):
                with open(file_path, "r", encoding="utf-8") as f:
                    try:
                        existing_data = json.load(f)
                        # 파일에 이미 있는 데이터 (list 형태일 것으로 가정)
                    except json.JSONDecodeError:
                        # 혹시 파일이 깨져있다면 빈 리스트로 처리
                        existing_data = []
            else:
                existing_data = []

            # 2) 기존 데이터와 새 데이터를 합침
            combined_data = existing_data + new_items

            # (선택) 중복 제거 로직이 필요하면 구현 가능
            # 혹은 deal_ymd / jibun / dealDay 등이 같으면 중복이라든지...

            # 3) 최종 combined_data를 파일로 저장
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(combined_data, f, ensure_ascii=False, indent=2)

        print(f"Saved appended JSON in '{folder_name}' folder.")

    elif result.get("status") == "stopped":
        print(">>> [save_land_info] STOPPED (daily limit reached)!")
        partial_data = result["partial_data"]
        print(f"Collected partial data (count={len(partial_data)}).")

        folder_name = "land_info_results_partial"
        os.makedirs(folder_name, exist_ok=True)

        grouped = defaultdict(list)
        for row in partial_data:
            code = row["lawd_cd"]
            grouped[code].append(row)

        # 동일하게 기존파일 로드 -> 새 데이터 합침 -> 저장
        for code, new_items in grouped.items():
            file_path = os.path.join(folder_name, f"{code}.json")
            if os.path.exists(file_path):
                with open(file_path, "r", encoding="utf-8") as f:
                    try:
                        existing_data = json.load(f)
                    except json.JSONDecodeError:
                        existing_data = []
            else:
                existing_data = []

            combined_data = existing_data + new_items

            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(combined_data, f, ensure_ascii=False, indent=2)

        print(f"Saved partial appended JSON in '{folder_name}' folder.")

    else:
        print(">>> [save_land_info] ERROR or unknown status.")
        print(result)


if __name__ == "__main__":
    asyncio.run(main())
