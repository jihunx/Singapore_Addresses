import requests
import pandas as pd
from pandas import json_normalize
import string 
from itertools import chain
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


access_token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoxMjkxOCwiZm9yZXZlciI6ZmFsc2UsImlzcyI6Ik9uZU1hcCIsImlhdCI6MTc3NzAxMTA4OCwibmJmIjoxNzc3MDExMDg4LCJleHAiOjE3NzcyNzAyODgsImp0aSI6Ijk1ODZmMTY1LWNlOGEtNDM5YS05NTMyLThiNGQ5YWY3NTVmYyJ9.XOdPso3iWIvSLyR_N-MaM-HQfoiD6JR_45Lic47KgemQ4UqoSwCBNDVzk6Hngh4Qugd-DVfcN2F68yWsbfq3Oa7v4e-3v5w2zGSikw97EzAcIQgBN_1B6McjKKBU4D1cY_04bp3O_MrHhrrEUPf2c3j-a9mAoEx-LdtTcU08_HftQb8m051w8MbdleZZCM-EY61mY0YuGG4e53JTcXjcotz7NH2OO94Ff7yUHU-8P84S52SaLssrYP5FQ6Y7E3cxI92Yni8VZLVT93z8ghunIyVshPRpFBSBUERLdK7_Dh_6qiEn0VgJm9lDqmnARArAP9rIxKImbIHQwGhjl2ogYw"
expiry_timestamp = "1689408622"
BASE_URL = "https://www.onemap.gov.sg/api/common/elastic/search"

session = requests.Session()
retries = Retry(total=3, backoff_factor=0.5, status_forcelist=(429, 500, 502, 503, 504), allowed_methods=("GET",))
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)
session.mount("http://", adapter)

alphabet = string.ascii_lowercase[:27]
directory = {}
temp = []
postal_codes = []

def fetch_search_page(search_val, page_num=1):
    params = {
        "searchVal": search_val,
        "returnGeom": "Y",
        "getAddrDetails": "Y",
        "pageNum": page_num,
    }
    try:
        response = session.get(BASE_URL, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as exc:
        print(f"[WARN] 요청 실패 searchVal={search_val}, pageNum={page_num}: {exc}")
        return None

    if "found" not in data or "totalNumPages" not in data:
        print(f"[WARN] 응답 형식 이상 searchVal={search_val}, pageNum={page_num}: {data}")
        return None
    return data

def fill_directory():
    for i in alphabet:
        val = i 
        data = fetch_search_page(val, 1)
        if not data:
            continue
        entries = data['found']
        pages = data['totalNumPages']
        directory[val.upper()] = (entries,pages)

def fill_postal_codes(start,end):
    #for i in range(start,end):
        #postal_codes.append('0'+str(i))
    for i in range(start,end):
        postal_codes.append(str(i))
    print("done1")

def fill_directory2():
    for i in postal_codes: 
        data = fetch_search_page(i, 1)
        if not data:
            continue
        entries = data['found']
        pages = data['totalNumPages']
        if pages>0:
            directory[i] = (entries,pages)
            print(i)
    print("done2")

def fill_df():
    for k,v in directory.items():
        print(k)
        val = k
        for i in range(1,v[1]+1):
            data = fetch_search_page(val, i)
            if not data:
                continue
            temp.append(data['results'])
            print(i)

    if not temp:
        print("[INFO] 수집된 데이터가 없어 엑셀 파일을 생성하지 않습니다.")
        return

    temp2 = list(chain(*temp))
    df = pd.DataFrame(temp2)
    print(df)
    df.to_excel('output10.xlsx',index=False)

if __name__ == "__main__":
    fill_postal_codes(804500,809999)
    fill_postal_codes(818900,819999)
    fill_postal_codes(820000,825200)
    fill_postal_codes(828500,829999)
    fill_directory2()
    fill_df()

















