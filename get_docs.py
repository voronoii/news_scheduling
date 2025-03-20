from prefect import flow
import requests
import datetime
from tqdm import tqdm
import pickle



seoul_gu_list = ['구로구', '동대문구', '용산구', '종로구', '성북구', '광진구', '영등포구', '관악구', '은평구',
 '중랑구', '도봉구', '금천구', '송파구', '강서구', '동작구', '서초구', '성동구', '마포구', '중구', '강동구', '강남구', '양천구',
 '노원구', '강북구', '서대문구']


@flow(log_prints=True)
def get_vk_news(date_range:int):
    url = 'https://tm2api.some.co.kr/TrendMap/JSON/ServiceHandler?'
    total_docs = {}


    # 오늘날짜로부터 일주일전의 날짜까지를 yyyymmdd 형식으로 리스트화
    dates = [(datetime.datetime.today() - datetime.timedelta(days=i)).strftime("%Y%m%d") for i in range(date_range)]
    for gu in tqdm(seoul_gu_list):
        total_docs[gu] = []
        total_temp = []

        for pid in range(1, 10):
            params = {"lang":"ko","source":"news",
                                    "startDate": dates[-1],
                                    "endDate":dates[0],
                                    "keyword":f"(#@VK#S2#부동산)&&({gu})",
                                    "rowPerPage":"1000","pageNum":str(pid),"orderType":"0",
                                    "command":"GetKeywordDocuments"}

            
            res = requests.get(url, params=params).json()
            doc_list = res['item']['documentList']
            if len(doc_list) == 0:
                continue

            doc_list = list(set([f'{doc["title"]} {doc["content"]}' for doc in doc_list]))
            total_temp += doc_list

        total_docs[gu] = total_temp

    today = (datetime.datetime.today()).strftime("%Y%m%d")
    with open(f'../data/naver_real_estate_with_comments_{today}_{date_range}_tm2.pkl', 'wb') as f:
        pickle.dump(total_docs, f)
    print("file saved.")

    # 병합
    # for k, v in origin_data.items():
    #     print(len(origin_data[k]))
    #     origin_data[k] += total_docs[k]
    #     print(len(origin_data[k]))


if __name__ == "__main__":
    get_vk_news(1)