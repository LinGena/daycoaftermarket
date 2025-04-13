import requests
from dotenv import load_dotenv
from proxies.proxy_manager import get_proxies
import random
from db.core import IsDbCreated
from db.core import Db
import json
import time
from utils.func import func_chunk_array
from threading import Thread
from utils.func import write_to_file_json


load_dotenv(override=True)

class GetTreadPagesContent():
    def run(self, threads_count: int = 10) -> None:
        years = list(range(1980, 2026))
        chunks = func_chunk_array(years, threads_count)
        for i, chunk in enumerate(chunks):
            print(f"Thread {i+1}: {chunk}")
            thread = Thread(target=self.run_tread, args=(chunk,))
            thread.start()

    def run_tread(self, years: list) -> None:
        GenerateTask().run(years)



class GenerateTask():
    def __init__(self):
        self.list_proxies = get_proxies()
        self.model = Db()

    def run(self, years: list):
        for year in years:
            try:
                result = {}
                result['year'] = year
                result['status'] = 1
                data = {
                    'year': year,
                    }
                resp_makes = self.get_response(data, 'makes')
                makes = resp_makes.get('d',{}).get('results')
                for make in makes:
                    try:
                        data.update({"make":make['make']})
                        result['make'] = make['make']
                        resp_types = self.get_response(data, 'types')
                        types = resp_types.get('d',{}).get('results')
                        for type in types:
                            try:
                                data.update({"type":type['type']})
                                result['type'] = type['type']
                                resp_models = self.get_response(data, 'models')
                                models = resp_models.get('d',{}).get('results')
                                for model in models:
                                    try:
                                        data.update({"model":model['model']})
                                        result['model'] = model['model']
                                        resp_engines = self.get_response(data, 'engines')
                                        engines = resp_engines.get('d',{}).get('results')
                                        for engine in engines:
                                            try:
                                                data.update({"engine":engine['engine']})
                                                result['engine'] = engine['engine']
                                                parts = self.get_response(data, 'parts')
                                                try:
                                                    result['response'] = json.dumps(parts)
                                                except:
                                                    result['response'] = None
                                                self.insert_datas(result)  
                                            except Exception as ex:
                                                print(ex)
                                    except Exception as ex:
                                        print(ex) 
                            except Exception as ex:
                                print(ex)
                    except Exception as ex:
                        print(ex)
            except Exception as ex:
                print(ex)

    def insert_datas(self, result: dict):
        try:
            print(result.get('year'), result.get('make'), result.get('type'), result.get('model'), result.get('engine'))
            value_placeholders = ', '.join(['%s'] * len(result)) 
            sql = f"INSERT INTO {self.model.table_tasks} ({', '.join(result.keys())}) VALUES ({value_placeholders})"
            self.model.insert(sql, list(result.values()))
        except Exception as ex:
            print(ex)

    def get_response(self, data: dict, resp_alt: str, count_try: int = 0) -> dict:
        headers = {
            'accept': 'application/json',
            'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7,uk;q=0.6',
            'content-type': 'application/json; charset=UTF-8',
            'origin': 'https://www.daycoaftermarket.com',
            'priority': 'u=1, i',
            'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'cross-site',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
        }
        proxy = random.choice(self.list_proxies)
        proxies = {
            'http': proxy,
            'https': proxy
        }
        try:
            response = requests.post(f'https://restcatna.dayco.com/api/alt/{resp_alt}',
                                     headers=headers,
                                     json=data,
                                     proxies=proxies)
            response.raise_for_status()
            return response.json()
        except Exception as ex:
            print(ex)
        if count_try > 10:
            print('no data')
            return None
        return self.get_response(data, resp_alt, count_try+1)


        

if __name__ == "__main__":
    IsDbCreated().check()
    GetTreadPagesContent().run()




