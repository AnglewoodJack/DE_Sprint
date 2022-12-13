"""
Необходимо спарсить данные о вакансиях python разработчиков с сайта hh.ru, введя в поиск “python разработчик” и указав,
что мы рассматриваем все регионы. Необходимо спарсить:

Название вакансии
Требуемый опыт работы
Заработную плату
Регион
"""
import json
import time
import random
import requests
import numpy as np
import logging.config
from tqdm.auto import tqdm
from bs4 import BeautifulSoup
from traceback import format_exc

from config.aux import runtime
from config.logger import LOGGING_CONFIG

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)


@runtime
def api_vacancies(search_params: dict, url: str = 'https://api.hh.ru/vacancies/') -> None:
	"""
	Extract vacancies information from hh.ru api.
	:param search_params: api search parameters.
	:param url: hh.ru api.
	"""
	# List of extracted vacancies.
	vacancy_list = []
	for i in tqdm(range(40)):
		# Search parameters.
		search_params['page'] = i
		try:
			# General data.
			r = requests.get(url, params=search_params)
			all_items = r.json()
			for item in all_items["items"]:
				currency = item.get("salary", "Not specified")
				if "salary" in item and item["salary"] is not None:
					min_sal = item.get("salary", {}).get("from", -1)
					max_sal = item.get("salary", {}).get("to", -1)
				else:
					min_sal, max_sal = -1, -1
				salary = f"{min_sal} - {max_sal} {currency}"
				# Vacancy specific data.
				url_vacancy = url + item["id"]
				r = requests.get(url_vacancy)
				vacancy = r.json()
				exp = item.get("experience", {}).get("name", "Not specified")
				# Final dictionary.
				vacancy = {
					"title": item["name"],
					"work experience": exp,
					"salary": salary,
					"region": item["area"]["name"]
				}
				vacancy_list.append(vacancy)
			# Random sleep between requests.
			time.sleep(np.round(random.uniform(1, 4), 2))
		except Exception as e:
			err = format_exc()
			logger.error(err)
			logger.info(e)
	# Save all data to file.
	with open("api.json", "w") as file:
		json.dump({"result": vacancy_list}, file, ensure_ascii=False)


def parse_vacancies(url: str) -> None:
	"""
	Extract vacancies information from hh.ru web.
	:param url: url for vacancies search.
	"""
	# Initiate session.
	s = requests.Session()
	s.headers.update(
		{'user-agent':
			 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
			 'AppleWebKit/537.36 (KHTML, like Gecko) '
			 'Chrome/105.0.0.0 Safari/537.36'
		}
	)
	# Initiate vacancy list.
	vacancy_list = []

	def get_data(session: requests.Session, search_url: str) -> tuple:
		"""
		Finds required vacancy information from search url.
		:param session: requests session object.
		:param search_url: url for vacancies search.
		:return: tuple with results.
		"""
		_r = session.get(search_url)
		_soup = BeautifulSoup(_r.text, "lxml")
		_title = _soup.find(attrs={"class": "vacancy-title"}).find("h1").text
		_we = _soup.find(attrs={"data-qa": "vacancy-experience"}).text
		_salaray = _soup.find(attrs={"class": "vacancy-title"}).find(attrs={"data-qa": "vacancy-salary"}).text
		_region = _soup.find(attrs={"data-qa": "vacancy-serp__vacancy-address"}).text
		return _title, _we, _salaray, _region

	for n in tqdm(range(40)):
		url = url + f"{n}&hhtmFrom=vacancy_search_list"
		r = s.get(url)
		soup = BeautifulSoup(r.text, "lxml")
		tags = soup.find_all(attrs={"class": "serp-item__title"})
		for i in tags:
			vac_url = i.attrs['href'].split('?')[0]
			try:
				title, we, salaray, region = get_data(session=s, search_url=vac_url)
				vacancy_list.append({
					"title": title,
					"work experience": we,
					"salary": salaray,
					"region": region,
				})
				with open("parse.json", "w") as file:
					json.dump({"result": vacancy_list}, file, ensure_ascii=False)
			except Exception as e:
				err = format_exc()
				logger.error(err)
				print(e)
		time.sleep(np.round(random.uniform(1, 4), 2))


if __name__ == "__main__":
	# Search parameters for api connection.
	params = {
		'text': 'python разработчик',
		'area': '113',
		'per_page': '40',
		'User-Agent': 'MyApp'
	}
	# Web search page (cut down off on page number).
	search_query = "https://hh.ru/search/vacancy?text=python++%D1%80%D0%B0%D0%B7%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D1%87%D0%B8%D0%BA&from=suggest_post&area=1&page="
	while True:
		selection = input("Select method of data extraction (api/parse): ")
		if selection == 'api':
			api_vacancies(search_params=params)
			break
		elif selection == 'parse':
			parse_vacancies(url=search_query)
			break
		else:
			print('Wrong method, please specify which method to use - "api" or "parse"')
