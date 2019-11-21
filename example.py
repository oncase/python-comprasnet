import logging.config
import time
from datetime import date, timedelta, datetime
import pandas as pd

from comprasnet.pages.search_auctions import SearchAuctions
from comprasnet.pages.statuse_auction_detail import StatuseAuctionDetail
import concurrent.futures

FILE_PATH = "temp_data"

# Logging configuration in script
logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
        },
    },
    'loggers': {
        '': {
            'level': 'DEBUG',
            'handlers': ['console'],
            'propagate': False
        },
    },
})


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def parse_json_data(data: pd.DataFrame, date: str):
    if data.empty:
        return
    file_name = "licitacao_" + date

    data.to_csv(FILE_PATH + "/" + file_name)


def extract_auctions(date):
    sa = SearchAuctions(day=date)
    sa.search()

    print(sa.total_pages)
    print(sa.current_page)
    all_action_details = []
    for result in sa.results:
        print(result)
        for possible_result in result:
            if "codigo-da-uasg" in possible_result.keys() and "pregao-eletronico" in possible_result.keys():
                details = StatuseAuctionDetail(uasg_code=possible_result["codigo-da-uasg"],
                                               auction_code=possible_result["pregao-eletronico"])
                auction_details = details.scrap_data()
                print(auction_details)
                temp_df = pd.DataFrame(auction_details["itens"])
                temp_df["cidade"] = possible_result["cidade"]
                temp_df["uf"] = possible_result["uf"]
                temp_df["objeto"] = possible_result["objeto"]
                temp_df["codigo-uash"] = possible_result["codigo-da-uasg"]
                temp_df["abertura-da-proposta"] = possible_result["abertura-da-proposta-str"]
                all_action_details.append(temp_df)
        print(sa.current_page)

    auction_items_df = pd.concat(all_action_details)
    parse_json_data(auction_items_df, str(date))
    return auction_items_df


if __name__ == "__main__":
    start_time = time.time()
    start_date = date(year=2019, month=7, day=1)
    end_date = date(year=2019, month=8, day=1)
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:

        for analyzed_date in daterange(start_date, end_date):
            logging.info("Date being scraped: " + str(analyzed_date))
            executor.submit(extract_auctions, analyzed_date)
    elapsed_time = time.time() - start_time
    print(elapsed_time)
