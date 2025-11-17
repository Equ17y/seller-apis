import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получить список товаров Яндекс.Маркета с пагинацией.

    Args:
        page (str): Токен страницы для пагинации, пустая строка для
        первой страницы.
        campaign_id (str): ID кампании продавца в Яндекс.Маркете.
        access_token (str): Токен доступа для API Яндекс.Маркета.

    Returns:
        dict: Результат запроса с товарами и метаданными пагинации.

    Examples:
        >>> get_product_list("", "12345", "token_abc")
        {'offerMappingEntries': [...], 'paging': {...}}

    Incorrect:
        >>> get_product_list(None, None, None)  # doctest: +SKIP
        Traceback (most recent call last):
            ...
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновить остатки товаров в Яндекс.Маркете.

    Args:
        stocks (list[dict]): Список словарей с данными об остатках товаров.
        campaign_id (str): ID кампании продавца в Яндекс.Маркете.
        access_token (str): Токен доступа для API Яндекс.Маркета.

    Returns:
        dict: Ответ API после обновления остатков.

    Examples:
        >>> stocks = [{'sku': '123', 'warehouseId': 'warehouse1',
        'items': [...]}]
        >>> update_stocks(stocks, "campaign123", "token456")
        {'status': 'OK'}

    Incorrect:
        >>> update_stocks("wrong", 123, None)
        Traceback (most recent call last):
        ...
        AttributeError: 'str' object has no attribute 'get'
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновить цены товаров в Яндекс.Маркете.

    Args:
        prices (list): Список словарей с данными о ценах товаров.
        campaign_id (str): ID кампании продавца в Яндекс.Маркете.
        access_token (str): Токен доступа для API Яндекс.Маркета.

    Returns:
        dict: Ответ API после обновления цен.

    Examples:
        >>> prices = [{'id': '123', 'price': {'value': 5990,
        'currencyId': 'RUR'}}]
        >>> update_price(prices, "campaign123", "token456")
        {'status': 'OK'}

    Incorrect:
        >>> update_price("not_list", None, 5)
        Traceback (most recent call last):
        ...
        AttributeError: 'str' object has no attribute 'get'
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получить все артикулы товаров из Яндекс.Маркета.

    Проходит по всем страницам пагинации и собирает shopSku товаров.

    Args:
        campaign_id (str): ID кампании продавца в Яндекс.Маркете.
        market_token (str): Токен доступа для API Яндекс.Маркета.

    Returns:
        list: Список артикулов товаров (shopSku).

    Examples:
        >>> get_offer_ids("campaign123", "token456")
        ['12345', '67890', '54321']

    Incorrect:
        >>> get_offer_ids(None, None)
        Traceback (most recent call last):
            ...
        AttributeError: 'NoneType' object has no attribute 'get'
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Создать структуру данных для обновления остатков в Яндекс.Маркете.

    Args:
        watch_remnants (list): Список товаров из файла остатков поставщика.
        offer_ids (list): Список артикулов товаров в Яндекс.Маркете.
        warehouse_id (str): ID склада в Яндекс.Маркете.

    Returns:
        list: Список словарей для отправки в API Яндекс.Маркета.

    Incorrect:
        >>> create_stocks("wrong", 5, None)
        Traceback (most recent call last):
            ...
        AttributeError: 'str' object has no attribute 'get'
    """
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создать структуру данных для обновления цен в Яндекс.Маркете.

    Args:
        watch_remnants (list): Список товаров из файла остатков поставщика.
        offer_ids (list): Список артикулов товаров в Яндекс.Маркете.

    Returns:
        list: Список словарей для отправки в API Яндекс.Маркета.

    Examples:
        >>> remnants = [{'Код': '123', 'Цена': "5'990.00 руб."}]
        >>> offers = ['123', '456']
        >>> create_prices(remnants, offers)
        [{'id': '123', 'price': {'value': 5990, 'currencyId': 'RUR'}}]

    Incorrect:
        >>> create_prices("wrong", None)
        Traceback (most recent call last):
            ...
        AttributeError: 'str' object has no attribute 'get'
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Асинхронно загрузить цены товаров в Яндекс.Маркет.

    Args:
        watch_remnants (list): Список товаров из файла остатков поставщика.
        campaign_id (str): ID кампании продавца в Яндекс.Маркете.
        market_token (str): Токен доступа для API Яндекс.Маркета.

    Returns:
        list: Список обновленных цен.

    Examples:
        >>> await upload_prices(remnants, "campaign123", "token456")
        [{'id': '123', 'price': {'value': 5990, 'currencyId': 'RUR'}}]

    Incorrect:
        >>> await upload_prices(None, None, None)
        Traceback (most recent call last):
            ...
        TypeError: ...
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Асинхронно загрузить остатки товаров в Яндекс.Маркет.

    Args:
        watch_remnants (list): Список товаров из файла остатков поставщика.
        campaign_id (str): ID кампании продавца в Яндекс.Маркете.
        market_token (str): Токен доступа для API Яндекс.Маркете.
        warehouse_id (str): ID склада в Яндекс.Маркете.

    Returns:
        tuple: Кортеж (непустые остатки, все остатки).

    Examples:
        >>> not_empty, all_stocks = await upload_stocks(remnants,
        "campaign123", "token456", "warehouse1")
        >>> len(not_empty)
        10
        >>> len(all_stocks)
        15
    Incorrect:
        >>> await upload_stocks(None, None, None, None)
        Traceback (most recent call last):
            ...
        TypeError: ...
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    """Основная функция для запуска синхронизации с Яндекс.Маркетом.

    Загружает данные из окружения и выполняет обновление остатков и цен
    для обеих кампаний (FBS и DBS).

    Examples:
        >>> main()
        # Производит обновление данных в Яндекс.Маркете
    """
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
