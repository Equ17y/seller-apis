import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получает список товаров магазина из API Ozon.

    Args:
        last_id (str): Идентификатор для пагинации, пустая строка для
        начала.
        client_id (str): ID клиента в системе Ozon.
        seller_token (str): API-токен для авторизации.

    Returns:
        dict: Словарь с блоком result API Ozon, содержащим товары,
        total и last_id.

    Examples:
        >>> get_product_list("", "client123", "token456")
        {'items': [...], 'total': 100, 'last_id': 'next_page'}

    Incorrect:
        >>> get_product_list(None, None, None)
        Traceback (most recent call last):
            ...
        requests.exceptions.InvalidURL: ...
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получает все артикулы товаров магазина Ozon.

    Функция постранично собирает каталог товаров Ozon, пока не будут
    загружены все позиции, и возвращает список offer_id.

    Args:
        client_id (str): ID клиента в системе Ozon.
        seller_token (str): API-токен для авторизации.

    Returns:
        list[str]: Список артикулов товаров (offer_id)

    Examples:
        >>> get_offer_ids("client123", "token456")
        ['12345', '67890']

    Incorrect:
        >>> get_offer_ids(None, None)
        Traceback (most recent call last):
            ...
        requests.exceptions.InvalidURL: ...

    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновляет цены товаров в магазине Ozon.

    Args:
        prices (list): Список словарей с данными цен для обновления.
        client_id (str): ID клиента в системе Ozon.
        seller_token (str): API-токен для авторизации.

    Returns:
        dict: Ответ API после обновления цен.

    Examples:
        >>> update_price([{"offer_id":"A", "price":"1000"}], "client",
        "token")
        {'result': {...}}

    Incorrect:
        >>> update_price("not list", "12345", "key")
        Traceback (most recent call last):
            ...
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновляет остатки товаров в магазине Ozon.

    Args:
        stocks (list): Список словарей с данными об остатках
        client_id (str): ID клиента в системе Ozon.
        seller_token (str): API-токен для авторизации.

    Returns:
        dict: Ответ API после обновления остатков

    Examples:
        >>> update_stocks([{"offer_id":"A", "stock": 5}], "client", "token")
        {'result': {...}}

    Incorrect:
        >>> update_stocks(not list, "12345", "key")
        Traceback (most recent call last):
            ...
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачивает и обрабатывает файл с остатками товаров с сайта Casio.

    Downloads ZIP-архив, извлекает Excel-файл и преобразует в список
    словарей.

    Returns:
        list: Список словарей с информацией о товарах и остатках.

    Examples:
        >>> data = download_stock()
        >>> isinstance(data, list)
        True

    Incorrect:
        >>> download_stock()
        Traceback (most recent call last):
            ...
    """
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Создает структуру данных для обновления остатков в Ozon.

    Args:
        watch_remnants (list): Список товаров из файла остатков.
        offer_ids (list): Список артикулов товаров в магазине Ozon.

    Returns:
        list: Список словарей для отправки в API Ozon.

    Examples:
        >>> create_stocks([{"Код": "123", "Количество": "5"}], ["123"])
        [{'offer_id': '123', 'stock': 5}]

    Incorrect:
        >>> create_stocks("not list", ["123"])
        Traceback (most recent call last):
            ...
    """
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создает структуру данных для обновления цен в Ozon.

    Проходит по watch_remnants и для каждого найденного артикула
    формирует словарь,
    где 'price' проходит через price_conversion (строка -> только цифры).

    Args:
        watch_remnants (list): Список товаров из файла остатков.
        offer_ids (list): Список артикулов товаров в магазине Ozon.

    Returns:
        list: Список словарей для отправки в API Ozon.

    Examples:
        >>> remnants = [{'Код': '123', 'Цена': "5'990.00 руб."}]
        >>> offers = ['123', '456']
        >>> create_prices(remnants, offers)
        [{'offer_id': '123', 'price': '5990', 'currency_code': 'RUB'}]

    Incorrect:
        >>> create_prices("not list", ["123"])
        Traceback (most recent call last):
            ...
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразует строку с ценой в числовой формат без разделителей.

    Функция очищает строку цены от всех нечисловых символов и дробной части,
    оставляя только целое число. Используется для подготовки цен к загрузке
    в API маркетплейсов.

    Args:
        price (str): Строка с ценой, например `"5'990.00 руб."`.

    Returns:
        str: Строка, содержащая только цифры. Например, `"5990"`.

    Examples:
        >>> price_conversion("5'990.00 руб.")
        '5990'

    Incorrect:
        >>> price_conversion(5990)
        Traceback (most recent call last):
            ...
        AttributeError: 'int' object has no attribute 'split'
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделяет список на части заданного размера.

    Args:
        lst (list): Исходный список для разделения.
        n (int): Размер каждой части.

    Yields:
        list: Очередная часть списка размера n.

    Examples:
        >>> list(divide([1,2,3,4], 2))
        [[1,2], [3,4]]

    Incorrect:
        >>> divide("abc", 2)
        Traceback (most recent call last):
            ...
    """
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Асинхронно загружает цены товаров в Ozon.

    Args:
        watch_remnants (list): Список товаров из файла остатков.
        client_id (str): ID клиента в системе Ozon.
        seller_token (str): API-токен для авторизации.

    Returns:
        list: Список обновленных цен.

    Examples:
        >>> await upload_prices(remnants, "client123", "token456")
        [{'offer_id': '123', 'price': '5990', ...}]
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Асинхронно загружает остатки товаров в Ozon.

    Args:
        watch_remnants (list): Список товаров из файла остатков.
        client_id (str): ID клиента в системе Ozon.
        seller_token (str): API-токен для авторизации.

    Returns:
        tuple: Кортеж (непустые остатки, все остатки).

    Examples:
        >>> not_empty, all_stocks = await upload_stocks(remnants,
        "client123", "token456")
        >>> len(not_empty)
        15
        >>> len(all_stocks)
        25
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """Основная точка входа: считывает переменные окружения и
    выполняет обновление остатков и цен.

       Ожидаемый набор переменных окружения:
       - SELLER_TOKEN
       - CLIENT_ID

       При ошибках сети печатает диагностические сообщения.

       Examples:
           >>> python seller.py
    """
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
