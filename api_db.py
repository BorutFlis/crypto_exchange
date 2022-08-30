from abc import ABC, abstractmethod
import requests
import asyncio
import datetime
from requests.auth import HTTPBasicAuth
import ccxt
import pandas as pd
from dataclasses import dataclass
from typing import Optional
import time
from sqlalchemy import create_engine
import sqlalchemy
import xlwings as xw


class Auth(ABC):

    @abstractmethod
    def get_from_endpoint(self, string):
        """Get JSON from specific endpoint

         Add endpoint address after the url, include the parameters.
        """
        pass

    @abstractmethod
    def post_to_endpoint(self, string, params):
        """Post Request to specific endpoint

         Add endpoint address and include the parameters in a dictionary.
        """
        pass


class data_writer(ABC):

    @abstractmethod
    def write(self, table, name):
        """write into the data-source"""

    @abstractmethod
    def commit(self):
        """commit changes"""


class XwWriter(data_writer):

    def __init__(self, filename):
        self.filename = filename
        self.workbook = xw.Book()

    def write(self, table, name):
        sheet = self.workbook.sheets.add(name)
        sheet["A1"].value = table.values

    def commit(self):
        self.workbook.save(self.filename)


class db_writer(data_writer):

    def __init__(self, connection_string):
        self.engine = create_engine(connection_string, echo=False)

    def write(self, table, name):
        table.to_sql(name, con=self.engine, dtype=self.sqlcol(table))

    def commit(self):
        pass

    def sqlcol(self, df):
        dtypedict = {}
        for i, j in zip(df.columns, df.dtypes):
            if "object" in str(j):
                dtypedict.update({i: sqlalchemy.types.NVARCHAR(length=255)})

            if "datetime" in str(j):
                dtypedict.update({i: sqlalchemy.types.DateTime()})

            if "float" in str(j):
                dtypedict.update({i: sqlalchemy.types.Float(precision=3, asdecimal=True)})

            if "int" in str(j):
                dtypedict.update({i: sqlalchemy.types.INT()})

        return dtypedict

class excel_writer(data_writer):

    def __init__(self, filename):
        self.filename = filename
        self.w = pd.ExcelWriter("new.xlsx")

    def write(self, table, name):
        table.to_excel(self.w, sheet_name=name)

    def commit(self):
        self.w.close()


class ApiDbHelpers:

    def basic_get_from_endpoint(self, string, params=None, headers=None):
        return requests.get(f"{self.url}/{string}", auth=self.auth, json=params, headers=headers)

    def basic_post_to_endpoint(self, string, params=None, headers=None):
        return requests.post(f"{self.url}/{string}", auth=self.auth, json=params, headers=headers)


class api_db(ABC):

    @abstractmethod
    def get_from_endpoint(self, string):
        """Get JSON from specific endpoint

         Add endpoint address after the url, include the parameters.
        """
        pass

    @abstractmethod
    def post_to_endpoint(self, string, params):
        """Post Request to specific endpoint

         Add endpoint address and include the parameters in a dictionary.
        """
        pass

    def db(self):
        pass

    def print_credentials(self):
        print(self.auth.username)

#TODO: exchange helpers that optionally inherit


class BinanceExchange(api_db):
    """connect to Binance"""

    def __init__(self, **kwargs):
        self.auth = {
            "api-key": kwargs["api_key"],
            "api-secret": kwargs["api_secret"]
        }
        self.exchange = ccxt.binance({
            'apiKey': kwargs["api_key"],
            'secret': kwargs["api_secret"],
            'enableRateLimit': True,  # https://github.com/ccxt/ccxt/wiki/Manual#rate-limit
            'options': {
                'defaultType': 'future',
            },
        })
        self.exchange.set_sandbox_mode(True)

    def order_wait(self, symbol: str = "BTC/USDT", order_type: str = "MARKET", side: str = "BUY", quantity: float = 0.01):
        """An order that waits to be filled

        Blocks the scripts and make queries in 1 second intervals.
        """
        order = self.exchange.create_order(symbol, order_type, side, quantity)
        # TODO: add the other posibilities to unfilled orders
        while True:
            query_order = self.exchange.fetch_order(order['id'], symbol=symbol)
            if query_order["info"]["status"] == "FILLED":
                return query_order
            time.sleep(1)

    def set_stop(
        self,
        stop_price: float,
        symbol: str = "BTC/USDT",
        order_type: str = "STOP_MARKET",
        side: str = "BUY",
        quantity: float = 0.01,
        adjustment: float = 0.0001
    ):
        multiplier = 1 + adjustment if side == "buy" else 1 - adjustment
        while True:
            try:
                print(f"trying {stop_price}")
                order = self.exchange.createOrder(symbol, order_type, side, quantity, params={"stopPrice": stop_price})
            except ccxt.errors.OrderImmediatelyFillable:
                stop_price *= multiplier
            else:
                return order

    def get_from_endpoint(self, string):
        """Get JSON from specific endpoint

         Add endpoint address after the url, include the parameters.
        """
        pass

    def post_to_endpoint(self, string, params):
        """Post Request to specific endpoint

         Add endpoint address and include the parameters in a dictionary.
        """
        pass

@dataclass
class ExanteExchange(api_db):

    url: str
    auth: list
    writer: Optional[data_writer] = None

    get_from_endpoint = ApiDbHelpers.basic_get_from_endpoint
    post_to_endpoint = ApiDbHelpers.basic_post_to_endpoint

    @property
    def auth(self):
        return self._auth

    @auth.setter
    def auth(self, value):
        #TODO: check validity
        if True:
            self._auth = HTTPBasicAuth(value[0], value[1])
        else:
            raise Exception("Invalid Auth details")


@dataclass
class sfox_exchange(api_db):

    #TODO: should have a translation from limit-> id and so on

    url: str
    auth: str
    currency: str = "btcusd"
    writer: Optional[data_writer] = None

    get_from_endpoint = ApiDbHelpers.basic_get_from_endpoint
    post_to_endpoint = ApiDbHelpers.basic_post_to_endpoint

    @property
    def auth(self):
        return self._auth

    @auth.setter
    def auth(self, value):
        #TODO: check validity
        if True:
            self._auth = HTTPBasicAuth(value, "")
        else:
            raise Exception("Invalid Auth details")

    def print_credentials(self):
        super().print_credentials()
        sfox_info = requests.get("https://api.sfox.com/v1/user/balance", auth=self.auth).json()
        print(sfox_info)

    async def order_adapting_price(self, params: dict, cancel_id: int, side: str, percent_adjustment: float, price: float) -> object:
        while True:
            cancel = requests.delete(
                "https://api.sfox.com/v1/orders",
                auth=self.auth,
                params={
                    "ids": f"{cancel_id}"
                }
            ).json()
            price_adjustment = 1 + percent_adjustment if side == "buy" else 1 - percent_adjustment
            price *= price_adjustment
            new_order = requests.post(
                f"https://api.sfox.com/v1/orders/{side}",
                auth=self.auth,
                json={**params, **{"price": price}}
            ).json()
            await asyncio.sleep(2)
            query = requests.get(
              f"https://api.sfox.com/v1/orders/{new_order['id']}",
              auth=self.auth
            ).json()
            cancel_id = new_order["id"]
            if query['status'] in ["Filled", "Done"]:
                return query


    async def order_wait(self, algorithm_id, quantity, price, side, wait_time=10, cancellation="c"):
        order = requests.post(
            f"https://api.sfox.com/v1/orders/{side}",
            auth=self.auth,
            json={
                "quantity": quantity,
                "currency_pair": self.currency,
                "price": price,
                "algorithm_id": algorithm_id
            }
        ).json()

        start_time = datetime.datetime.now()
        while True:
            await asyncio.sleep(1)
            query = requests.get(
                f"https://api.sfox.com/v1/orders/{order['id']}",
                auth=self.auth
            ).json()
            if query['status'] in ["Filled", "Done"]:
                return query
            elif (datetime.datetime.now() - start_time).seconds > wait_time:
                cancel_id = order['id']
                if cancellation == "c":
                    cancel = requests.delete(
                        "https://api.sfox.com/v1/orders",
                        auth=self.auth,
                        params={
                            "ids": f"{cancel_id}"
                        }
                    ).json()
                    print(f"Not Filled in time and cancelled")
                    return query
                elif cancellation == "desc":
                    return await self.order_adapting_price(algorithm_id, cancel_id, quantity, side, 0.005, price)
                else:
                    print("Not Filled in time, script continues.")
                    return query

class globe_exchange(api_db):

    def __init__(self, **kwargs):
        self._AUTH = {
            "api-key": kwargs["api-key"],
            "passphrase": kwargs["passphrase"],
            "secret": kwargs["secret"]
        }

