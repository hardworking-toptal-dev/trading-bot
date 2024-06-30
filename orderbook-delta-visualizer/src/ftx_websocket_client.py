import hmac
import json
import time
import zlib
from collections import defaultdict, deque
from gevent.event import Event
from itertools import zip_longest
from typing import DefaultDict, Deque, List, Dict, Tuple, Optional

from src.ftx_websocket_manager import WebsocketManager


class FtxWebsocketClient(WebsocketManager):
    _ENDPOINT = 'wss://ftx.com/ws/'

    def __init__(self) -> None:
        super().__init__()
        self._trades: DefaultDict[str, Deque[Dict]] = defaultdict(lambda: deque(maxlen=10000))
        self._fills: Deque[Dict] = deque(maxlen=10000)
        self._api_key: str = ''  # TODO: Place your API key here
        self._api_secret: str = ''  # TODO: Place your API secret here
        self._orderbook_update_events: DefaultDict[str, Event] = defaultdict(Event)
        self._reset_data()

    def _on_open(self, ws) -> None:
        self._reset_data()

    def _reset_data(self) -> None:
        self._subscriptions: List[Dict] = []
        self._orders: DefaultDict[int, Dict] = defaultdict(dict)
        self._tickers: DefaultDict[str, Dict] = defaultdict(dict)
        self._orderbook_timestamps: DefaultDict[str, float] = defaultdict(float)
        self._orderbooks: DefaultDict[str, Dict[str, DefaultDict[float, float]]] = defaultdict(
            lambda: {side: defaultdict(float) for side in {'bids', 'asks'}})
        self._logged_in = False
        self._last_received_orderbook_data_at: float = 0.0
        self._orderbook_update_events.clear()

    def _reset_orderbook(self, market: str) -> None:
        self._orderbooks.pop(market, None)
        self._orderbook_timestamps.pop(market, None)

    def _get_url(self) -> str:
        return self._ENDPOINT

    def _login(self) -> None:
        ts = int(time.time() * 1000)
        sign_payload = f'{ts}websocket_login'.encode()
        sign = hmac.new(self._api_secret.encode(), sign_payload, 'sha256').hexdigest()
        self.send_json({
            'op': 'login',
            'args': {
                'key': self._api_key,
                'sign': sign,
                'time': ts,
            }
        })
        self._logged_in = True

    def _subscribe(self, subscription: Dict) -> None:
        self.send_json({'op': 'subscribe', **subscription})
        self._subscriptions.append(subscription)

    def _unsubscribe(self, subscription: Dict) -> None:
        self.send_json({'op': 'unsubscribe', **subscription})
        self._subscriptions = [sub for sub in self._subscriptions if sub != subscription]

    def get_fills(self) -> List[Dict]:
        self._ensure_logged_in()
        self._ensure_subscribed({'channel': 'fills'})
        return list(self._fills)

    def get_orders(self) -> Dict[int, Dict]:
        self._ensure_logged_in()
        self._ensure_subscribed({'channel': 'orders'})
        return dict(self._orders)

    def get_trades(self, market: str) -> List[Dict]:
        self._ensure_subscribed({'channel': 'trades', 'market': market})
        return list(self._trades[market])

    def get_orderbook(self, market: str) -> Dict[str, List[Tuple[float, float]]]:
        self._ensure_subscribed({'channel': 'orderbook', 'market': market})
        if self._orderbook_timestamps[market] == 0:
            self.wait_for_orderbook_update(market, 5)
        return self._get_sorted_orderbook(market)

    def get_orderbook_timestamp(self, market: str) -> float:
        return self._orderbook_timestamps[market]

    def wait_for_orderbook_update(self, market: str, timeout: Optional[float] = None) -> None:
        self._ensure_subscribed({'channel': 'orderbook', 'market': market})
        self._orderbook_update_events[market].wait(timeout)

    def get_ticker(self, market: str) -> Dict:
        self._ensure_subscribed({'channel': 'ticker', 'market': market})
        return self._tickers[market]

    def _handle_orderbook_message(self, message: Dict) -> None:
        market = message['market']
        data = message['data']

        if data['action'] == 'partial':
            self._reset_orderbook(market)
        
        for side in {'bids', 'asks'}:
            book = self._orderbooks[market][side]
            for price, size in data[side]:
                if size:
                    book[price] = size
                else:
                    book.pop(price, None)
        
        self._orderbook_timestamps[market] = data['time']
        if not self._verify_checksum(data, market):
            self._resubscribe_orderbook(market)
        else:
            self._orderbook_update_events[market].set()
            self._orderbook_update_events[market].clear()

    def _verify_checksum(self, data: Dict, market: str) -> bool:
        checksum = data['checksum']
        orderbook = self._get_sorted_orderbook(market)
        checksum_data = [
            f'{order[0]}:{order[1]}' for bid, ask in zip_longest(orderbook['bids'][:100], orderbook['asks'][:100])
            for order in (bid, ask) if order
        ]
        computed_result = int(zlib.crc32(':'.join(checksum_data).encode()))
        return computed_result == checksum

    def _resubscribe_orderbook(self, market: str) -> None:
        self._reset_orderbook(market)
        self._unsubscribe({'market': market, 'channel': 'orderbook'})
        self._subscribe({'market': market, 'channel': 'orderbook'})

    def _handle_trades_message(self, message: Dict) -> None:
        self._trades[message['market']].append(message['data'])

    def _handle_ticker_message(self, message: Dict) -> None:
        self._tickers[message['market']] = message['data']

    def _handle_fills_message(self, message: Dict) -> None:
        self._fills.append(message['data'])

    def _handle_orders_message(self, message: Dict) -> None:
        data = message['data']
        self._orders[data['id']] = data

    def _on_message(self, ws, raw_message: str) -> None:
        message = json.loads(raw_message)
        message_type = message['type']
        if message_type in {'subscribed', 'unsubscribed'}:
            return
        elif message_type == 'info' and message['code'] == 20001:
            return self.reconnect()
        elif message_type == 'error':
            raise Exception(message)
        
        channel = message['channel']
        handler = {
            'orderbook': self._handle_orderbook_message,
            'trades': self._handle_trades_message,
            'ticker': self._handle_ticker_message,
            'fills': self._handle_fills_message,
            'orders': self._handle_orders_message
        }.get(channel)

        if handler:
            handler(message)

    def _ensure_logged_in(self) -> None:
        if not self._logged_in:
            self._login()

    def _ensure_subscribed(self, subscription: Dict) -> None:
        if subscription not in self._subscriptions:
            self._subscribe(subscription)

    def _get_sorted_orderbook(self, market: str) -> Dict[str, List[Tuple[float, float]]]:
        return {
            side: sorted(
                ((price, quantity) for price, quantity in self._orderbooks[market][side].items() if quantity),
                key=lambda order: order[0] * (-1 if side == 'bids' else 1)
            )
            for side in {'bids', 'asks'}
        }
