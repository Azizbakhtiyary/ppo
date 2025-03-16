import asyncio
import json
import websockets
import pandas as pd
from datetime import datetime

BINANCE_WS_URL = "wss://fstream.binance.com/ws"

# Глобальные переменные для хранения данных
order_book_df = pd.DataFrame(columns=["timestamp", "bid_price", "bid_size", "ask_price", "ask_size"])
tick_trades_df = pd.DataFrame(columns=["timestamp", "price", "volume", "side"])


async def listen_order_book(symbol="btcusdt"):
    """ Получаем DOM (Depth of Market) и записываем в DataFrame """
    global order_book_df
    stream_name = f"{symbol}@depth5@100ms"  # 5 уровней книги с обновлением каждые 100 мс
    url = f"{BINANCE_WS_URL}/{stream_name}"
    
    async with websockets.connect(url) as ws:
        while True:
            response = await ws.recv()
            data = json.loads(response)
            
            bids = data["b"][:5]  # ТОП-5 заявок покупателей
            asks = data["a"][:5]  # ТОП-5 заявок продавцов
            timestamp = datetime.now()

            # Берем лучший бид и лучший аск
            best_bid_price, best_bid_size = bids[0]
            best_ask_price, best_ask_size = asks[0]

            # Добавляем в DataFrame
            new_row = pd.DataFrame([[timestamp, best_bid_price, best_bid_size, best_ask_price, best_ask_size]],
                                   columns=order_book_df.columns)
            order_book_df = pd.concat([order_book_df, new_row], ignore_index=True).dropna(axis=1, how='all')

            if len(order_book_df) % 30 == 0:  # Вывод каждые 10 обновлений
                print(f"🔥 DOM Update: Bid {best_bid_price} / Ask {best_ask_price}")


async def listen_tick_volume(symbol="btcusdt"):
    """ Получаем тик-сделки и записываем в DataFrame """
    global tick_trades_df
    stream_name = f"{symbol}@trade"  
    url = f"{BINANCE_WS_URL}/{stream_name}"

    async with websockets.connect(url) as ws:
        while True:
            response = await ws.recv()
            trade = json.loads(response)

            price = trade["p"]
            volume = trade["q"]
            side = "BUY" if trade["m"] == False else "SELL"  # m=False → покупка, m=True → продажа
            timestamp = datetime.now()

            # Добавляем в DataFrame
            new_row = pd.DataFrame([[timestamp, price, volume, side]],
                                   columns=tick_trades_df.columns)
            tick_trades_df = pd.concat([tick_trades_df, new_row], ignore_index=True).dropna(axis=1, how='all')

            if len(tick_trades_df) % 15 == 0:  # Вывод каждые 5 тик-трейдов
                print(f"⚡ Trade: {side} {volume} BTC at {price}")


async def main():
    """ Запускаем WebSocket-сессии одновременно """
    await asyncio.gather(
        listen_order_book(),
        listen_tick_volume(),
        save_and_print_data()  # Выведем данные через 10 сек
    )

import time

async def save_and_print_data():
    """Ждём 10 секунд, затем выводим последние записи из DataFrame"""
    await asyncio.sleep(10)  # Даем время собраться данным

    print("\n🔥 Последние 5 записей DOM:")
    print(order_book_df.tail())  # Вывести последние 5 строк DOM

    print("\n⚡ Последние 5 записей тик-сделок:")
    print(tick_trades_df.tail())  # Вывести последние 5 строк тик-сделок



if __name__ == "__main__":
    asyncio.run(main())

# Выводим последние 5 записей DOM
print("\n🔥 Последние 5 записей DOM:")
print(order_book_df.tail())

# Выводим последние 5 записей тик-сделок
print("\n⚡ Последние 5 записей тик-сделок:")
print(tick_trades_df.tail())

