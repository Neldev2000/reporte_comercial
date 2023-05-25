import telethon
from telethon.events import NewMessage
import logging
import psycopg2 as pg
import pandas as pd

import asyncio

import os
# almacen 1427866381
chats = {
    'almacen' : os.environ.get('ALMACEN_ID'),
    'gerencia'  : os.environ.get('GERENCIA_ID'),
    'test' : os.environ.get('TEST_ID')
}
api = {
    'id' : os.environ.get('API_ID'),
    'hash' : os.environ.get('API_HASH'),
    'token' : os.environ.get('API_TOKEN')
}
# Create a Telegram client
async def main():
    client = telethon.TelegramClient('my_username', api_id=api['id'], api_hash=api['hash'])

    # Login to Telegram
    await client.connect()

    # Register a handler for incoming messages
    @client.on(NewMessage)
    async def handler(event):
        # Check the message text
        if event.text == 'reporte':
            # Send a message back to the user
            await event.respond('Hello!')
        elif event.text == 'goodbye':
            # Send a message back to the user
            await event.respond('Goodbye!')

    # Close the client
    await client.disconnect()
if __name__ == '__main__':
    # Run the main function
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())