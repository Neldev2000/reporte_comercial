import logging
import uvloop
import psycopg2 as pg
import pandas as pd

from pyrogram import Client

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
with open('queries/reporte.sql') as f, open('queries/kits.sql') as r:
    reporte = f.read()
    kits = r.read()
    f.close()
    r.close()

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


uvloop.install()
app = Client("my_account", api_id = api['id'], api_hash=api['hash'], bot_token=api['token'])
def construccion_mensaje():
    conn_reporte = pg.connect(os.environ.get('SALES_URL'))
   
    data_atlas = pd.read_sql(sql = reporte, con = conn_reporte)
    data = data_atlas
    result = f"""
        Sucursal Merida
    -------------------

        1️⃣*Acumulado de ventas del 1 al n de mes corriente:* {data['ventas_mes'].values[0]}

        2️⃣*Acumulado de instalaciones realizadas (del 1 al n de mes corriente):* {data['instalaciones_mes'].values[0]}

        3️⃣*Instalaciones pendientes:* {data['pendientes'].values[0]}
    """
    return result

@app.on_message()
async def mensaje_auxiliar(client, message):
    if message.text == '/send':
        msg = construccion_mensaje()
        await client.send_message(message.chat.id, msg)
app.run()
