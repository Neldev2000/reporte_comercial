import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler

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


app = Client("my_account", api_id = api['id'], api_hash=api['hash'], bot_token=api['token'])
def construccion_mensaje():
    conn_stock = pg.connect(os.environ.get('STOCK_URL'))
    conn_reporte = pg.connect(os.environ.get('SALES_URL'))
    data_stock = pd.read_sql(sql=kits, con=conn_stock)
    data_atlas = pd.read_sql(sql = reporte, con = conn_reporte)
    data = data_atlas.join(data_stock.set_index('fecha'), on = 'fecha', lsuffix='', rsuffix='')
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
    elif message.chat.id == chats['almacen']:
        num = int(message.text)
        print(num)
        conn = pg.connect(os.environ.get('STOCK_URL'))
        cur = conn.cursor()
        fecha = pd.Timestamp.now()
        cur.execute('INSERT INTO kits_instalacion (fecha, kits) VALUES (%s, %s)', (fecha, num))
        # Confirme los cambios
        conn.commit()

        # Cierre el cursor y la conexión
        cur.close()
        conn.close()

        await client.send_message(chats['almacen'], 'Mensaje recibido😎😎')

async def enviar_reporte():
    msg = construccion_mensaje()
    chat_id = chats['gerencia']
    await app.send_message(chat_id = chat_id, text=msg)

async def enviar_recordatorio():
    msg = 'Recuerda cargar el numero de kits.'
    chat_id = chats['almacen']
    await app.send_message(chat_id=chat_id, text=msg)


scheduler = AsyncIOScheduler()
scheduler.add_job(enviar_reporte, "cron",day_of_week='mon,wed,fri', hour='16', minute='45')
scheduler.add_job(enviar_recordatorio, "cron",day_of_week='mon,wed,fri', hour='16', minute='00')

scheduler.start()
app.run()
