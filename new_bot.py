import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler

import psycopg2 as pg
import pandas as pd

from pyrogram import Client
from pyrogram.types import Message

database_stock = {
    'dbname' : "kits_instalacion",
    'host' : 'dpg-cgd11oceoogljts1sfjg-a.oregon-postgres.render.com',
    'port' : 5432,
    'user' : 'kits_instalacion_user',
    'password' : 'nwaCknYUE8SVgCVx6Eh0LQceYdehSuP6'
}
database_atlas = {
    'dbname' : "tenant_viginet_yzi0ia1v",
    'host' : '190.97.235.2',
    'port' : 5432,
    'user' : 'consultor01',
    'password' : '$K10F2#q6nt5'
}
# miguel 1427866381
chats = {
    'Miguel' : 1427866381,
    'Julio'  : 457625276,
    'Nelson' : 1397110563
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

api = {
    'id' : 15560437,
    'hash' : 'b8215bf1fb018f7611f3e98cb3f98a3d',
    'token' : '5893714044:AAHNgtoFnl_BKZQ2frmX4dc32t1SjFuI-74'
}
app = Client("my_account", api_id = api['id'], api_hash=api['hash'], bot_token=api['token'])
def construccion_mensaje():
    conn_stock = pg.connect(**database_stock)
    conn_reporte = pg.connect(**database_atlas)
    data_stock = pd.read_sql(sql=kits, con=conn_stock)
    data_atlas = pd.read_sql(sql = reporte, con = conn_reporte)
    data = data_atlas.join(data_stock.set_index('fecha'), on = 'fecha', lsuffix='', rsuffix='')
    result = f"""
        Sucursal Merida
    -------------------

        1️⃣*Acumulado de ventas del 1 al n de mes corriente:* {data['ventas_mes'].values[0]}

        2️⃣*Acumulado de instalaciones realizadas (del 1 al n de mes corriente):* {data['instalaciones_mes'].values[0]}

        3️⃣*Instalaciones pendientes:* {data['pendientes'].values[0]}

        4️⃣*Kit de instalación disponible en almacen al día de hoy 17 de marzo:* {data['kits'].values[0]}
    """
    return result

"""
@app.on_message()
async def obtener_kits(client, message):
    if message.chat.id == chats['Miguel']:
        num = int(message.text)
        print(num)
        conn = pg.connect(**database_stock)
        cur = conn.cursor()
        fecha = pd.Timestamp.now()
        cur.execute('INSERT INTO kits_instalacion (fecha, kits) VALUES (%s, %s)', (fecha, num))
        # Confirme los cambios
        conn.commit()

        # Cierre el cursor y la conexión
        cur.close()
        conn.close()

        await client.send_message(chats['Miguel'], 'Mensaje recibido😎😎')  
"""
@app.on_message()
async def mensaje_auxiliar(client, message):
    if message.text == '/send':
        msg = construccion_mensaje()
        await client.send_message(message.chat.id, msg)
    elif message.chat.id == chats['Miguel']:
        num = int(message.text)
        print(num)
        conn = pg.connect(**database_stock)
        cur = conn.cursor()
        fecha = pd.Timestamp.now()
        cur.execute('INSERT INTO kits_instalacion (fecha, kits) VALUES (%s, %s)', (fecha, num))
        # Confirme los cambios
        conn.commit()

        # Cierre el cursor y la conexión
        cur.close()
        conn.close()

        await client.send_message(chats['Miguel'], 'Mensaje recibido😎😎')

async def enviar_reporte():
    msg = construccion_mensaje()
    chat_id = chats['Julio']
    await app.send_message(chat_id = chat_id, text=msg)

async def enviar_recordatorio():
    msg = 'Recuerda cargar el numero de kits.'
    chat_id = chats['Miguel']
    await app.send_message(chat_id=chat_id, text=msg)


scheduler = AsyncIOScheduler()
scheduler.add_job(enviar_reporte, "cron",day_of_week='mon,wed,fri', hour='16', minute='45')
scheduler.add_job(enviar_recordatorio, "cron",day_of_week='mon,wed,fri', hour='16', minute='00')

scheduler.start()
app.run()