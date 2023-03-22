import logging
from telegram import Update
from telegram.ext import filters, MessageHandler,ApplicationBuilder, ContextTypes, CommandHandler

#datos de database_stock
import psycopg2 as pg
import pandas as pd
import datetime
#postgres://kits_instalacion_user:nwaCknYUE8SVgCVx6Eh0LQceYdehSuP6@dpg-cgd11oceoogljts1sfjg-a.oregon-postgres.render.com/kits_instalacion
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
chats = {
    'Miguel' : 1427866381,
    'Julio'  : 457625276,
    'Nelson' : 1397110563
}
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

with open('queries/reporte.sql') as f, open('queries/kits.sql') as r:
    reporte = f.read()
    kits = r.read()
    f.close()
    r.close()


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(chat_id=update.effective_chat.id, text="I'm a bot, please talk to me!")
async def cargado_kits(update: Update, context: ContextTypes.DEFAULT_TYPE):
    num = int(update.message.text)
    print(f'Kits en almacen: {num}')
    chat_id = update.effective_chat.id
    if chat_id == chats['Miguel']:
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
        await context.bot.send_message(chat_id=chats['Miguel'], text="Dato cargado")
        pass
    #await context.bot.send_message(chat_id=update.effective_chat.id, text=update.message.text)

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
async def envio_reporte(context: ContextTypes.DEFAULT_TYPE):
    msg = construccion_mensaje()
    print(msg)
    await context.bot.send_message(chat_id=chats['Julio'], text=msg)
def set_timer_gerente(update, context):
    context.job_queue.run_daily(envio_reporte, days=(0, 2, 4), time=datetime.time(hour=16, minute=45, second=0), context=update.message.chat_id)

async def envio_auxiliar(update: Update, context: ContextTypes.DEFAULT_TYPES):
    msg = construccion_mensaje()
    print(f"Auxiliar: \n\n{msg}")
    await context.bot.send_message(chat_id=update.effective_chat.id, text=msg)

async def recordatorio_kits(context: ContextTypes.DEFAULT_TYPE):
    msg =  'Por favor recuerda cargar el numero de kits😎😎'
    await context.bot.send_message(chat_id=chats['Miguel'], text=msg)
def set_timer_almacen(update, context):
    context.job_queue.run_daily(envio_reporte, days=(0, 2, 4), time=datetime.time(hour=15, minute=30, second=0), context=update.message.chat_id)

if __name__ == '__main__':
    application = ApplicationBuilder().token('5893714044:AAHNgtoFnl_BKZQ2frmX4dc32t1SjFuI-74').build()
    
    start_handler = CommandHandler('start', start)
    auxiliar_handler = CommandHandler('send', envio_auxiliar)
    set_gerente_handler = CommandHandler('set', set_timer_gerente)
    set_almacen_handler = CommandHandler('set_almacen', set_timer_almacen)
    echo_handler = MessageHandler(filters.TEXT & (~filters.COMMAND), cargado_kits)
    
    application.add_handler(start_handler)
    application.add_handler(auxiliar_handler)

    application.add_handler(set_gerente_handler)
    application.add_handler(set_almacen_handler)

    application.add_handler(echo_handler)
    
    application.run_polling()