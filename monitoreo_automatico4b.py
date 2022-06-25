import fdb
import threading
from threading import Thread, Lock
import telegram
#import pywhatkit as kit
import datetime
import time
from itertools import groupby
from operator import itemgetter
from twilio.rest import Client
from twilio.twiml.voice_response import Play, VoiceResponse, Say, Dial, Number
import queue

bot_token = '1464667317:AAGF1qjunnKbyibYotOk9KYTtRzhlEFIhPs'
# chatid='1256819397'
bot = telegram.Bot(token=bot_token)

con = fdb.connect(dsn='127.0.0.1:C:/firebird/monitoreofb.fdb',
                  user='sysdba', password='masterkey')
cur = con.cursor()

# lista de variables globales
inicio = time.time()
chatid1 = '1256819397'  # iphone luciano
chatid2 = '76848454'  # nova
chatid3 = '1938022118'  # note4 galaxy
chatid4 = '107723150'  # note 7 xiaomi


# funcion para enviar mensajes por telegram:
def luk(notad, chatid):
    # if chatid != '':
    try:
        bot.sendMessage(chatid, text=notad)
    except BaseException as error:
        print("no se pudo enviar mensaje", error)

    try:
        bot.sendMessage(chatid2, text=notad)  # nova
    except BaseException as error:
        print("no se pudo enviar mensaje", error)

    try:
        # bot.sendMessage(chatid3, text=notad)  # luciano note4
        bot.sendMessage(chatid4, text=notad)  # luciano xiaomi
    except BaseException as error:
        print("no se pudo enviar mensaje", error)


# funcion para hacer llamadas por twilio:
def udpmensaje(telefono, msgFromClient):

    account_sid = 'ACee2f202ef9a1b3c71d5772af35b9caad'
    auth_token = '9f2abf6fbb46bf0a6f5512559379816a'
    client = Client(account_sid, auth_token)
    telefono = '+1'+telefono
    response = VoiceResponse()
    response.say(msgFromClient, voice="woman", language="es-MX", loop=2)
    #response.say(msgFromClient, voice="Polly.Miguel", loop=2)
    dial = Dial()
    dial.number(telefono, status_callback_event='busy')
    print(telefono)

    try:

        call = client.calls.create(
            # twiml='<Response><Say voice = "Alice" language = "es-MX"> HOLA, LE HABLAMOS DE' message '</Say>  <Play>http://luciano-casa.dyndns.biz:8080/mensajealarma.mp3</Play></Response>',
            # url='http://luciano-casa.dyndns.biz:8080/mensajealarma.mp3',
            twiml=response,
            to=telefono,
            from_='+12057367682'
        )
        print(call.sid)

        #sock.sendto(response_bytes, address)
    except BaseException as error:
        print("no se pudo realizar la llamada", error)


def tiempo(q):
    # id_hilo = threading.current_thread().ident
    # name_hilo = threading.current_thread().name
    # print("ID HILO: ", id_hilo)
    # print("nombre: ", name_hilo)
    while True:
        try:
            csid = q.get(block=False)
            print(csid)
        except queue.Empty:
            print("la cola esta vacia")
            break

        time.sleep(30)

        try:
            cur.execute(""" select acmsub.csid, clientes, acmsub.telefono, evento.detalle, activas.fechan as fech, nombre, listcall.telefono as tel1, acmsub.telegram_id from acmsub left join activas on acmsub.csid = activas.csid left join listcall on acmsub.csid = listcall.csid left join evento on activas.evento = evento.evento where COMPLETA = 'PENDIENTE' and acmsub.csid = '%s' and listcall.callnumber = 'A'  order by alarmnum  """ % csid)
            row1 = cur.fetchall()

        except BaseException as error:
            print(
                "en tiempo ocurrio un error con la informacion de la base de datos", error)

        else:
            resultado = []
            for k, g in groupby(row1, itemgetter(0)):
                g = list(g)           # Convertir el grupo en una lista
                record = list(g[0])   # Copiar el primer registro del grupo
                # Y ahora reemplazar en ese registro los campos 3 y 4
                # <----- AQUI se añade set()
                record[3] = "ROBO " + \
                    " y ".join(set(t[3].replace("ROBO", "") for t in g))
                record[4] = max(t[4] for t in g)
                # Añadir ese registro (como tupla) al resultado
                resultado.append(tuple(record))

                for j in range(len(resultado)):

                    # asignar variables a los resultados de la consulta de la base de datos
                    csid, cliente, telprincipal, eventodetalle, fecha, nombrellamada, tel1llamada, chatid = resultado[
                        j][0:8]

                    if chatid is None:
                        chatid = '1938022118'
                    elif chatid == '':
                        chatid = '1938022118'
                    else:
                        print("chatid")
                        print(chatid)

                    # crear mensaje que se va a enviar al cliente y el que se se la a dar en mensaje de voz por twilio
                    msgFromClient = 'COMPU ALARMA 809-547-8958 ALARMA DE ' + \
                        str(cliente) + ' ' + \
                        str(eventodetalle) + '  FECHA ' + \
                        str(fecha.strftime("%d-%m-%Y  HORA %I:%M:%S %p"))
                    print(msgFromClient, "\n")

                    # enviamos el mensaje por telegram segun el bot id del cliente y a la administracion con la funcion luk
                    try:
                        hilo2 = Thread(name='hilo%s' % csid, target=luk,
                                       args=(msgFromClient, chatid, ))
                        hilo2.start()
                        hilo2.join()
                    except BaseException as error:
                        print("no se pudo enviar telegram", error)

                    # hacemos llamada telefonica por twilio con la funcion udpmensaje
                    try:
                        hilo5 = Thread(name='hilo%s' % csid, target=udpmensaje, args=(
                            tel1llamada, msgFromClient,))
                        hilo5.start()
                        hilo5.join()
                    except BaseException as error:
                        print("no se pudo hacer llamada", error)

                    # completada le asignamos el valor completada a el campo completa de la base de datos
                    try:
                        print('este es el {}'.format(csid))
                        cur.execute(
                            "update activas set COMPLETA = 'COMPLETADA' where completa = 'PENDIENTE' and csid='{}'".format(csid))
                        # cur.execute(
                        #     "update activas set COMPLETA = 'COMPLETADA' where completa = 'PENDIENTE' and csid= '%s'", (csid,))
                        con.commit()
                        # con.close()

                    except BaseException as error:
                        print("no se pudo realizar el update", error)

                    # borramos del conjunto llamado alarmas el id del cliente que procesamos
                    alarmas.discard(csid)
                    break
                break


q = queue.Queue()

alarmas = set()
while True:
    with con.event_conduit(['nueva_activa']) as conduit:
        events = conduit.wait()
        print(events)
        try:
            sql1 = "select first 1 csid from activas order by alarmnum desc"
            cur.execute(sql1)
            row = cur.fetchall()
            for h in row:
                csid = h[0]
        except BaseException as error:
            print(
                "en evento ocurrio un error con la informacion de la base de datos", error)

        else:
            if csid not in alarmas:

                q.put(csid)
                alarmas.add(csid)
                print(alarmas)
                n_hilos = threading.active_count()
                # print("hilos activos: ", n_hilos)
                if n_hilos < 3:
                    hilocsid = Thread(target=tiempo, args=(q,))
                    hilocsid.start()

                print("hilos activos: ", n_hilos)
