import os
import json
import time
import requests
import pandas as pd
import warnings
from datetime import datetime, timedelta
from dotenv import load_dotenv
from paho.mqtt.client import Client, CallbackAPIVersion

# Игнорируем предупреждения openpyxl при чтении стилей
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# 1. Загрузка настроек из .env
load_dotenv()

# Настройки серверов
IP_SAIMAN = os.getenv("SAIMAN_IP")
IP_CHIRPSTACK = os.getenv("CHIRPSTACK_IP")

# MQTT конфигурация
MQTT_HOST = os.getenv("MQTT_HOST")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_USER = os.getenv("MQTT_USER")  # Может быть None
MQTT_PASS = os.getenv("MQTT_PASS")  # Может быть None

# Параметры команды
F_PORT = 8
DATA_B64 = "AAEAAQABABLDAYEAKAACGQkA/wIBBgAAAAE="

# Данные для авторизации
SAIMAN_AUTH = {
    'login': os.getenv("SAIMAN_USER"),
    'password': os.getenv("SAIMAN_PASS")
}
CHIRPSTACK_AUTH = {
    'email': os.getenv("CHIRPSTACK_USER"),
    'password': os.getenv("CHIRPSTACK_PASS")
}


def send_downlink_command(app_id, dev_eui):
    """Отправка команды на устройство через MQTT"""
    try:
        client = Client(
            client_id=f"cs-downlink-{dev_eui}",
            clean_session=True,
            callback_api_version=CallbackAPIVersion.VERSION2
        )

        if MQTT_USER:
            client.username_pw_set(MQTT_USER, MQTT_PASS)

        # Контейнер для отслеживания результата внутри callback
        state = {"done": False}

        def on_publish(client, userdata, mid, reason_code, properties):
            print(f"[{datetime.now()}] [OK] Сообщение доставлено на брокер (mid: {mid})")
            state["done"] = True

        def on_connect(client, userdata, flags, rc, properties=None):
            if rc == 0:
                topic = f"application/{app_id}/device/{dev_eui.lower()}/command/down"
                msg = {
                    "confirmed": False,
                    "fPort": F_PORT,
                    "data": DATA_B64
                }
                client.publish(topic, json.dumps(msg), qos=1)  # Используем QoS 1 для надежности
            else:
                print(f"[!] Ошибка подключения к MQTT: {rc}")
                state["done"] = True

        client.on_connect = on_connect
        client.on_publish = on_publish

        client.connect(MQTT_HOST, MQTT_PORT, 60)
        client.loop_start()

        # Ждем максимум 10 секунд на отправку, чтобы не зависать
        start_wait = time.time()
        while not state["done"] and (time.time() - start_wait) < 10:
            time.sleep(0.1)

        client.loop_stop()
        client.disconnect()

        if not state["done"]:
            print(f"[{datetime.now()}] [!] Тайм-аут отправки для {dev_eui}")

    except Exception as e:
        print(f"[{datetime.now()}] [!] Критическая ошибка MQTT: {e}")


def get_tokens():
    """Получение свежих токенов от обоих серверов"""
    print(f"[{datetime.now()}] Обновление токенов доступа...")

    # Токен Chirpstack
    c_resp = requests.post(f'http://{IP_CHIRPSTACK}/api/internal/login', json=CHIRPSTACK_AUTH)
    c_token = c_resp.json()['jwt']

    # Токен Saiman
    s_resp = requests.post(f'http://{IP_SAIMAN}/api/v1/account/login', json=SAIMAN_AUTH)
    s_token = s_resp.json()['accessToken']

    return c_token, s_token


# --- ОСНОВНОЙ ЦИКЛ ---
def main():
    try:
        # Получаем токены один раз перед входом в цикл
        c_token, s_token = get_tokens()
    except Exception as e:
        print(f"Ошибка при начальной авторизации: {e}")
        return

    while True:
        try:
            cheaders = {"Authorization": f'Bearer {c_token}'}
            sheaders = {"Authorization": f'Bearer {s_token}'}

            # Подготовка дат
            now = datetime.now()
            yesterday_iso = (now - timedelta(days=1)).strftime("%Y-%m-%dT19:00:00.000Z")

            print(f"[{now}] Проверка данных за {yesterday_iso}...")

            # Запрос списка приборов без данных
            url = f"http://{IP_SAIMAN}/api/v1/record/read/group"
            payload = {
                "from": yesterday_iso,
                "to": yesterday_iso,
                "type": "electric",
                "meterType": "",
                "timeType": "DAILY",
                "parameter": "",
                "recordId": "DAILY_DATA_ARCHIVE",
                "meters": ["43b449e0-8723-46b3-b23c-1065ebec0bd6"]  # Укажи нужные ID
            }

            response = requests.post(url, json=payload, headers=sheaders)

            # Если токен протух (401), обновляем его
            if response.status_code == 401:
                c_token, s_token = get_tokens()
                continue

            # Сохранение и анализ Excel
            file_name = "Saiman_Tat.xlsx"
            with open(file_name, "wb") as f:
                f.write(response.content)

            df = pd.read_excel(file_name, header=3)
            # Фильтр пустых показаний
            meters_missing = df[(df['Показания'].isna()) | (df['Показания'] == '')]['Номер прибора'].dropna().tolist()

            print(f"Найдено приборов без данных: {len(meters_missing)}")

            for i, meter_no in enumerate(meters_missing, 1):
                # Поиск устройства в Chirpstack
                search_url = f"http://{IP_CHIRPSTACK}/api/internal/search?search={meter_no}&limit=10"
                res_search = requests.get(search_url, headers=cheaders).json().get("result", [])

                if res_search:
                    dev = res_search[0]
                    app_id = dev["applicationID"]
                    dev_eui = dev["deviceDevEUI"]

                    print(f"[{i}] Переопрос: №{meter_no} (EUI: {dev_eui}, App: {app_id})")
                    send_downlink_command(app_id, dev_eui)

                    # Пауза между отправками, чтобы не забить очередь
                    time.sleep(5)
                else:
                    print(f"[{i}] Прибор №{meter_no} не найден в Chirpstack")

            print(f"[{datetime.now()}] Круг завершен. Спим 1 час...")
            time.sleep(3600)  # Проверка раз в час

        except Exception as e:
            print(f"[{datetime.now()}] Ошибка в цикле: {e}")
            time.sleep(300)  # Подождать 5 минут перед повтором при ошибке


if __name__ == "__main__":
    main()