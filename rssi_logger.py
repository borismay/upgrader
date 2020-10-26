from siklu_api import *

if __name__ == '__main__':

    unit = SikluUnit('31.168.34.109', 'admin', 'admin', debug=False)
    unit.connect()
    for i in range(100):
        reply = ShowRSSI(unit).parse()
        rssi = int(reply[0])
        # ts = time.strftime('%d-%m-%Y %H:%M:%S.%f', time.localtime())
        ts = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        print(f'{ts},{rssi}')
