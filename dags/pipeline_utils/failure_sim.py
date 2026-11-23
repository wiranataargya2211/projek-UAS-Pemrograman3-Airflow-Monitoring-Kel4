import random
import time


def maybe_fail(scenario, stage):
    '''Dipanggil di awal setiap tahap untuk mensimulasikan kegagalan.'''
    if scenario == 'api_error' and stage == 'extract':
        raise ConnectionError('Simulasi: API cuaca tidak dapat dihubungi')
    if scenario == 'slow_api' and stage == 'extract':
        time.sleep(60)  # lebih lama dari execution_timeout task extract (30 detik)
    if scenario == 'random_fail' and random.random() < 0.4:
        raise RuntimeError('Simulasi: error acak pada tahap ' + stage)


def corrupt_payload(payload):
    '''Menyisipkan data rusak untuk skenario bad_data.'''
    hourly = payload['hourly']
    hourly['temperature_2m'][3] = None   # nilai kosong
    hourly['temperature_2m'][5] = 99.0   # suhu tidak masuk akal
    return payload