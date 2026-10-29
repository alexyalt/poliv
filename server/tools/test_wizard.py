"""Interactive local acceptance; durable answers, scoped processes, no implicit watering."""
import argparse
from datetime import datetime, timezone
import getpass
import json
import os
from pathlib import Path
import platform
import socket
import subprocess
import sys
import time
import uuid
import webbrowser
import httpx

ROOT = Path(__file__).resolve().parents[2]


def stamp():
    return datetime.now(timezone.utc).isoformat()


class Report:
    def __init__(self, folder, stage, resume=False):
        self.folder = Path(folder)
        self.path = self.folder / 'report.json'
        if resume:
            self.data = json.loads(self.path.read_text(encoding='utf-8'))
            if self.data['stage'] != stage:
                raise ValueError('Resume stage differs from saved report')
            self.data['status'] = 'in_progress'
        else:
            self.folder.mkdir(parents=True, exist_ok=False)
            self.data = {'id': uuid.uuid4().hex, 'stage': stage, 'started': stamp(),
                         'status': 'in_progress', 'os': platform.platform(),
                         'python': platform.python_version(), 'answers': [], 'checks': []}
            try:
                self.data['commit'] = subprocess.check_output(
                    ['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True, stderr=subprocess.DEVNULL).strip()
            except (OSError, subprocess.CalledProcessError):
                self.data['commit'] = 'ZIP checkout: unknown'
        self.save()

    def save(self):
        temporary = self.folder / 'report.json.tmp'
        temporary.write_text(json.dumps(self.data, ensure_ascii=False, indent=2), encoding='utf-8')
        os.replace(temporary, self.path)
        text = ['# Отчёт интерактивной проверки', '',
                f"Этап: {self.data['stage']} · Статус: {self.data['status']}",
                f"Начало: {self.data['started']} · Commit: {self.data.get('commit')}",
                f"ОС: {self.data['os']}", '', '## Автоматические проверки', '']
        for row in self.data['checks']:
            text += [f"- {row['status']} · {row['name']} · {row['detail']}"]
        text += ['', 'FAIL: '+str(sum(c['status']=='FAIL' for c in self.data['checks'])), '', '## Ответы оператора', '']
        for row in self.data['answers']:
            text += [f"### {row['id']}: {row['question']}", '', row['answer'], '', f"Время: {row['at']}", '']
        text += ['## Ограничения', '', 'Ответы оператора не заменяют измерения. Completed означает завершение опроса, а не допуск к эксплуатации. Не проверенные сценарии не считаются успешными. OTA не проверяется; обновление по USB.']
        (self.folder / 'report.md').write_text('\n'.join(text), encoding='utf-8')

    def check(self, name, ok, detail=''):
        self.data['checks'].append({'at': stamp(), 'name': name,
                                    'status': 'PASS' if ok else 'FAIL', 'detail': detail})
        self.save()
        print(('PASS ' if ok else 'FAIL ') + name + ': ' + detail)

    def ask(self, key, question, kind='yesno', reuse=True):
        old = next((a for a in self.data['answers'] if a['id'] == key), None)
        if old and reuse:
            print('Сохранённый ответ:', key, old['answer'])
            return old['answer']
        print('\n' + question)
        if kind == 'text':
            print('Развёрнутый ответ: несколько строк; отдельная точка завершает ввод.')
            lines = []
            while True:
                line = input('> ')
                if line == '.':
                    if lines:
                        answer = '\n'.join(lines)
                        break
                    print('Нужен хотя бы один ответ, либо напишите «не проверено».')
                else:
                    lines.append(line)
        else:
            while True:
                answer = input('[да/нет/пропустить] ' if kind == 'yesno' else '[число >= 0] ').strip()
                if kind == 'yesno' and answer.lower() in ('да', 'нет', 'пропустить'):
                    answer = answer.lower()
                    break
                if kind == 'number':
                    try:
                        value = float(answer.replace(',', '.'))
                        if 0 <= value < 1e8:
                            break
                    except ValueError:
                        pass
                print('Ответ не распознан. Повторите ввод.')
        self.data['answers'].append({'id': key, 'question': question, 'answer': answer, 'at': stamp()})
        self.save()
        return answer


class Processes:
    def __init__(self, report):
        self.report, self.owned = report, []

    def start(self, name, args, env=None):
        log = (self.report.folder / (name + '.log')).open('ab')
        try:
            p = subprocess.Popen(args, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
        finally:
            log.close()
        self.owned.append((name, p))
        return p

    def close(self):
        for name, p in reversed(self.owned):
            if p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    p.kill()
                    p.wait(timeout=5)
            self.report.check('Остановка собственного процесса ' + name, True, 'PID ' + str(p.pid))


def free_port(port):
    with socket.socket() as s:
        try:
            s.bind(('127.0.0.1', port))
            return True
        except OSError:
            return False


def wait_http(client, process, seconds=40):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if process.poll() is not None:
            return False
        try:
            if client.get('/login').status_code == 200:
                return True
        except httpx.HTTPError:
            pass
        time.sleep(.4)
    return False


def questionnaire(report):
    for key, question in [
        ('ui', 'Откройте монитор. Читаемы ли статусы, выделение аварий и offline с рабочего расстояния?'),
        ('weather', 'На /weather работают 24 часа / 7 дней, ветер, осадки, ET0 и смена вкладок?'),
        ('settings', 'Координаты 44.423062, 34.06452 и настройки монитора доступны и сохраняются?'),
        ('roles', 'Проверили отдельного viewer: управление отсутствует, настройки не сохраняются?')]:
        report.ask(key, question)
    report.ask('observations', 'Опишите неудобства, ошибки и замечания к интерфейсу (без паролей).', 'text')


def controller_tests(report, client):
    response = client.get('/api/controllers/live')
    response.raise_for_status()
    controllers = response.json()
    print('Контроллеры:', [(c['id'], c['name'], c['connection_status']) for c in controllers])
    selected = input('Введите ID подключённого контроллера: ').strip()
    device = next((c for c in controllers if str(c['id']) == selected), None)
    if device and (device.get('connection_status') != 'online' or not device.get('time_valid')):
        raise ValueError('Controller must be online with valid time before physical scenarios')
    if not device:
        raise ValueError('Controller not found; create it in UI and rerun')
    report.data['controller'] = {k: device.get(k) for k in ('id', 'name', 'box_id', 'firmware_version')}
    report.save()
    report.ask('hardware', 'Запишите COM, модель платы, полярность реле, датчик, нагрузку и версии. Не пишите пароли.', 'text')
    if report.ask('safe-now', 'Именно сейчас насос отключён, нет давления, полярность проверена и вы готовы к короткому открытию?', reuse=False) != 'да':
        report.check('Физические команды', False, 'Оператор не разрешил; сценарии пропущены')
        return
    zone = int(input('Номер проверяемой зоны 1..16: '))
    if not 1 <= zone <= 16:
        raise ValueError('Zone must be 1..16')
    endpoint = f"/api/controllers/{device['id']}/actions/"
    # Every replay requires fresh consent. Stop is attempted even on Ctrl+C during a valve scenario.
    try:
        for action, params, prompt in [
            ('zone_open', {'zone': zone, 'duration_sec': 10}, 'Запустить зону на 10 секунд?'),
            ('pause_controller', {'duration_sec': 60}, 'Отправить паузу на 60 секунд?'),
            ('resume_controller', {}, 'Отправить возобновление?')]:
            if report.ask('permit-' + action, prompt, reuse=False) != 'да':
                continue
            result = client.post(endpoint + action, json={'params': params})
            report.check('API ' + action, result.status_code == 202,
                         'HTTP ' + str(result.status_code) + '; ' + result.text[:500])
            report.ask('physical-' + action, 'Физическое состояние клапана соответствует команде?', reuse=False)
    finally:
        try:
            result = client.post(endpoint + 'stop_all', json={'params': {'reason': 'test wizard cleanup'}})
            report.check('STOP cleanup', result.status_code == 202, 'HTTP ' + str(result.status_code))
        except httpx.HTTPError:
            report.check('STOP cleanup', False, 'Нет связи! Закройте клапан/питание вручную.')
    report.ask('stop-latency', 'Измеренная задержка физического закрытия STOP в секундах:', 'number')
    for key, prompt in [
        ('schedule', 'В UI создайте короткий water/soak/water, отправьте и дождитесь applied. Факт соответствует плану?'),
        ('power', 'Под наблюдением проверьте reset/потерю питания: клапаны закрыты, самопроизвольного старта нет?'),
        ('network', 'Отключите и верните Wi-Fi платы: offline/reconnect правильно видны?'),
        ('flow', 'Если есть датчик: no_flow/unexpected_flow закрывают выходы и показывают причину? Без датчика — пропустить.'),
        ('reset', 'После устранения причины нажмите Сброс аварии в деталях: только ACK снимает блокировку?')]:
        report.ask(key, prompt)
    report.ask('hardware-observations', 'Опишите физические результаты, объём/время, расхождения истории и неисправности.', 'text')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime', required=True, type=Path)
    parser.add_argument('--stage', choices=['software', 'controller'], default='software')
    parser.add_argument('--resume', type=Path)
    args = parser.parse_args()
    runtime = args.runtime.resolve()
    if not runtime.is_dir():
        raise SystemExit('Сначала создайте runtime по docs/START_HERE_WINDOWS.md')
    if args.resume and not args.resume.resolve().is_relative_to(runtime / 'test-reports'):
        raise SystemExit('Resume must point inside this runtime/test-reports')
    folder = args.resume or runtime / 'test-reports' / (datetime.now().strftime('%Y%m%d-%H%M%S') + '-' + uuid.uuid4().hex[:6])
    report = Report(folder, args.stage, bool(args.resume))
    processes = Processes(report)
    print('Отчёт после каждого ответа:', report.folder)
    try:
        setup = json.loads((runtime / 'setup.json').read_text(encoding='utf-8'))
        import tomllib
        cfg = tomllib.loads((runtime / 'config.local.toml').read_text(encoding='utf-8-sig'))
        if report.ask('equipment-ready', 'Не вводите пароли в ответы. Прочитали ограничения и готовы начать?', reuse=False) != 'да':
            raise KeyboardInterrupt
        # Refuse adoption of unknown services: no stopping another application's broker/server.
        if not free_port(8080) or not free_port(1883):
            raise RuntimeError('Порт 8080/1883 занят. Закройте ранее запущенный Poliv/Mosquitto или остановите его службу и повторите. Чужой процесс не останавливаем.')
        broker = Path(setup['mosquitto_dir']) / ('mosquitto.exe' if os.name == 'nt' else 'mosquitto')
        processes.start('broker', [str(broker), '-c', str(runtime / 'broker/mosquitto.conf'), '-v'])
        from server.tools.preflight import mqtt_check
        ok = False
        for _ in range(10):
            if mqtt_check(cfg, runtime):
                ok = True
                break
            time.sleep(.5)
        report.check('Брокер и аутентификация', ok)
        if not ok:
            raise RuntimeError('MQTT не подключён; проверьте broker.log, пароль и IP')
        env = dict(os.environ, POLIV_DATA_ROOT=str(runtime),
                   POLIV_CONFIG_LOCAL=str(runtime / 'config.local.toml'), PYTHONUTF8='1')
        server = processes.start('server', [sys.executable, '-m', 'uvicorn', 'server.main:app',
                                            '--host', '127.0.0.1', '--port', '8080', '--workers', '1'], env)
        with httpx.Client(base_url='http://127.0.0.1:8080', timeout=45, follow_redirects=False,
                          headers={'Origin': 'http://127.0.0.1:8080'}) as client:
            ok = wait_http(client, server)
            report.check('HTTP сервер', ok)
            if not ok:
                raise RuntimeError('Сервер не запущен; проверьте server.log')
            webbrowser.open('http://127.0.0.1:8080')
            print('В браузере выполните первоначальную смену пароля. Затем войдите здесь текущим operator/admin.')
            username = input('Логин: ').strip()
            password = getpass.getpass('Текущий пароль (не записывается в отчёт): ')
            login = client.post('/login', data={'username': username, 'password': password})
            password = None
            ok = login.status_code == 302 and bool(client.cookies.get('poliv_session'))
            report.check('Вход', ok)
            if not ok:
                raise RuntimeError('Вход не выполнен')
            for url in ('/', '/active-zones', '/weather', '/settings', '/api/controllers/live', '/api/v1/weather/outlook'):
                response = client.get(url)
                report.check('GET ' + url, response.status_code == 200, 'HTTP ' + str(response.status_code))
                if response.status_code != 200:
                    raise RuntimeError('Нет доступа к странице: проверьте обязательную смену пароля и роль')
            if args.stage == 'software':
                print('Автосценарии Chromium запускаются на отдельной временной БД, без физического контроллера.')
                if report.ask('install-browser', 'Установить/проверить Chromium и запустить автоматические UI-сценарии?') == 'да':
                    for command in ([sys.executable, '-m', 'pip', 'install', '-q', 'playwright==1.64.1'],
                                    [sys.executable, '-m', 'playwright', 'install', 'chromium'],
                                    [sys.executable, '-m', 'server.tools.weather_monitor_smoke'],
                                    [sys.executable, '-m', 'server.tools.dashboard_smoke']):
                        print('Выполняется:', ' '.join(command[2:]))
                        with (report.folder / 'automation.log').open('ab') as log:
                            result = subprocess.run(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=600)
                        report.check(' '.join(command[2:]), result.returncode == 0, 'См. automation.log')
                        if result.returncode:
                            break
            else:
                controller_tests(report, client)
            questionnaire(report)
            report.ask('final', 'Итог: что не проверено, блокирующие ошибки, предложения. Приведите шаги воспроизведения.', 'text')
            report.data['status'] = 'completed'
    except (KeyboardInterrupt, EOFError):
        report.data['status'] = 'interrupted'
        print('\nПрервано; ответы сохранены. Проверьте физическое закрытие клапанов.')
    except Exception as exc:
        report.data['status'] = 'failed'
        # Do not serialize exception strings that may contain credentials or HTTP URLs.
        report.check('Выполнение мастера', False, type(exc).__name__ + '; проверьте локальные журналы и инструкцию')
        print('Не удалось продолжить:', type(exc).__name__, '— проверьте порты, конфиг, логи и инструкцию.')
    finally:
        processes.close()
        report.data['finished'] = stamp()
        report.save()
        print('Сохранены report.md и report.json в', report.folder)
        print('Журналы локальные: перед передачей проверьте отсутствие секретов.')
    return 0 if report.data['status'] == 'completed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
