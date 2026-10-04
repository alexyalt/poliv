"""Запуск N эмуляторов контроллеров одновременно (Этап 3, Блок 7.4).

Имена: BOX-EMUL-01 .. BOX-EMUL-{N} (префикс по заданию). Каждый эмулятор —
отдельный процесс controller_sim.py со своими параметрами; управление общим
пулом, завершение по Ctrl+C.

Примеры:
  python emulator/fleet_sim.py --count 3 --status-interval 5
  python emulator/fleet_sim.py --count 2 --broker 192.168.1.10 --port 1883 \
      --user-prefix poliv_box_ --password test123
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
SIM = HERE / "controller_sim.py"


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Параллельный запуск N эмуляторов контроллеров (Этап 3)")
    ap.add_argument("--count", type=int, default=3,
                    help="количество эмуляторов (по умолчанию 3)")
    ap.add_argument("--prefix", default="BOX-EMUL-",
                    help="префикс имён box_id (итог: BOX-EMUL-01..N)")
    ap.add_argument("--broker", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=1883)
    ap.add_argument("--user-prefix", default=None,
                    help="префикс MQTT-логина: {prefix}{NN} (без логина — анонимно)")
    ap.add_argument("--password", default=None)
    ap.add_argument("--status-interval", type=float, default=30.0)
    ap.add_argument("--zones", type=int, default=16)
    return ap


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    procs: list[tuple[str, subprocess.Popen]] = []
    for i in range(1, max(1, args.count) + 1):
        box_id = f"{args.prefix}{i:02d}"
        cmd = [sys.executable, str(SIM),
               "--box-id", box_id,
               "--broker", args.broker,
               "--port", str(args.port),
               "--status-interval", str(args.status_interval),
               "--zones", str(args.zones)]
        if args.user_prefix:
            cmd += ["--user", f"{args.user_prefix}{i:02d}"]
        if args.password:
            cmd += ["--password", args.password]
        procs.append((box_id, subprocess.Popen(cmd)))
        print(f"[fleet] запущен {box_id} (pid={procs[-1][1].pid})")
    print(f"[fleet] всего {len(procs)} эмуляторов. Ctrl+C — остановить все.")
    try:
        while True:
            alive = [(b, p) for b, p in procs if p.poll() is None]
            if len(alive) != len(procs):
                for b, p in procs:
                    if p.poll() is not None:
                        print(f"[fleet] {b} завершился (код {p.returncode})")
                procs = alive
                if not procs:
                    break
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\n[fleet] остановка всех эмуляторов...")
    finally:
        for _box, p in procs:
            p.terminate()
        deadline = time.time() + 5
        for _box, p in procs:
            try:
                p.wait(timeout=max(0.1, deadline - time.time()))
            except subprocess.TimeoutExpired:
                p.kill()
        print("[fleet] все процессы остановлены")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
