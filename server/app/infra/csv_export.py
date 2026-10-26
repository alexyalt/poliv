"""Общая утилита CSV-выгрузки (Этап 8, вынесено из routes_stage6._csv_safe).

Назначение: единая точка экранирования значений при выгрузке CSV во всех
модулях (события Этапа 6, отчёты Этапа 8), чтобы защита от spreadsheet
formula injection не дублировалась в каждом маршруте.
"""
from __future__ import annotations

import csv
import io
from typing import Any, Iterable


def _csv_safe(value: Any) -> Any:
    """Защита от spreadsheet formula injection (аудит P2-12): строки,
    начинающиеся с = + - @ или табуляции, экранируются апострофом — Excel/
    Sheets не интерпретируют их как формулу."""
    if isinstance(value, str) and value[:1] in ("=", "+", "-", "@", "\t", "\r"):
        return "'" + value
    return value


def csv_response(rows: Iterable[Iterable[Any]], header: list[str],
                 filename: str) -> "StreamingResponse":
    """Собрать CSV (заголовок + строки, все значения через _csv_safe) и
    отдать как attachment. Используется /api/v1/reports/*.csv и events.csv."""
    from fastapi.responses import StreamingResponse

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(header)
    for r in rows:
        writer.writerow([_csv_safe(v) for v in r])
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"})
