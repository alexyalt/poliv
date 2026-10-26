# Миграция 0011_archive_integrity

Добавляет к archive_exports:

- format_version INTEGER NOT NULL DEFAULT 1 — старые строки автоматически legacy;
- manifest_hash TEXT — SHA-256 companion manifest новых партий v2.

Данные, старые пути и файлы не меняются. Новый writer устанавливает version=2,
хеш полного неизменяемого файла и manifest. Старые миграции не редактируются.
Применение идемпотентно; upgrade проверен на минимальной старой таблице,
legacy-reader — на старой записи журнала, fresh install — полным набором тестов.

Downgrade намеренно не удаляет дополнительные столбцы: старый код их игнорирует.
При откате приложения отключить старый архивный daemon и не выполнять restore
v2 старым reader: он не знает manifest/отношения/типы. Сохранить БД и оба файла
каждой партии. Полная процедура и crash recovery — docs/adr/ADR-18-stage8-stabilization.md.
