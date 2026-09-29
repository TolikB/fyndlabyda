# Runbook: paper-торгівля на VM (симулятор v2)

Бот працює **лише в симуляції**: публічні ринкові дані Bybit, Gate, OKX, Binance,
Hyperliquid; жодних приватних ключів і жодних ордерів. Сервіс відмовляється
стартувати, якщо в середовищі є змінні, схожі на біржові ключі
(`*_API_KEY`, `*_API_SECRET`, `*_SECRET_KEY`, `*PASSPHRASE*`, `*PRIVATE_KEY*`, `*WALLET*`).

## 0. Що важливо знати

* **Баланс.** Кожна серія (`candidate`, `baseline`) має **1000 USDT загалом**, а не
  1000 USDT на кожній біржі. Попередня версія runbook помилково писала «$1,000 на
  кожну з 5 бірж».
* **Експозиція.** Розмір позиції — нотіонал однієї ноги хеджу. У `candidate`
  сукупно не більше **100 USDT** у funding-позиціях (2 × 50 USDT) і мінімальна ставка
  **0,02% за 8 год** на користь позиції. Застава: spot-нога повністю + perp-нога /
  плече (за замовчуванням плече 1, тобто позиція 50 USDT блокує ~100 USDT).
* **Серія = незмінні налаштування.** `label` серії разом із версією симулятора,
  комісіями, набором бірж і правилами fills утворює її «відбиток». Якщо змінити
  будь-що з цього, сервіс не продовжить стару серію й попросить новий `label` —
  так результати ніколи не змішуються.
* **Нова БД.** Compose використовує новий том `funding_arbitrage_paper_pgdata_v2`.
  Стара історія (старий том) не видаляється і не змішується з новою серією.
* **Порти** слухають лише `127.0.0.1`. Доступ до API/дашборда — через SSH-тунель:
  `ssh -L 8000:127.0.0.1:8000 <user>@<vm>` → http://127.0.0.1:8000/dashboard/

Вимоги до VM: Docker Engine + Compose v2, ≥2 CPU, ≥4 GB RAM, ≥20 GB вільного диска;
`systemctl is-enabled docker` = `enabled` (автостарт після перезавантаження).

## 1. Інвентаризація та збереження (нічого не змінює)

Працюємо тільки в `/opt/funding_arbitrage_paper`. Не виконуйте `docker system prune`,
`docker compose down -v`, `docker volume rm` і не чіпайте контейнери інших проєктів.

```bash
cd /opt/funding_arbitrage_paper
# 1.1 Знімок стану VM і контейнерів проєкту (без секретів)
ops/scripts/vm_inventory.sh            # якщо скриптів ще немає — див. крок 1.3

# 1.2 Зберегти локальні зміни коду на VM (у т.ч. зміну Gate) до оновлення
git status --short
git stash list
git diff > /root/vm-local-changes-$(date -u +%Y%m%dT%H%M%SZ).patch
git switch -c vm-snapshot-$(date -u +%Y%m%d) && git add -A && git commit -m "VM snapshot before v2"
git push -u origin HEAD                # далі зміну переглядаємо і переносимо в main окремо
```

1.3 Резервна копія старої БД і конфігурації (секрети лишаються на VM з правами 600):

```bash
docker ps --format '{{.Names}}\t{{.Status}}' | grep -i funding
ops/scripts/backup.sh --container <назва-старого-postgres-контейнера>
scp -r <user>@<vm>:/opt/funding_arbitrage_paper/backups/<папка> ./   # копія поза VM
```

1.4 Журнали проваленого Shadow-вікна і «нездорових» контейнерів — зберегти до зупинки:

```bash
ops/scripts/save_container_logs.sh <container-1> <container-2> <container-3>
```

1.5 Перед зупинкою переконатися, що контейнер не робить корисної роботи:

```bash
docker stats --no-stream <container>
docker logs --since 15m <container> | tail -n 50
docker inspect --format '{{.State.Health.Status}} {{.RestartCount}}' <container>
```

Лише після цього: `docker stop <container>` (не `rm`; видаляти після тижня й повторного
бекапу).

## 2. Розгортання релізу

```bash
cd /opt/funding_arbitrage_paper
git fetch origin && git checkout <release-branch-or-tag>
cp .env.paper-live-data.example .env        # якщо .env уже є — переносьте значення вручну
chmod 600 .env
sed -i "s/^POSTGRES_PASSWORD=.*/POSTGRES_PASSWORD=$(openssl rand -hex 24)/" .env
# Заповнити TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID, TELEGRAM_ENABLED=true.
# PAPER_AUTOTRADE=false на етапі спостереження.

docker compose build app
# Докази релізу: файли на VM == файли, що пройшли тести (ops/release-manifest.json)
docker run --rm -v "$PWD":/src -w /src --entrypoint funding-arbitrage \
  funding-arbitrage-paper:local release-manifest check
docker compose up -d
docker compose ps
curl -s 127.0.0.1:8000/health          # release_manifest_ok: true, execution_mode: paper
curl -s 127.0.0.1:8000/health/ready    # 200 після першого успішного циклу
```

Старий контейнер `redis` більше не потрібен; `docker compose up` попередить про
«orphan» — видаляйте його (`--remove-orphans`) лише після кроку 1.

## 3. Preflight усіх публічних потоків (без угод)

```bash
docker compose exec app funding-arbitrage preflight --json /tmp/preflight.json
```

Перевіряється кожен потік «біржа/ринок» (за замовчуванням 9: spot і perp на Bybit,
Gate, OKX, Binance + perp Hyperliquid; ТЗ згадує 8 — узгодьте, який потік не
потрібен, і вимкніть біржу через `ENABLED_VENUES` **до** старту серії):
інструменти, свіжість тикерів, funding з часом наступного розрахунку, свіжа
неперехрещена книга з правдоподібною глибиною в базових одиницях, історія funding.

* `FAIL` → потік не можна використовувати. Типово: геоблокування (HTTP 451/403 від
  Binance/Bybit для IP VM). Рішення: інший регіон VM або прибрати біржу з
  `ENABLED_VENUES` до старту серії.
* `WARN` → прочитати пояснення; допустимо, якщо зрозуміло чому.

## 4. Спостереження без угод (6–24 години)

`PAPER_AUTOTRADE=false`: runner збирає дані, сканує, тягне книги й історію funding,
пише записи циклів, але не відкриває позицій і не створює записів у ledger.

```bash
curl -s 127.0.0.1:8000/exchanges | python3 -m json.tool      # статус кожної біржі
curl -s "127.0.0.1:8000/analytics/readiness?hours=6" | python3 -m json.tool
docker compose logs --since 30m app | grep -E "venue_collection_failed|orderbook_fetch_failed|cycle_failed"
```

Очікування: `cycles.by_status` без `error`, `max_gap_seconds` < 300, доступність бірж
близька до 1.0. Єдина очікувана причина FAIL на цьому етапі — `no_daily_report_sent`.

## 5. Увімкнення paper-торгівлі

```bash
sed -i 's/^PAPER_AUTOTRADE=.*/PAPER_AUTOTRADE=true/' .env
docker compose up -d app
```

У Telegram приходить «▶️ Paper-бот запущено». Серії створюються з `label` із
`config/paper_series.yaml`. `PAPER_AUTOTRADE` не входить у відбиток серії.

## 6. Перевірка першого добового звіту

Наступного дня після 00:05 за Києвом приходить один звіт за попередню добу:
результат дня й за весь час, баланс, витрати (комісії, прослизання), funding,
угоди, відкриті позиції. Більше нічого в Telegram не надсилається (крім старту/зупинки).
Порівняння з baseline — в API: `GET /analytics/compare?a=candidate&b=baseline`.

## 7. Приймання: 72 години

```bash
docker compose exec app funding-arbitrage readiness --hours 72
docker compose exec app funding-arbitrage reconcile
```

`PASS` означає: цикли покривають усе вікно без прогалин > 5 хв, немає циклів зі
статусом `error`, інваріант `equity = cash + locked + PnL` тримається в межах $0,01 для
всіх серій, звірка ledger ↔ fills ↔ funding ↔ позиції збігається, надіслано щонайменше
один добовий звіт, у середовищі немає біржових ключів, реальних ордерів 0.
Формальні V1 Shadow/Paper-gates проходяться окремо й цим кроком не підміняються.

## 8. Збір статистики (≥ 30 днів)

* Не змінювати параметри серій, комісії, біржі. Будь-яка зміна → новий `label`
  (нова серія, стара лишається в БД для порівняння).
* Щоденно: `ops/scripts/backup.sh` (cron), перевірка `df -h` і `docker compose ps`.
* Щотижня: `readiness --hours 168`, `reconcile`, `/analytics/series/<id>/attribution`.

Приклад cron (root): `15 3 * * * cd /opt/funding_arbitrage_paper && ops/scripts/backup.sh >> backups/backup.log 2>&1`

## 9. Експлуатація

| Дія | Команда |
| --- | --- |
| Логи | `docker compose logs -f --since 10m app` |
| Зупинити / запустити без втрати даних | `docker compose stop` / `docker compose start` |
| Перезапуск після оновлення `.env` | `docker compose up -d app` |
| Моніторинг (опційно) | `docker compose --profile monitoring up -d` (задайте `GRAFANA_ADMIN_PASSWORD`) |
| Перевірка автостарту | `sudo reboot`, потім `docker compose ps` — обидва контейнери `healthy` |

Ніколи не виконуйте `docker compose down -v`: це видалить paper-історію.

## 10. Діагностика

| Симптом | Де дивитися | Що робити |
| --- | --- | --- |
| `unhealthy` | `curl 127.0.0.1:8000/health/ready` (деталі в `detail`) | `fatal_error` → логи старту; `persistence_ok=false` → БД/диск |
| `SeriesConfigMismatch` у логах | повідомлення містить змінені ключі | повернути налаштування або дати серії новий `label` |
| серія `halted` | `/health/ready` → `halted_series`, `reconcile` | зберегти логи, звірку; розбір до відновлення торгівлі |
| біржа OFFLINE | `/exchanges` (`last_error`, `retry_at`) | breaker сам повторює спробу з наростаючою паузою |
| `close_deferred` | логи `paper_close_deferred` | нема свіжої книги: закриття відкладається, fill не вигадується |
| `funding_settled_from_snapshot` | атрибуція `funding_sources` | історія funding запізнилася >15 хв; подія позначена як `snapshot` |
