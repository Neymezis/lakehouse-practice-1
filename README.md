# Практическая №1: lakehouse в Docker (вариант Q18, TPC-H)

Автор: Носов Евгений Владиславович

## 1. Вариант и масштаб

| Поле | Значение |
|---|---|
| ФИО (как в VK Education) | Носов Евгений Владиславович |
| Вариант (запрос) | Q18 «Large Volume Customer» (вариант 3) |
| Масштаб `sfN` | sf1 |
| Почему выбрал такой масштаб | Q18 ищет заказы, где суммарное количество товара больше 300, и таких заказов мало: на sf1 их ровно 57. На меньших масштабах результат был бы совсем крошечным и бессмысленным для проверки Gold. При этом sf1 небольшой: 6 001 215 строк в `lineitem`, 1,5 млн в `orders`, 150 тыс. в `customer`. Он спокойно помещается в ограниченную память Trino (куча 1,8 ГБ) на ноутбуке с 15 ГБ RAM, и каждый запрос выполняется за секунды, поэтому можно прогнать сравнение сжатия несколько раз. Масштаб sf10 дал бы больше заказов, но сильно увеличил бы время и память без пользы для задачи. Один и тот же sf1 использован на всех слоях. |

## 2. Поднятые сервисы — и почему именно они

Состояние на момент сдачи (`docker ps`):

```
CONTAINER ID   IMAGE                                    STATUS                   PORTS                      NAMES
353edff41ab4   trinodb/trino:470                        Up 8 minutes (healthy)   0.0.0.0:8080->8080/tcp     trino
18dae3865948   quay.io/lakekeeper/catalog:latest-main   Up 8 minutes             0.0.0.0:8181->8181/tcp     lakekeeper
267fd1b7bdf5   postgres:16-alpine                       Up 8 minutes (healthy)   5432/tcp                   lakehouse-pg
b25bfe9b405b   bitnamilegacy/minio:latest               Up 8 minutes             0.0.0.0:9000-9001->9000-9001/tcp   lakehouse-minio
cc0442bf8064   metabase/metabase:latest                 Up                       0.0.0.0:3000->3000/tcp     metabase
```

| Сервис | Роль в стеке | Почему именно он (а не альтернатива) |
|---|---|---|
| MinIO | S3-совместимое объектное хранилище (сюда физически падают parquet-файлы) | Открытый исходный код и S3 API, как у настоящего облачного хранилища, поэтому любой движок (Trino, Spark, DuckDB) подключается так же, как к AWS S3, но всё работает локально и бесплатно. Лёгкий: одному контейнеру хватает около 400 МБ. Альтернативы: HDFS тяжелее в развёртывании и привязан к Hadoop; настоящий AWS S3 платный и требует аккаунта; локальная файловая система не даёт S3-интерфейса, на который рассчитаны Iceberg и Lakekeeper. Примечание: официальные образы MinIO в публичных реестрах оказались недоступны, поэтому взят образ `bitnamilegacy/minio`, внутри тот же MinIO. |
| PostgreSQL | БД, где Lakekeeper хранит метаданные каталога | Lakekeeper хранит свой каталог (какие таблицы и схемы существуют, где лежат их метаданные) в Postgres. Он надёжный, транзакционный, открытый и лёгкий (вариант `alpine` занимает ~256 МБ). Каталог требует именно транзакций, чтобы одновременные изменения таблиц не ломали друг друга, поэтому файлов или key-value хранилища недостаточно. |
| Lakekeeper | Iceberg REST Catalog — каталог метаданных таблиц | Реализует открытый REST-протокол каталога Iceberg, поэтому Trino, Spark и другие движки подключаются к нему одинаково, без привязки к производителю. Написан на Rust, быстро стартует и потребляет мало памяти (256 МБ). Альтернативы: Hive Metastore тяжёлый (Java, Hadoop-зависимости) и устаревает; Nessie и Polaris тоже подходят, но Lakekeeper проще всего поднять в одном контейнере с одним Postgres. |
| Trino | SQL-движок: ходит в Lakekeeper за метаданными и читает/пишет данные в MinIO | Один SQL-движок для всех шагов работы: коннектор `tpch` генерирует исходные данные, коннектор Iceberg пишет таблицы, `EXCEPT` проверяет корректность. Хранилища у него нет, поэтому он естественно ложится в схему «вычисления отдельно, данные отдельно». Поддерживает нужные возможности Iceberg: time travel, `ALTER TABLE`, выбор кодека сжатия. Альтернатива Spark потребляет больше памяти и требует больше настройки для той же задачи; обычная БД не умеет работать с таблицами Iceberg в объектном хранилище. |
| Metabase | BI-инструмент поверх Bronze (п. 6.2) | Открытый исходный код, веб-интерфейс, не нужно писать код для просмотра данных, а встроенный драйвер Starburst/Trino подключается к уже имеющемуся Trino без установки плагинов. Подробное сравнение с альтернативами в п. 6.2. |

### Скрипты, которыми поднимал стек

Стек описан в `docker-compose.yml`, запускается командами:

```bash
docker compose up -d      # поднимает все контейнеры
./init.sh                 # bootstrap Lakekeeper и создание warehouse "lakehouse" (бакет lakehouse в MinIO)
./run_all.sh sf1          # создаёт Bronze/Silver/Gold, сравнивает сжатие, проверяет Gold, time travel, schema evolution
```

Структура репозитория:

- `README.md`: этот отчёт
- `docker-compose.yml`: все сервисы
- `trino/`: конфигурация Trino (каталоги `lakehouse` и `tpch`, ограничения памяти)
- `init.sh`: инициализация каталога Lakekeeper
- `run_all.sh`: все SQL-запросы и замеры
- `results/`: результаты запуска (сжатие, размеры слоёв, проверка Gold, time travel, schema evolution)
- `screenshots/`: скриншоты Metabase для п. 6.2

`docker-compose.yml`:

```yaml
services:
  postgres:
    image: postgres:16-alpine
    container_name: lakehouse-pg
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: lakekeeper
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres -d lakekeeper"]
      interval: 3s
      timeout: 3s
      retries: 20
    mem_limit: 256m

  minio:
    image: bitnamilegacy/minio:latest
    container_name: lakehouse-minio
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
      MINIO_DEFAULT_BUCKETS: lakehouse
    ports:
      - "9000:9000"
      - "9001:9001"
    mem_limit: 384m

  lakekeeper-migrate:
    image: quay.io/lakekeeper/catalog:latest-main
    container_name: lakekeeper-migrate
    command: ["migrate"]
    environment: &lk_env
      LAKEKEEPER__PG_ENCRYPTION_KEY: "local-dev-encryption-key"
      LAKEKEEPER__PG_DATABASE_URL_READ: postgresql://postgres:postgres@postgres:5432/lakekeeper
      LAKEKEEPER__PG_DATABASE_URL_WRITE: postgresql://postgres:postgres@postgres:5432/lakekeeper
      RUST_LOG: info
    depends_on:
      postgres:
        condition: service_healthy
    restart: "no"

  lakekeeper:
    image: quay.io/lakekeeper/catalog:latest-main
    container_name: lakekeeper
    command: ["serve"]
    environment: *lk_env
    depends_on:
      lakekeeper-migrate:
        condition: service_completed_successfully
    ports:
      - "8181:8181"
    mem_limit: 256m

  trino:
    image: trinodb/trino:470
    container_name: trino
    depends_on:
      - lakekeeper
      - minio
    ports:
      - "8080:8080"
    volumes:
      - ./trino/jvm.config:/etc/trino/jvm.config:ro
      - ./trino/config.properties:/etc/trino/config.properties:ro
      - ./trino/catalog:/etc/trino/catalog:ro
    mem_limit: 2500m

  metabase:
    image: metabase/metabase:latest
    container_name: metabase
    depends_on:
      - trino
    environment:
      JAVA_OPTS: "-Xmx900m"
    ports:
      - "3000:3000"
    mem_limit: 1300m
```

Конфигурация Trino лежит в `trino/`: `catalog/lakehouse.properties` (Iceberg REST-каталог → Lakekeeper, S3 → MinIO), `catalog/tpch.properties` (генератор данных), `jvm.config` и `config.properties` (ограничение памяти: куча 1,8 ГБ). Скрипт `init.sh` обращается к API Lakekeeper (`/management/v1/bootstrap` и `/management/v1/warehouse`), а `run_all.sh` выполняет все SQL через `docker exec trino trino`; оба файла лежат в репозитории рядом с этим отчётом.

## 3. Разбор запроса — какие данные нужны

Q18 «Large Volume Customer»: клиенты по заказам, где суммарное количество товара больше 300; топ-100 по сумме заказа.

| Таблица TPC-H | Нужные колонки | Что запрос с ними делает (фильтр / джойн / агрегат / вывод) |
|---|---|---|
| `customer` | `custkey`, `name` | джойн с `orders` по `custkey`; `name` и `custkey` выводятся и входят в `GROUP BY` |
| `orders` | `orderkey`, `custkey`, `orderdate`, `totalprice` | джойн с `customer` (по `custkey`) и с `lineitem` (по `orderkey`); фильтр `orderkey IN (подзапрос)`; `orderdate` и `totalprice` выводятся, по ним сортировка `ORDER BY totalprice DESC, orderdate` |
| `lineitem` | `orderkey`, `quantity` | в подзапросе: `GROUP BY orderkey HAVING sum(quantity) > 300` (отбирает «крупные» заказы); в основном запросе: джойн по `orderkey` и агрегат `sum(quantity)` как `total_quantity` |

Остальные колонки (адрес, телефон, цены и скидки позиций, даты отгрузки и т.д.) запросу не нужны, поэтому в Bronze не попали.

## 4. Медальон — решения по каждому слою

Значения «Строк» и «Вес, МБ» получены из `SELECT count(*)` и из метатаблицы Iceberg `"таблица$files"` (сумма `file_size_in_bytes`) на масштабе sf1. Вес указан для итогового состояния слоёв (после сравнения GZIP/ZSTD слой Bronze пересоздан и остался в ZSTD).

### Bronze (сырой)

| Таблица | Колонки (что оставил) | Строк | Вес, МБ | Формат | Сжатие | Партиционирование | Обоснование |
|---|---|---|---|---|---|---|---|
| bronze.customer | custkey, name | 150 000 | 0,215 | PARQUET | ZSTD | нет | Данные взяты как есть, без фильтров и преобразований, но только колонки, нужные Q18: остальные только занимали бы место и память. Parquet колоночный: запрос читает только нужные столбцы, а повторяющиеся значения хорошо сжимаются. ZSTD выбран по итогам сравнения в п. 5 (меньше и быстрее GZIP). Партиционирование не нужно: таблица занимает 0,2 МБ, и разбиение на части дало бы только множество мелких файлов. |
| bronze.orders | orderkey, custkey, orderdate, totalprice | 1 500 000 | 12,887 | PARQUET | ZSTD | нет | Самая «тяжёлая» таблица Bronze из-за `totalprice` и `orderdate`. Запрос не фильтрует заказы по дате, а читает все, поэтому партиционирование по `orderdate` ничего бы не отсекало и только создало бы много мелких файлов (приём «partition pruning» здесь бесполезен). Выбор формата и сжатия как у остальных таблиц слоя. |
| bronze.lineitem | orderkey, quantity | 6 001 215 | 6,721 | PARQUET | ZSTD | нет | Самая большая таблица по числу строк, но всего две колонки, поэтому она занимает лишь 6,7 МБ: `orderkey` идёт упорядоченными повторяющимися значениями, а `quantity` принимает небольшой набор чисел, такие столбцы Parquet сжимает очень сильно. Запрос агрегирует эту таблицу целиком (`GROUP BY orderkey`), поэтому партиционирование снова ничего не отсекло бы. |

### Silver (очищенный)

| Таблица | Колонки (что оставил) | Строк | Вес, МБ | Формат | Сжатие | Партиционирование | Обоснование |
|---|---|---|---|---|---|---|---|
| silver.big_orders | orderkey, total_quantity | 57 | 0,001 | PARQUET | ZSTD (по умолчанию Trino) | нет | Первый шаг подготовки: из `lineitem` оставлены только «крупные» заказы (`GROUP BY orderkey HAVING sum(quantity) > 300`). Это самая дорогая часть запроса (агрегация 6 млн строк), и её выполнение один раз и сохранение результата делает следующие шаги почти мгновенными. Партиционировать 57 строк бессмысленно. |
| silver.order_facts | name, custkey, orderkey, orderdate, totalprice, total_quantity | 57 | 0,002 | PARQUET | ZSTD (по умолчанию Trino) | нет | Факт «крупных заказов»: заказы соединены с клиентами и с таблицей `big_orders`. В таком виде таблица уже содержит всё, что нужно для ответа на Q18, остаётся только отсортировать и взять топ. Формат тот же, чтобы слои были согласованы; партиционирования нет, потому что данных 57 строк. |

### Gold (витрина)

| Таблица | Колонки | Строк | Вес, МБ | Формат | Сжатие | Партиционирование | Обоснование |
|---|---|---|---|---|---|---|---|
| gold.top100_customers | name, custkey, orderkey, orderdate, totalprice, total_quantity | 57 | 0,002 | PARQUET | ZSTD (по умолчанию Trino) | нет | Готовый ответ на Q18: `ORDER BY totalprice DESC, orderdate LIMIT 100` над Silver. Строк получилось 57, а не 100, потому что на sf1 существует только 57 заказов с суммарным количеством больше 300, и `LIMIT 100` их не обрезает. Корректность проверена прямым сравнением с оригинальным запросом на `tpch.sf1`: `orig EXCEPT gold` = 0 строк и `gold EXCEPT orig` = 0 строк, то есть результаты совпадают. Витрина крошечная, поэтому формат и сжатие здесь не влияют на производительность, а партиционирование не имеет смысла. |

## 5. Тесты производительности

Запрос Q18, выполненный поверх Bronze, прогнан три раза подряд для каждого кодека. Время взято из `system.runtime.queries` (разница `end` и `started`, то есть время выполнения на стороне Trino без запуска клиента). Перед каждым замером Bronze пересоздан с нужным кодеком.

| Сжатие на Bronze | Время запроса | Размер Bronze, МБ | Твой вывод (что лучше и почему) |
|---|---|---|---|
| GZIP | 1262 / 1134 / 1109 мс (в среднем ≈ 1168 мс) | 21,61 | Больше по размеру и медленнее. GZIP сильно сжимает, но распаковка у него дороже по процессору, и на небольших данных, которые целиком помещаются в память, это заметно. |
| ZSTD | 928 / 871 / 958 мс (в среднем ≈ 919 мс) | 19,82 | Лучше по обоим показателям: примерно на 21% быстрее и на 8% меньше. ZSTD распаковывается быстрее GZIP при сопоставимой или лучшей степени сжатия, и на числовых колонках Parquet это даёт выигрыш сразу и по месту, и по времени. |

Вывод: ZSTD лучше для моих данных, поэтому слой Bronze оставлен в ZSTD. Замечание о точности: данных мало (около 20 МБ), абсолютная разница измеряется долями секунды, а первый прогон каждого кодека чуть медленнее остальных (кэш), поэтому важна не конкретная цифра, а то, что ZSTD выигрывает по всем трём прогонам. На больших объёмах разница в размере и времени вырастет.

## 6. Часть 2 — осмысление и работа с инструментами

### 6.1. Понять и объяснить

| Вопрос | Твой ответ |
|---|---|
| Зачем нужны большие данные и lakehouse (а не обычная БД/хранилище)? | Обычная БД хранит данные и считает запросы на одном сервере: когда данных становится слишком много, приходится покупать более мощный сервер, а хранение и вычисления растут вместе, хотя нужны они по отдельности. В lakehouse данные лежат в дешёвом объектном хранилище в открытом формате (Parquet), а считать их может любой движок: Trino для SQL, Spark для обработки и так далее, и вычисления можно масштабировать независимо от хранения. В отличие от простого «озера файлов» (data lake), формат таблиц Iceberg добавляет то, что есть у баз данных: транзакции, схему таблицы, версии (time travel), изменение структуры без переписывания данных. В отличие от классического хранилища (DWH), одни и те же файлы могут использоваться и для отчётов, и для машинного обучения, а данные не заперты в формате одного производителя. |
| Пример schema evolution на Silver: добавил/переименовал колонку — что произошло? | В таблице `silver.order_facts` выполнены `ALTER TABLE silver.order_facts ADD COLUMN note varchar` и `ALTER TABLE silver.order_facts RENAME COLUMN total_quantity TO qty_total`. Обе операции прошли мгновенно: файлы с данными не переписывались, изменились только метаданные таблицы. После `DESCRIBE` в таблице колонки `name, custkey, orderkey, orderdate, totalprice, qty_total, note`; у уже существующих строк в новой колонке `note` пусто (NULL), а старые данные под переименованной колонкой `qty_total` читаются без потерь, например `Customer#000128120` имеет `qty_total` = 323. Так получается потому, что Iceberg связывает колонки с файлами по внутреннему идентификатору колонки, а не по её имени, поэтому переименование ничего не ломает. |
| Пример скрипта восстановления данных из снапшота (time travel) | См. ниже. Результат моего запуска: до удаления 57 строк, после `DELETE` 29, чтение старой версии через `FOR VERSION AS OF` даёт 57, после `rollback_to_snapshot` снова 57. |

```sql
-- 1. запомнить снапшот до порчи данных
SELECT snapshot_id FROM silver."order_facts$snapshots" ORDER BY committed_at DESC LIMIT 1;
--    у меня: 4490753912889489942

-- 2. «случайно» удалить половину строк
DELETE FROM silver.order_facts WHERE custkey % 2 = 0;      -- удалено 28 строк, осталось 29

-- 3. посмотреть старую версию, не меняя таблицу (time travel)
SELECT count(*) FROM silver.order_facts FOR VERSION AS OF 4490753912889489942;   -- 57

-- 4. вернуть таблицу к снапшоту
CALL lakehouse.system.rollback_to_snapshot('silver', 'order_facts', 4490753912889489942);
SELECT count(*) FROM silver.order_facts;                   -- 57
```

### 6.2. Подключить инструмент к lakehouse

| Инструмент | Почему выбрал именно его |
|---|---|
| Metabase (BI) | Задача: показать данные Bronze в виде, понятном без знания SQL. Metabase открытый (open source), поднимается одним контейнером и имеет встроенный драйвер Starburst/Trino, поэтому подключается к уже работающему Trino без установки плагинов и без отдельной БД. Это важно: сам Metabase Iceberg не читает, он спрашивает данные у Trino, а Trino берёт метаданные у Lakekeeper и файлы из MinIO, то есть вся цепочка lakehouse работает целиком. Альтернатива Apache Superset тоже подключается к Trino, но тяжелее: ему нужен дополнительный драйвер, отдельная база метаданных и несколько сервисов, а на ноутбуке с ограниченной памятью это лишние ~2 ГБ. Движки Doris/StarRocks/Spark тоже читают Iceberg, но это ещё один тяжёлый SQL-движок рядом с уже имеющимся Trino, а задача была показать работу с данными, а не заменять движок. Metabase занимает около 1,3 ГБ, ограничение в compose. |

**Скрипты развертывания.** Сервис добавлен в тот же `docker-compose.yml` (блок `metabase` приведён выше в п. 2) и запущен командой:

```bash
docker compose up -d       # остальные контейнеры не пересоздаются, стартует только metabase
docker logs metabase 2>&1 | grep -i "initialization complete"   # проверка готовности (у меня: за 13,6 с)
```

Веб-интерфейс: `http://localhost:3000`.

**Мои действия в интерфейсе.**


1. Открыл `http://localhost:3000`, прошёл первичную настройку (локальная учётная запись), на шаге «Add your data» выбрал «добавлю данные позже».

2. Admin settings → Databases → Add a database, тип **Starburst (Trino)**: Display name `Lakehouse`, Host `trino` (имя контейнера в Docker-сети, а не `localhost`), Port `8080`, Catalog `lakehouse`, Schema `bronze`, Username `admin`, пароль пустой, SSL выключен. Статус подключения: «Подключено».

![Подключение базы Lakehouse в Metabase](screenshots/01-connection.png)

3. В обозревателе данных база Lakehouse показала три таблицы Bronze: `Customer`, `Lineitem`, `Orders`. Таблица `Orders` открылась с колонками `Orderkey`, `Custkey`, `Orderdate`, `Totalprice` (показаны первые 2000 строк, время ответа 268 мс).

![Таблицы схемы bronze](screenshots/02-tables.png)

![Таблица Orders](screenshots/03-orders.png)

4. SQL-запрос `SELECT count(*) FROM bronze.lineitem` вернул **6 001 215**, что совпадает с числом строк Bronze из п. 4.

![Результат count(*) по lineitem](screenshots/04-count.png)

5. SQL-запрос по годам:

```sql
SELECT year(orderdate) AS year, count(*) AS orders_count
FROM bronze.orders
GROUP BY 1
ORDER BY 1
```

| year | orders_count |
|---|---|
| 1992 | 227 089 |
| 1993 | 226 645 |
| 1994 | 227 597 |
| 1995 | 228 637 |
| 1996 | 228 626 |
| 1997 | 227 783 |
| 1998 | 133 623 |

Сумма по годам равна 1 500 000, то есть числу строк `bronze.orders` (п. 4). Результат визуализирован столбчатой диаграммой:

![Заказы по годам](screenshots/05-chart.png)

## 7. Выводы

Медальонная архитектура разделила работу на понятные этапы: Bronze хранит сырые данные только с нужными колонками, Silver подготавливает «крупные заказы» и склеивает их с клиентами, а Gold даёт готовый ответ на Q18. Благодаря этому тяжёлая агрегация 6 млн строк выполняется один раз, а следующие слои состоят из 57 строк, и корректность Gold подтверждена сравнением с исходным запросом (`EXCEPT` дал 0 строк в обе стороны). Для хранения выбран формат Parquet со сжатием ZSTD: на моих данных он оказался и меньше (19,82 МБ против 21,61 МБ), и быстрее (в среднем 919 мс против 1168 мс), чем GZIP. Партиционирование не применялось, потому что запрос читает таблицы целиком, и разбиение только создало бы множество мелких файлов. Формат таблиц Iceberg вместе с каталогом Lakekeeper дал возможности, недоступные простым файлам: откат к снапшоту и изменение схемы без переписывания данных.
