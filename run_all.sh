#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
SF="${1:-sf1}"
mkdir -p results
TR=(docker exec -i trino trino --catalog lakehouse)
sql()       { "${TR[@]}" --execute "$1"; }
sql_codec() { "${TR[@]}" --session "lakehouse.compression_codec=$1" --execute "$2"; }
val()       { "${TR[@]}" --output-format CSV_UNQUOTED --execute "$1"; }
size_mb()   { val "SELECT coalesce(round(CAST(sum(file_size_in_bytes) AS double)/1048576, 3), 0) FROM $1.\"$2\$files\""; }

echo "== Ждём Trino =="
until val "SELECT 1" >/dev/null 2>&1; do sleep 5; done

sql "CREATE SCHEMA IF NOT EXISTS bronze"
sql "CREATE SCHEMA IF NOT EXISTS silver"
sql "CREATE SCHEMA IF NOT EXISTS gold"

Q18B="/* q18bronze */ SELECT c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice, sum(l.quantity) AS total_quantity
FROM lakehouse.bronze.customer c
JOIN lakehouse.bronze.orders o ON c.custkey = o.custkey
JOIN lakehouse.bronze.lineitem l ON o.orderkey = l.orderkey
WHERE o.orderkey IN (SELECT orderkey FROM lakehouse.bronze.lineitem GROUP BY orderkey HAVING sum(quantity) > 300)
GROUP BY c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice
ORDER BY o.totalprice DESC, o.orderdate
LIMIT 100"

build_bronze() {
  local codec="$1" spec t cols
  for spec in "customer:custkey, name" \
              "orders:orderkey, custkey, orderdate, totalprice" \
              "lineitem:orderkey, quantity"; do
    t="${spec%%:*}"; cols="${spec#*:}"
    sql_codec "$codec" "DROP TABLE IF EXISTS bronze.$t"
    sql_codec "$codec" "CREATE TABLE bronze.$t WITH (format = 'PARQUET') AS SELECT $cols FROM tpch.$SF.$t"
  done
}

bench() {
  local i out=""
  for i in 1 2 3; do
    "${TR[@]}" --execute "$Q18B" >/dev/null
    out+="$(val "SELECT date_diff('millisecond', started, \"end\") FROM system.runtime.queries WHERE query LIKE '/* q18bronze */%' AND state = 'FINISHED' ORDER BY \"end\" DESC LIMIT 1") "
  done
  echo "$out"
}

echo "== Тест сжатия на Bronze (scale: $SF) =="
{
  echo "Масштаб: $SF"
  echo
  echo "| Сжатие на Bronze | Время запроса, мс (3 прогона) | Размер Bronze, МБ |"
  echo "|---|---|---|"
} > results/compression.md

for codec in GZIP ZSTD; do
  echo "--- $codec ---"
  build_bronze "$codec"
  total=$(for t in customer orders lineitem; do size_mb bronze "$t"; done | awk '{s+=$1} END {printf "%.2f", s}')
  times=$(bench)
  echo "| $codec | ${times% } | $total |" | tee -a results/compression.md
done

echo "== Silver =="
sql "DROP TABLE IF EXISTS silver.big_orders"
sql "CREATE TABLE silver.big_orders WITH (format = 'PARQUET') AS
     SELECT orderkey, sum(quantity) AS total_quantity
     FROM bronze.lineitem GROUP BY orderkey HAVING sum(quantity) > 300"
sql "DROP TABLE IF EXISTS silver.order_facts"
sql "CREATE TABLE silver.order_facts WITH (format = 'PARQUET') AS
     SELECT c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice, b.total_quantity
     FROM bronze.orders o
     JOIN bronze.customer c ON c.custkey = o.custkey
     JOIN silver.big_orders b ON b.orderkey = o.orderkey"

echo "== Gold =="
sql "DROP TABLE IF EXISTS gold.top100_customers"
sql "CREATE TABLE gold.top100_customers WITH (format = 'PARQUET') AS
     SELECT name, custkey, orderkey, orderdate, totalprice, total_quantity
     FROM silver.order_facts ORDER BY totalprice DESC, orderdate LIMIT 100"

echo "== Метрики слоёв =="
{
  echo "| Таблица | Строк | Вес, МБ |"
  echo "|---|---|---|"
  for st in bronze.customer bronze.orders bronze.lineitem silver.big_orders silver.order_facts gold.top100_customers; do
    s="${st%%.*}"; t="${st#*.}"
    echo "| $st | $(val "SELECT count(*) FROM $s.$t") | $(size_mb "$s" "$t") |"
  done
} | tee results/layers.md

echo "== Проверка: Gold == оригинальный Q18 на tpch.$SF (ожидаем 0 и 0) =="
ORIG="SELECT c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice, sum(l.quantity) AS total_quantity
FROM tpch.$SF.customer c JOIN tpch.$SF.orders o ON c.custkey = o.custkey
JOIN tpch.$SF.lineitem l ON o.orderkey = l.orderkey
WHERE o.orderkey IN (SELECT orderkey FROM tpch.$SF.lineitem GROUP BY orderkey HAVING sum(quantity) > 300)
GROUP BY c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice
ORDER BY o.totalprice DESC, o.orderdate LIMIT 100"
GOLD="SELECT name, custkey, orderkey, orderdate, totalprice, total_quantity FROM gold.top100_customers"
{
  echo "orig EXCEPT gold: $(val "SELECT count(*) FROM (($ORIG) EXCEPT ($GOLD))")"
  echo "gold EXCEPT orig: $(val "SELECT count(*) FROM (($GOLD) EXCEPT ($ORIG))")"
} | tee results/check.txt

echo "== Time travel на Silver =="
{
  S1=$(val "SELECT snapshot_id FROM silver.\"order_facts\$snapshots\" ORDER BY committed_at DESC LIMIT 1")
  echo "Снапшот до удаления: $S1"
  echo "Строк до:  $(val 'SELECT count(*) FROM silver.order_facts')"
  sql "DELETE FROM silver.order_facts WHERE custkey % 2 = 0"
  echo "Строк после DELETE: $(val 'SELECT count(*) FROM silver.order_facts')"
  echo "Чтение старой версии (FOR VERSION AS OF): $(val "SELECT count(*) FROM silver.order_facts FOR VERSION AS OF $S1")"
  sql "CALL lakehouse.system.rollback_to_snapshot('silver', 'order_facts', $S1)"
  echo "Строк после rollback: $(val 'SELECT count(*) FROM silver.order_facts')"
} 2>&1 | tee results/time_travel.log

echo "== Schema evolution на Silver =="
{
  sql "ALTER TABLE silver.order_facts ADD COLUMN note varchar"
  sql "ALTER TABLE silver.order_facts RENAME COLUMN total_quantity TO qty_total"
  sql "DESCRIBE silver.order_facts"
  sql "SELECT name, orderkey, qty_total, note FROM silver.order_facts ORDER BY totalprice DESC LIMIT 3"
} 2>&1 | tee results/schema_evolution.log

echo
echo "Готово. Результаты в папке results/."
