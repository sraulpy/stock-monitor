--TAREA REDPANDA 

1- Crear un stream para los eventos de Finnhub
CREATE STREAM stock_stream (symbol STRING, price DOUBLE, volume DOUBLE, timestamp STRING) 
WITH (KAFKA_TOPIC='stock-updates', VALUE_FORMAT='JSON',partitions=1);

2- 
CREATE TABLE symbol_stats AS
SELECT symbol,
       AVG(price) AS avg_price,
       COUNT(*) AS transaction_count,
       MAX(price) AS max_price,
       MIN(price) AS min_price
FROM stock_stream
GROUP BY symbol emit changes;

3 CONSULTAS
-- Consulta 1: Promedio ponderado de precio por símbolo
SELECT symbol, SUM(price * volume) / SUM(volume) AS weighted_avg_price
FROM stock_stream
GROUP BY symbol emit changes;

-- Consulta 2: Cantidad de transacciones por símbolo
SELECT symbol, COUNT(*) AS transaction_count
FROM stock_stream
GROUP BY symbol emit changes;

-- Consulta 3: Máximo precio registrado por símbolo
SELECT symbol, MAX(price) AS max_price
FROM stock_stream
GROUP BY symbol emit changes;

-- Consulta 4: Mínimo precio registrado por símbolo
SELECT symbol, MIN(price) AS min_price
FROM stock_stream
GROUP BY symbol emit changes;
