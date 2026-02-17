SELECT
  date_trunc ('hour', et.block_time) AS block_datetime,
  round(sum(et.value / 1e18), 2) AS eth_volume,
  round(sum(et.value / 1e18) * any_value (ph.price), 2) AS usd_volume
FROM
  ethereum.transactions et
  LEFT JOIN prices.hour ph ON date_trunc ('hour', et.block_time) = date_trunc ('hour', ph.timestamp)
  AND blockchain = 'ethereum'
  AND symbol = 'WETH'
WHERE
  et.block_time > now () - interval '24' hour
GROUP BY
  1
ORDER BY
  1 ASC;