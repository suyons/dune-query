WITH
  user_volume AS (
    SELECT
      "from" AS from_address,
      sum(value / 1e18) AS eth_volume
    FROM
      ethereum.transactions
    WHERE
      block_time > now () - interval '24' hour
      AND success = true
    GROUP BY
      1
  ),
  eth_price AS (
    SELECT
      timestamp,
      price
    FROM
      prices.latest
    WHERE
      blockchain = 'ethereum'
      AND symbol = 'WETH'
    ORDER BY
      timestamp DESC
    LIMIT
      1
  )
SELECT
  uv.from_address,
  oa.custody_owner AS owner_name,
  round(uv.eth_volume, 2) AS eth_volume,
  round(uv.eth_volume * ep.price, 2) AS usd_volume,
  RANK() OVER (
    ORDER BY
      uv.eth_volume DESC
  ) AS volume_rank
FROM
  user_volume uv
  LEFT JOIN eth_price ep ON true
  LEFT JOIN labels.owner_addresses oa ON uv.from_address = oa.address
  AND oa.blockchain = 'ethereum'
ORDER BY
  3 DESC
LIMIT
  100;