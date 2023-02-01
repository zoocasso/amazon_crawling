WITH ab AS (SELECT url, COUNT(product_idx) AS a, COUNT(distinct create_date) AS b FROM amz_product_info_tb
GROUP BY url)
SELECT url FROM ab
WHERE b > 1
ORDER BY a
;