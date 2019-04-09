SELECT
date,
tier,
territory_code,
slot_code,
campaign_code,
SUM(album) as album_clicks,
SUM(playlist) as playlist_clicks,
SUM(views) as views_count,
SUM(clicks) as clicks_count,
IF ((SUM(views)>950) OR (SUM(clicks)>10), 1, 0) as valid_field
FROM
  `umg-comm-tech-dev.adhoc.hpto_tier_territory`
GROUP BY
date,
tier,
territory_code,
slot_code,
campaign_code
ORDER BY
date,
tier,
territory_code,
slot_code,
campaign_code