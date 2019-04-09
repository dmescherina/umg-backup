SELECT
date,
pagePath,
regexp_extract(pagePath,r"tiers/(.)/") as tier,
regexp_extract(pagePath,r'territories/([0-9]{1,2})/') as territory_code,
eventCategory,
IF(eventCategory='slot',1, NULL) as views,
IF(eventCategory='playlist',1, NULL) as clicks,
eventLabel,
regexp_extract(eventLabel,r'slot:([0-9]{2,4})') as slot_code,
regexp_extract(eventLabel,r'campaign:([0-9]{3,4})') as campaign_code,
IF(regexp_extract(eventLabel,r'album+')='album',1,0) as album,
IF(regexp_extract(eventLabel,r'playlist+')='playlist',1,0) as playlist
FROM
  `umg-comm-tech-dev.adhoc.htpo_raw_ga`