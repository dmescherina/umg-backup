SELECT
  parent_brand,
  brand,
  name,
  napster_link,
  spotify_id
FROM
  `umg-comm-tech-dev.deezer.create_export_napster`
WHERE
  napster_link IS NOT NULL