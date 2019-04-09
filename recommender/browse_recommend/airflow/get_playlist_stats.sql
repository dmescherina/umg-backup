SELECT
  DISTINCT playlist_history.playlist_name,
  listeners,
  country_owner,
  data.playlist_owner,
  ranking,
  pl_ranking,
  data.playlist_uri
FROM (
  SELECT
    *
  FROM (
    SELECT
      playlist_name,
      COUNT(DISTINCT user_id) AS listeners,
      user_country_code  AS country_owner,
      playlist_owner,
      playlist_uri,
      RANK() OVER (PARTITION BY playlist_uri ORDER BY COUNT(DISTINCT user_id) DESC) AS ranking
    FROM (
      SELECT
        user_id,
        user_country_code,
        CASE
          WHEN source_uri LIKE 'spotify:user%' THEN REPLACE(source_uri, REGEXP_EXTRACT(source_uri, r"user:[^:]*:"), "")
          ELSE source_uri
        END AS source_uri
      FROM
        `umg-partner.spotify.streams`
      WHERE
        stream_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 33 DAY)
        AND _PARTITIONDATE > DATE_SUB(CURRENT_DATE(), INTERVAL 35 DAY) ) AS AllStreams
    INNER JOIN (
      SELECT
        DISTINCT playlist_name,
        playlist_owner,
        CASE
          WHEN playlist_uri LIKE 'spotify:user%' THEN REPLACE(playlist_uri, REGEXP_EXTRACT(playlist_uri, r"user:[^:]*:"), "")
          ELSE playlist_uri
        END AS playlist_uri
      FROM
        `umg-partner.spotify.playlist_history`
      WHERE
        playlist_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 33 DAY)
        AND _PARTITIONDATE > DATE_SUB(CURRENT_DATE(), INTERVAL 35 DAY)) `umg-partner.spotify.playlist_history`
    ON
      AllStreams.source_uri = `umg-partner.spotify.playlist_history`.playlist_uri
    GROUP BY
      playlist_name,
      playlist_owner,
      country_owner,
      playlist_uri
    HAVING
      COUNT(user_id) > 500)
  WHERE
    ranking = 1
  ORDER BY
    listeners DESC ) AS data
INNER JOIN (
  SELECT
    DISTINCT playlist_name,
    ROW_NUMBER() OVER (PARTITION BY playlist_id ORDER BY playlist_date DESC) AS pl_ranking,
    CASE
      WHEN playlist_uri LIKE 'spotify:user%' THEN REPLACE(playlist_uri, REGEXP_EXTRACT(playlist_uri, r"user:[^:]*:"), "")
      ELSE playlist_uri
    END AS playlist_uri
  FROM
    `umg-partner.spotify.playlist_history`
  WHERE
    playlist_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 33 DAY)
    AND _PARTITIONDATE > DATE_SUB(CURRENT_DATE(), INTERVAL 35 DAY)) playlist_history
ON
  data.playlist_uri = playlist_history.playlist_uri
WHERE
  ranking = 1
  AND pl_ranking = 1
ORDER BY
  listeners DESC