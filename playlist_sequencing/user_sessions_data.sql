#standardSQL
  #assigning session id's
CREATE TEMP FUNCTION
  assignsessionid(t ARRAY<TIMESTAMP>)
  RETURNS ARRAY<INT64>
  LANGUAGE js AS """
var arrayLength = t.length;
var timediff = [];
timediff[0] = 0;
for (let i=1; i < arrayLength; i++) {
  timediff[i] = (t[i] - t[i-1]) / 1000 / 60;
}
var session_id = [];
session_id[0] = 1;
for (let i = 1; i < arrayLength; i++) {
  if (timediff[i] <= 60) {
    session_id[i] = session_id[i-1];
  } else {
    session_id[i] = session_id[i-1] + 1;
  }
}
return session_id;
""";
SELECT
  *,
  assignsessionid(offline_timestamp) AS session_id
FROM (
  SELECT
    s.user_id,
    REGEXP_EXTRACT(s.source_uri,r'playlist:(.*)') AS playlist_id,
    s.isrc,
    s.user_country_code,
    s.user_gender,
    s.user_age_group,
    s.stream_duration,
    s.stream_source,
    s.device_type,
    s.os_name,
    s.engagement_style,
    s.partner_user_type,
    s.revenue_model,
    s.consumer_group,
    s.consumer_group_detail,
    s.partner_report_date,
    utc_timestamp_offset,
    ARRAY_AGG(CASE
        WHEN EXTRACT(YEAR  FROM  s.offline_timestamp) = 1970 OR s.offline_timestamp IS NULL THEN s.stream_datetime
      ELSE
      s.offline_timestamp
    END
    ORDER BY
      CASE
        WHEN EXTRACT(YEAR  FROM  s.offline_timestamp) = 1970 OR s.offline_timestamp IS NULL THEN s.stream_datetime
      ELSE
      s.offline_timestamp
    END
      ) AS offline_timestamp,
    COUNT(isrc) AS total_isrc_count,
    COUNTIF(completed_stream_flag IS TRUE) AS completed_isrc_count
  FROM
    `umg-partner.spotify.streams` AS s
  WHERE
    s.stream_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 9 DAY)
    AND s.stream_date < DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    AND REGEXP_EXTRACT(s.source_uri,r'playlist:(.*)') IN (
    SELECT
      playlist_id
    FROM
      `umg-comm-tech-dev.playlist_sequencing.playlists_create_spotify`
    GROUP BY
      playlist_id)
    AND cached_play_flag IS FALSE
    AND shuffle_play_flag IS FALSE
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16,
    17)