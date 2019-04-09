SELECT
  date,
  hits.page.pagePath,
  hits.eventInfo.eventCategory,
  hits.eventInfo.eventAction,
  hits.eventInfo.eventLabel,
  hits.eventInfo.eventValue 
FROM
  `universal-music-bigquery.172831759.ga_sessions_*`, UNNEST(hits) as hits