# old_optimize
PROD and QA files for Optimize
* prod_20190321 were the PROD Optimize files before we did the playlist_uri fix
* qa_20180711 were original QA files which seemed to be identical to the original PROD
* qa_20190321 was the proper QA environment I created, running on and poiniting to QA environment on Google Composer **comm-tech-flow-qa**:
* QA GCS bucket: https://console.cloud.google.com/storage/browser/us-central1-comm-tech-flow--636e9a31-bucket
* QA Airflow: https://i312fd71e7f296680-tp.appspot.com/
* prod_20190326 are the latest files with all the fixes for playlist_uri using REGEX to extract playlist_id for tables joining
