SELECT
  stream_date,
  f.source_uri,
  position,
  track_uri,
  isrc,
  track,
  f.streams,
  skips,
  length,
  disc,
  col,
  ret,
  sup,
  httype,
  baseline,
  skipsperc,
  skipsperf,
  lengthperf,
  discperf,
  colperf,
  retperf,
  supperf,
  skipsb4,
  skipsb4perf,
  skipsperf-CASE
    WHEN usercluster = 1 THEN 0.000688+0.1021233*discperf+0.0792791*POW(discperf,2)+ 0.0610996*POW(discperf,3)
    WHEN usercluster = 2 THEN 0.007967+0.057036*discperf -0.223396*POW(discperf,2) -0.489979*POW(discperf,3)
    WHEN usercluster = 3 THEN -0.0007416+0.1266863*discperf +0.2081091*POW(discperf,2) -1.2255956 *POW(discperf,3)
    WHEN usercluster = 4 THEN 0.0007586+0.0732498*discperf +0.0086405*POW(discperf,2)+ 0.0282904*POW(discperf,3)
    WHEN usercluster = 5 THEN -0.002138+0.079274*discperf +0.6251*POW(discperf,2) -0.162375*POW(discperf,3) -10.435921*POW(discperf,4)+2.710223*POW(discperf,5)+38.54804*POW(discperf,6) -9.899568*POW(discperf,7)
    WHEN usercluster = 6 THEN 0.0006367+1.12E-01*discperf +0.0145771*POW(discperf,2) -0.1002147*POW(discperf,3)
    WHEN usercluster = 7 THEN 0.00201 + 0.114264*discperf +0.002789*POW(discperf,2) -0.039977*POW(discperf,3)
    WHEN usercluster = 8 THEN 0.00193+0.068714*discperf -0.036026*POW(discperf,2)+ 0.308078*POW(discperf,3)
    WHEN usercluster = 9 THEN 0.003698+0.096162*discperf +0.249971*POW(discperf,2) -0.098588*POW(discperf,3) -0.722229*POW(discperf,4) -0.186934*POW(discperf,5) -10.880121*POW(discperf,6)
    WHEN usercluster = 10 THEN 0.002203+0.098643*discperf +0.026109*POW(discperf,2)+ 0.107929*POW(discperf,3)
    WHEN usercluster = 11 THEN 0.001446+0.109677*discperf +0.065806*POW(discperf,2) -0.027251*POW(discperf,3)
    WHEN usercluster = 12 THEN 0.003731+0.115204*discperf -0.027575*POW(discperf,2) -0.27075*POW(discperf,3)
    WHEN usercluster = 13 THEN -0.0001681+0.0829937*discperf+ 0.1892607*POW(discperf,2)+ 0.1062685*POW(discperf,3) END as skipsperfbench,  colperf - CASE
    WHEN usercluster = 1 THEN -0.0021214+-0.0615389*discperf + 0.0793316*POW(discperf,2)+ 0.2010129*POW(discperf,3) + -0.6709576*POW(discperf,4) + -0.4486741*POW(discperf,5) + 0.7492039*POW(discperf,6)
    WHEN usercluster =2 THEN 0.0005769+-0.0373199*discperf + -0.0408422*POW(discperf,2)+ 0.1481123*POW(discperf,3) + 0.1736785*POW(discperf,4) + -0.2467469*POW(discperf,5) + -0.2617539*POW(discperf,6)
    WHEN usercluster =3 THEN -0.0004159+-0.0220563*discperf + 0.009197*POW(discperf,2)+ 0.0272916*POW(discperf,3) + -0.186623*POW(discperf,4) + -0.0552452*POW(discperf,5) + 0.1361593*POW(discperf,6)
    WHEN usercluster =4 THEN -0.0014509+-0.0871488*discperf + 0.1486422*POW(discperf,2)+ 0.6684835*POW(discperf,3) + -1.0260448*POW(discperf,4) + -1.7383111*POW(discperf,5) + 0.8413712*POW(discperf,6)
    WHEN usercluster =5 THEN -0.0007168+-0.0855024*discperf + 0.0104649*POW(discperf,2)+ 0.4641368*POW(discperf,3) + 0.0447037*POW(discperf,4) + -0.9980484*POW(discperf,5) + -0.6820985*POW(discperf,6)
    WHEN usercluster =6 THEN -0.0026745+-0.2032693*discperf + 0.3245161*POW(discperf,2)+ 0.7469477*POW(discperf,3) + -2.2393951*POW(discperf,4) + -0.6970455*POW(discperf,5) + 2.8447547*POW(discperf,6)
    WHEN usercluster =7 THEN -0.002475+-0.110097*discperf + 0.162986*POW(discperf,2)+ -0.410583*POW(discperf,3) + -1.006143*POW(discperf,4) + 6.842303*POW(discperf,5) + -7.54178*POW(discperf,6)
    WHEN usercluster =8 THEN -0.003767+-0.247144*discperf + 1.713972*POW(discperf,2)+ -7.894571*POW(discperf,3) + 9.575832*POW(discperf,4) + 22.159188*POW(discperf,5) + -42.107533*POW(discperf,6)
    WHEN usercluster =9 THEN -0.0007428+-0.033918*discperf + 0.029155*POW(discperf,2)+ 0.0721808*POW(discperf,3) + -0.3445946*POW(discperf,4) + -0.1042575*POW(discperf,5) + 0.3981898*POW(discperf,6)
    WHEN usercluster =10 THEN 0.03457+-0.09058*discperf + -0.3287*POW(discperf,2)+ 0.03342*POW(discperf,3) + 0.30714*POW(discperf,4)
    WHEN usercluster =11 THEN -0.003659+-0.116799*discperf + 0.112417*POW(discperf,2)+ 0.736075*POW(discperf,3) + -0.55447*POW(discperf,4) + -2.851031*POW(discperf,5) + -1.939948*POW(discperf,6)
    WHEN usercluster =12 THEN -0.002257+-0.098671*discperf + 0.049962*POW(discperf,2)+ 0.430278*POW(discperf,3) + -0.257121*POW(discperf,4) + -0.803388*POW(discperf,5) + -0.013702*POW(discperf,6)
    WHEN usercluster =13 THEN -0.005356+-0.229557*discperf + 0.688396*POW(discperf,2)+ -0.95063*POW(discperf,3) + -1.296703*POW(discperf,4) + 6.137863*POW(discperf,5) + -5.886972*POW(discperf,6) END as colperfbench,
  usercluster
FROM
  `umg-comm-tech-dev.optimize_qa.Ar_TPR_Streams_Final1_Days_stats2` f
JOIN
  `umg-comm-tech-dev.optimize_qa.ar_tpe_playlistclusters_base_perc` a
ON
  f.source_uri = a.source_uri
WHERE
  userindex = 1;