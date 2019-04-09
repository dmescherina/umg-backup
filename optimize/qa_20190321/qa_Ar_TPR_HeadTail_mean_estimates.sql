SELECT stream_date ,source_uri,testposition,httype
          ,AVG(ln(relposition)) AS xmean
          ,AVG(streams) AS ymean                                                 
    FROM `umg-comm-tech-dev.optimize_qa.AR_TPR_HeadTail_Build2` 
    GROUP BY stream_date ,source_uri,testposition,httype