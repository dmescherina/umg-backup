import numpy as np
import pandas as pd
import sys

import pickle
#import joblib

from pitched_recommend import Recommender

from google.cloud import storage

def get_recommendations():
    playlists = pd.read_csv(sys.argv[1])

    storage_client = storage.Client()
    bucket = storage_client.get_bucket('umg-comm-tech-dev')
    model_bucket='recommender/model/model_latest.pkl'
    model_local='model.pkl'
    blob = bucket.blob(model_bucket)
    blob.download_to_filename(model_local)

    meta_bucket='recommender/metadata/metadata_dict.pkl'
    meta_local='meta_dict.pkl'
    blob_meta = bucket.blob(meta_bucket)
    blob_meta.download_to_filename(meta_local)
    
    model = pickle.load(open(model_local, 'rb'))

    metadata = pickle.load(open(meta_local, 'rb'))

    all_territories = list(playlists.territory.unique())

    rec = []

    df=playlists.copy()

    for territory in all_territories:
    #print("Producing recommendations for ", territory)
        terr_cat = list(df[df.territory==territory].category_id.unique())
        for category in terr_cat:
        #print("Producing recommendations for category ", category)
            terr_cat_playlists = list(df[(df.territory==territory)&(df.category_id==category)].playlist_uri.unique())
            rec.extend(model.rec_multiple_playlists(terr_cat_playlists, model.playlists_sparse, territory=territory,
                                          category = category, N=150))

    recommendations = [(model.isrcs[x[0]],float(x[1]),x[2],x[3],x[4]) for x in rec]
    recs_final = [(x[0],x[1],metadata['track_artist'][x[0]], metadata['track_title'][x[0]], metadata['major_label'][x[0]], metadata['label_studio'][x[0]],
               metadata['original_release_date'][x[0]], metadata['genre_name'][x[0]], metadata['parent_genre_name'][x[0]], 
               x[2], x[3], x[4]) for x in list(recommendations)]

    all_columns = ['isrc','score','artist','title','label_studio','major_label','genre_name',
               'parent_genre_name','original_release_date','seed_playlist_uri','territory','category']
    rec_df = pd.DataFrame(recs_final, columns = all_columns)
    rec_df_umg = rec_df[rec_df.major_label=='UMG']

    final_columns = ['territory', 'category', 'score', 'isrc', 'seed_playlist_uri', 'artist', 'title', 'original_release_date']
    rec_df_final = rec_df_umg[final_columns]

    rec_df_final.to_csv(sys.argv[2], index=False, sep='\t')

if __name__ == "__main__":
    get_recommendations()
