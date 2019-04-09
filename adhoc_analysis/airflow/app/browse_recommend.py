import numpy as np
import pandas as pd
import sys

def get_browse_playlists():

	playlists_list = pd.read_csv(sys.argv[1])

	playlists_data = pd.read_csv(sys.argv[2])
	playlists_data['long_uri'] = ['spotify:user:'+str(x) for x in playlists_data.playlist_uri]

	data = playlists_list.merge(playlists_data, how='left', left_on='playlist_uri', right_on='long_uri')

### Get the 'same_as_country' column

	data['same_as_country'] = [1 if x==y else 0 for (x,y) in zip(data.territory,data.country_owner)]

### Get the 'count_appearance' column

	playlist_count = data.playlist_uri_x.value_counts()
	playlist_count = pd.DataFrame(playlist_count).reset_index().rename(columns={'index':'playlist_uri',
                                                                            'playlist_uri_x':'count_appearance'})
	data = data.merge(playlist_count, how='left', left_on = 'playlist_uri_x', right_on = 'playlist_uri')

### Get the 'local' column

	data['local'] = [1 if x<20 else 0 for x in data.count_appearance]

### Get the 'mean_listeners_cat_terr' column

	by_cat_terr = data.groupby(by=['territory','category_id']).listeners.mean()
	df_cat_terr = pd.DataFrame(by_cat_terr).reset_index()
	df_cat_terr = df_cat_terr.rename(columns = {'listeners':'mean_listeners_cat_terr'})

	data = data.merge(df_cat_terr, how='left', left_on = ['territory','category_id'], 
                  	  right_on = ['territory','category_id'])

### Get the 'popular' column

	data['popular'] = [1 if x>y else 0 for (x,y) in zip(data.listeners,data.mean_listeners_cat_terr)]

### Get the 'score' column with the overall score
	data['score'] = data.same_as_country + data.local + data.popular

	cols = ['territory', 'category_id', 'Position', 'playlist_uri',
       'playlist_name_x', 'listeners', 'country_owner', 'count_appearance',
        'mean_listeners_cat_terr', 'same_as_country', 'local', 'popular',
        'score']


	data = data[cols].sort_values(by=['territory','category_id','score'], ascending=False)
	data_final = pd.DataFrame(data.groupby(by=['territory','category_id']).head(10).reset_index())

	data_final.to_csv(sys.argv[3], index=False)

if __name__ == "__main__":
    get_browse_playlists()

