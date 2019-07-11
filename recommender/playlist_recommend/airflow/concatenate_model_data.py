import sys
import glob
import os

import pandas as pd

def concatenate_model_data():
	#path = 'r'+sys.argv[1]
	#all_files = glob.glob(path + "/model_data_{{ macros.ds_format(macros.ds_add( ds, -1), \'%Y-%m-%d\', \'%Y%m%d\') }}-*.csv")
	all_files = [root+filename for root, dirs, files in os.walk(sys.argv[1]) for filename in files]
	print(all_files)
	playlists_data = pd.concat((pd.read_csv(f, delimiter='\t', header=None,
                                       names = ['playlist_uri','isrc','days_on_playlist',
                                                'track_artist','track_title']) for f in all_files), ignore_index=True)
	playlists_data = playlists_data.fillna("Unknown")
	playlists_data.to_csv(sys.argv[2], sep='\t', index=False, header=None)

if __name__ == "__main__":
	concatenate_model_data()