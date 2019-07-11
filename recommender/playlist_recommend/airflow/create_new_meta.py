import numpy as np
import pandas as pd

import sys

import pickle

def create_new_meta():
	all_columns = ['isrc','score','artist','title','label_studio','major_label','genre_name',
               'parent_genre_name','original_release_date','seed_playlist_uri','territory','category']
	meta_data = pd.read_csv(sys.argv[1], delimiter='\t', header=None, 
                        names = ['isrc','track_artist','track_title','label_studio','major_label',
                                 'genre_name','parent_genre_name','original_release_date'], dtype=str)
	meta_data = meta_data.fillna('Unknown')
	master_meta = meta_data.set_index('isrc', drop=True)
	meta_dict = master_meta.to_dict()

	with open(sys.argv[2], 'wb') as file:
		pickle.dump(meta_dict, file)

if __name__ == "__main__":
	create_new_meta()