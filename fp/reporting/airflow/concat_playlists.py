import pandas as pd
import sys

def concat_playlists():
  print(sys.argv[0])
  print(sys.argv[1])
  print(sys.argv[2])
  print(sys.argv[3])

  df_old_cols = ['artists', 'genre', 'playlist_owner', 'release_date', 'release_title',
  'repertoire_pools', 'status', 'streaming_services', 'take_down_date','territory', 'upc']

  df_old = pd.read_csv(sys.argv[1], sep='\t',header=0, error_bad_lines=False, index_col=False, dtype='unicode', names=df_old_cols)

  col_names = ['upc','release_title','artists', 'genre', 'status', 'release_date', 'take_down_date', 'playlist_owner', 
  'repertoire_pools', 'streaming_services', 'territory']

  dtype_dict = {
  'upc':str
  }

  df_new = pd.read_csv(sys.argv[2], index_col=None, dtype=dtype_dict, header=0, names=col_names, error_bad_lines=False)

  print(df_new.shape)
  print(df_new.head())

  print(df_new.columns)
  print(df_old.columns)

  truly_new = df_new[(df_new['upc'].isnull()==False)&(df_new['upc'].isin(df_old['upc'].unique())==False)]
  truly_new['release_date'] = pd.to_datetime(truly_new.release_date)
  truly_new['take_down_date'] = pd.to_datetime(truly_new.take_down_date)

  all_data = pd.concat([df_old,truly_new], ignore_index=True, sort=True)

  
  all_data.to_csv(sys.argv[3], index=False)


  
if __name__ == "__main__":
    concat_playlists()