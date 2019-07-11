"""
Created on Tue May 15 11:21:26 2018

@author: quintoe/usvyatd/meshchd
"""
from __future__ import division

import time
import logging

overall_start_time = time.time()


import pandas as pd
import scipy.sparse as sparse
import numpy as np
# from scipy.sparse.linalg import spsolve
import random
# import sys
import implicit
#from sklearn import metrics
#import gzip
import json
#import os
import pickle
import operator
#from pandas.io import gbq
#from pandas import ExcelWriter

#from datetime import date
#import pygsheets
#from pprint import pprint
#from oauth2client.service_account import ServiceAccountCredentials
#from googleapiclient.discovery import build
import itertools

logging.info("Importing packages took " + str(round(time.time() - overall_start_time, 4)) + " seconds.")


class Data_prep():
    def __init__(self, *initial_data, **kwargs):
        # Initialise parameters from a config dictionary
        for dictionary in initial_data:
            for key in dictionary:
                setattr(self, key, dictionary[key])
        for key in kwargs:
            setattr(self, key, kwargs[key])
        self.print_info = False

    #def load_data(self, playlist_data_path):, isrc_data_path,non_spotify_playlists_sample_path,spotify_playlists_sample_path):
        #self.playlists_data = pd.read_csv(playlist_data_path, error_bad_lines=False)
        #self.isrc_data = pd.read_table(isrc_data_path)
        ##self.non_spotify_playlists_sample = pd.read_table(non_spotify_playlists_sample_path)
        #self.spotify_playlists_sample = pd.read_table(spotify_playlists_sample_path)
        # Optionally print some info
        #if self.print_info:
        #    print('====== Playlist_data head ======')
        #    playlists_data.head()
        #    print('====== Playlist_data info ======')
        #    playlists_data.info()
        #    print('====== Isrc_data info ======')
            #isrc_data.info()
        #    print('====== non-spotify playlists sample head ======')
            #non_spotify_playlists_sample.head()
        #    print('====== Spotify playlists sample head ======')
            #spotify_playlists_sample.head()

    def prepare_data(self, playlist_data_path):
        playlists_data = pd.read_csv(playlist_data_path, delimiter='\t', header=None,
                                       names = ['playlist_uri','isrc','days_on_playlist',
                                                'playlist_owner','playlist_name','playlist_description'])
        self.playlist_lookup = playlists_data[['playlist_uri', 'playlist_owner', 'playlist_name','playlist_description']].drop_duplicates()  # Only get unique isrcs
        self.isrc_lookup['isrc'] = self.isrc_lookup.isrc.astype(str)  # Encode as string
        self.playlists = list(np.sort(playlists_data.playlist_uri.unique()))  # Get our unique playlists
        self.isrcs = list(playlists_data.isrc.unique())  # Get our unique isrcs
        self.dop = list(playlists_data.days_on_playlist)  # days on playlist
        self.cols = playlists_data.playlist_uri.astype(pd.api.types.CategoricalDtype(categories = self.playlists)).cat.codes  # Get the associated row indices
        self.rows = playlists_data.isrc.astype(pd.api.types.CategoricalDtype(categories = self.isrcs)).cat.codes  # Get the associated column indices
        self.playlists_sparse = sparse.csr_matrix((self.dop, (self.rows, self.cols)), shape=(len(self.isrcs), len(self.playlists)))
        # Optionally print info
        if self.print_info:
            matrix_size = self.playlists_sparse.shape[0] * self.playlists_sparse.shape[1]  # Number of possible interactions in the matrix
            num_isrcs = len(self.playlists_sparse.nonzero()[0])  # Number of isrcs interacted with
            sparsity = 100 * (1 - (num_isrcs / matrix_size))
            print('Sparsity', sparsity)

    #def make_trainset(self,ratings, pct_test=0.01, rseed=0):
        '''
        This function will take in the original playlist-isrc matrix and "mask" a percentage of the original ratings
        '''
    #    self.pct_test = pct_test # Log parameter value
    #    test_set = ratings.copy()  # Make a copy of the original set to be the test set.
    #    test_set[test_set != 0] = 1  # Store the test set as a binary preference matrix
    #    training_set = ratings.copy()  # Make a copy of the original data we can alter as our training set.
    #    nonzero_inds = training_set.nonzero()  # Find the indices in the ratings data where an interaction exists
    #    nonzero_pairs = list(zip(nonzero_inds[0], nonzero_inds[1]))  # Zip these pairs together of playlist,isrc index into list
    #    random.seed(rseed)  # Set the random seed for reproducibility
    #    num_samples = int(np.ceil(pct_test * len(nonzero_pairs)))  # Round the number of samples needed to the nearest integer
    #    samples = random.sample(nonzero_pairs,num_samples)  # Sample a random number of playlist-isrc pairs without replacement
        # Make a data frame that stores which isrcs from which playlists have been held out of the training set.
    #    holdout_plist = [x[0] for x in samples]
    #    holdout_isrcs = [x[1] for x in samples]
    #    holdout_set_lookup = pd.DataFrame(data={'playlist_idx':holdout_plist,'isrc_idx':holdout_isrcs})
    #    playlist_inds = [index[0] for index in samples]  # Get the playlist row indices
    #    isrc_inds = [index[1] for index in samples]  # Get the isrc column indices
    #    training_set[playlist_inds, isrc_inds] = 0  # Assign all of the randomly chosen playlist-isrc pairs to zero
    #    training_set.eliminate_zeros()  # Get rid of zeros in sparse array storage after update to save space
    #    return training_set, test_set, list(set(playlist_inds)), holdout_set_lookup  # Output trainset, testset (=untouched playlist_isrc matrix) the unique list of playlist rows that were altered and a data frame mapping heldout isrcs to their corresponding playlists.

    #def get_historic_isrcs(self, playlist_uri, playlists_trainset, playlist_list, isrc_list, isrc_lookup):
        '''
        This just tells me which isrcs have been already on a playlist in the training set.

        returns:

        A list of isrcs for particular playlist that have already been on the playlist in the training set
        '''
    #    plist_ind = np.where(playlist_list == playlist_uri)[0][0]  # Returns the index row of our playlist uri
    #    isrc_ind = playlists_trainset[plist_ind, :].nonzero()[1]  # Get column indices of isrcs for the playlist
    #    isrc_codes = isrc_list[isrc_ind]  # Get the isrcs from this playlist
    #    return isrc_lookup.loc[isrc_lookup.isrc.isin(isrc_codes)]


class Recommender_core():
    '''This class is designed to train and use the core recommendation engine'''

    def train(self, playlists_set, factors, regularization,iterations,alpha):
        self.model = implicit.als.AlternatingLeastSquares(factors=factors, regularization=regularization, iterations=iterations)
        self.model.fit((playlists_set.transpose() * alpha).astype('double'))
        self.playlist_vecs =  self.model.item_factors
        self.isrc_vecs = self.model.user_factors

    def recommend(self,isrc, playlist_isrc_matrix, N=10):
        recommend_start_time = time.time()
        '''Returns a list of tuple (item_index, score, target playlist) of recommended tracks'''
        isrc_index = self.isrcs.index(isrc) # get the index of the target playlist
        recommendations = self.model.recommend(isrc_index, playlist_isrc_matrix,N=N) # returns list of tuples (item_index, score)
        isrc_col = [isrc] * len(recommendations)
        playlists = [x[0] for x in recommendations]
        scores = [x[1] for x in recommendations]
        recommendations = map(lambda x,y,z:(x,y,z),playlists,scores,isrc_col)
        logging.info("Recommend module took " + str(round(time.time() - recommend_start_time, 4)) + " seconds.")
        return recommendations

    def recommend_outofmodel(self, new_playlist_data, playlist_isrc_matrix, N=10, include_original_tracks=False):
        '''Returns a list of tuples of recommended tracks for the playlist
        not in the model'''
        '''Computing new user factor vector as
        x_u = ((Y.T*Y + Y.T*(Cu - I) * Y) + lambda*I)^-1 * (X.T * Cu * p(u))'''
        #TODO: Starting with the case of one new playlist only. Expand to multiple later
        recommend_outofmodel_start = time.time()

        lambda_val = self.regularization
        user_size=len(new_playlist_data.playlist_uri.unique())
        item_size=playlist_isrc_matrix.shape[1]
        features = self.isrc_vecs.shape[1]

    #Get the sparse X matrix (vector) for the new playlist
        isrc_indices = []
        for i in range(0,new_playlist_data.shape[0]):
            ref_isrc = new_playlist_data['isrc'][i]
            if ref_isrc in self.isrcs:
                try:
                    ref_isrc_index = [i for i, j in enumerate(self.isrc_lookup.isrc) if j == ref_isrc]
                    sparce_data[ref_isrc_index] = new_playlist_data['days_on_playlist'][i]
                    isrc_indices.append(ref_isrc_index)
                except:
                    pass
        isrc_indices = [item for sublist in isrc_indices for item in sublist]

        new_dop = list(new_playlist_data.days_on_playlist)
        new_rows = new_playlist_data.playlist_uri.astype(pd.api.types.CategoricalDtype(categories=new_playlist_data.playlist_uri.unique())).cat.codes
        new_cols = new_playlist_data.isrc.astype(pd.api.types.CategoricalDtype(categories=self.isrcs)).cat.codes
        confidence = sparse.csr_matrix((new_dop, (new_rows, new_cols)), shape=(user_size, item_size))
        confidence = (confidence * self.alpha).astype('double')

        logging.info("Making the sparce matrix for the playlist data took " + str(round(time.time() - recommend_outofmodel_start, 4)) + " seconds.")
    # Compute the optimal latent features vector

        latent_feature_start = time.time()
        # Placeholder for the user item vector
        X = sparse.csr_matrix(np.random.normal(size = (user_size, features)))
        Y = self.isrc_vecs

        #Precompute I and lambda * I
        X_I = sparse.eye(user_size)
        Y_I = sparse.eye(item_size)
        I = sparse.eye(features)
        lI = lambda_val * I

        # Precompute X-transpose-X and Y-transpose-Y
        xTx = X.T.dot(X)
        yTy = Y.T.dot(Y)

            # Loop through all users
        for u in range(user_size):
            u_row = confidence[u,:].toarray()

            # Calculate the binary preference p(u)
            p_u = u_row.copy()
            p_u[p_u != 0] = 1.0

            # Calculate Cu and Cu - I
            CuI = sparse.diags(u_row, [0])
            Cu = CuI + Y_I

            # Put it all together and compute row factor vectors
            yT_CuI = Y.T*CuI
            yT_CuI_y = yT_CuI.dot(Y)
            yT_Cu = Y.T * Cu
            yT_Cu_pu = yT_Cu.dot(p_u.T)
            X[u] = sparse.linalg.spsolve(yTy + yT_CuI_y + lI, yT_Cu_pu)

            logging.info("Computing latent features for the playlist took " + str(round(time.time() - latent_feature_start, 4)) + " seconds.")

        # Compute the recommendations
        rec_start = time.time()
        scores = X.dot(self.isrc_vecs.T)
        scores = scores[0]
        logging.info("Computing recommendations vector took " + str(round(time.time() - rec_start, 4)) + " seconds.")

        #Exclude tracks already in the playlist
        beautify_start = time.time()
        if include_original_tracks==False:
            liked = set(list(new_cols))
        else:
            liked = set()
        count = N + len(liked)
        if count < len(scores):
            ids = np.argpartition(scores, -count)[-count:]
            best = sorted(zip(ids, scores[ids]), key=lambda x: -x[1])
        else:
            best = sorted(enumerate(scores), key=lambda x: -x[1])
        recommendations =  list(itertools.islice((rec for rec in best if rec[0] not in liked), N))

        playlist = [new_playlist_data['playlist_uri'][0]] * len(recommendations)
        isrcs = [x[0] for x in recommendations]
        scores = [x[1] for x in recommendations]
        recommendations = map(lambda x,y,z:(x,y,z),isrcs,scores,playlist)

        logging.info("Making final recommendations output took " + str(round(time.time() - beautify_start, 4)) + " seconds.")

        return recommendations

    def rec_to_isrc(self, recommendations):
        '''Turns the list of recommendations into a list of tuples (item_index_isrc, score, target playlist)'''
        return [ (self.playlists[x[0]],float(x[1]),x[2]) for x in recommendations ] # turns the results into a list of tuples (item_index_isrc, score)

    def rec_to_json(self,recommendations,metadata):

        json_start = time.time()

        recommendations = self.rec_to_isrc(recommendations) # turns the results into a list of tuples (item_index_isrc, score)
        recs_final = [(x[0],x[1],metadata['track_artist'][x[0]], metadata['track_title'][x[0]], metadata['major_label'][x[0]], metadata['label_studio'][x[0]],
        metadata['original_release_date'][x[0]], metadata['genre_name'][x[0]], metadata['parent_genre_name'][x[0]], x[2]) for x in list(recommendations)]
        keys = ['isrc','score','artist','title','major_label','label_studio','release_date','genre','parent_genre','seed_playlist_uri']
        list_of_dict = [dict(zip(keys,x)) for x in recs_final]
        
        logging.info("Making json recommendations output took " + str(round(time.time() - json_start, 4)) + " seconds.")

        return json.dumps(list_of_dict)

    def rec_readable(self,recommendations):
        '''Format recommendations in a human-readable format - i.e. with ISRC, trackname and artist name.
        Returns a Pandas DataFrame'''
        readable_start = time.time()

        recommendations = self.rec_to_isrc(recommendations) # turns the results into a list of tuples (item_index_isrc, score)
        playlists = [x[0] for x in recommendations]
        scores = [x[1] for x in recommendations]
        owner = [self.playlist_lookup.playlist_owner.loc[self.playlist_lookup.playlist_uri == x[0]].iloc[0] for x in recommendations] # get artist name for each isrc
        name  = [self.playlist_lookup.playlist_name.loc[self.playlist_lookup.playlist_uri == x[0]].iloc[0] for x in recommendations]# get title for each isrc
        desc  = [self.playlist_lookup.playlist_description.loc[self.playlist_lookup.playlist_uri == x[0]].iloc[0] for x in recommendations]
        seed_isrc = [x[2] for x in recommendations]
        #rec_readable_df = pd.DataFrame({'scores':scores,'isrc': isrcs, 'artist': artist, 'title': title,'seed_playlist_uri':seed_playlist_uri})  # Create a dataframe
        
        logging.info("Making final recommendations output took " + str(round(time.time() - readable_start, 4)) + " seconds.")

        return pd.DataFrame({'scores':scores,'playlists': playlists, 'owners': owner, 'names': name,'seed_isrc':seed_isrc})


class Recommender(Data_prep, Recommender_core):
    '''This class is a wrapper to prepare data, make training set, train the recommendation engine and then make recommendations'''
    def __init__(self, *initial_data, **kwargs):
        # Initialise parameters from a config dictionary
        for dictionary in initial_data:
            for key in dictionary:
                setattr(self, key, dictionary[key])
        for key in kwargs:
            setattr(self, key, kwargs[key])
        self.print_info = False

    def build_recommender(self,playlist_data_path):#,isrc_data_path,non_spotify_playlists_sample_path,spotify_playlists_sample_path):
        print('Loading and preparing data...')
        #self.load_data(playlist_data_path)#, isrc_data_path, non_spotify_playlists_sample_path,spotify_playlists_sample_path)
        self.prepare_data(playlist_data_path)
        #self.playlists_trainset, self.playlists_testset, self.playlists_isrcs_altered, self.holdout_set_lookup = self.make_trainset(self.playlists_sparse,pct_test=self.pct_test, rseed=self.rseed)
        print('Training recommender...')
        self.train(self.playlists_sparse, self.factors, self.regularization, self.iterations, self.alpha)

    def filter_unknown_playlists(self,playlist_uris_list):
        '''This function checks if the playlist_uris provided in "playlist_uris_list" are in our dataset.
        It returns a list of only the uris that are in our dataset. '''
        unique_playlists = set(self.playlists)
        lst3 = [value for value in playlist_uris_list if value in unique_playlists]
        return  lst3

    
    def rec_multiple_playlists(self,playlist_uris_list, playlist_isrc_matrix, territory, category, N=10):
        '''This function produces recommendations for a list of playlist URIs - this is typically designed to make recommendations to a browse category.
        Inputs:
            - playlist_uris_list: a list of playlist uris to make recommendations to (typically playlists in a browse category)
            - playlist_isrc_matrix: the matrix of isrc and playlists used to train the recommender
            - N: the number of tracks to recommend
        Output:
            - A list of list of tuple (playlist_index, score) of recommended tracks'''
        # Only consider playlists that are in our dataset
        playlists_in_the_model = self.filter_unknown_playlists(playlist_uris_list)
        playlists_not_in_the_model = [x for x in playlist_uris_list if x not in playlists_in_the_model]
        # For all uris, get corresponding indexes
        #playlist_idxs = [self.playlists.index(playlist_uri) for playlist_uri in playlist_uris_list]
        # Get recommendations for all playlists
        recommendations = []
        
        if len(playlists_in_the_model)>0:
            nrec=int(2*(np.ceil(N/len(playlists_in_the_model)))+1)
        else:
            print("No recommendations for territory-category ", territory, category)

        # in the model recommendations
        for playlist_uri in playlists_in_the_model:
            recommendations.extend(self.recommend(playlist_uri,playlist_isrc_matrix,N=nrec))
        # out of the model recommendations
        #for playlist_uri in playlists_not_in_the_model:
        #    recommendations.extend(self.recommend_outofmodel(playlist_uri,playlist_isrc_matrix,N=nrec))

        # get rid of duplicates
        ids = [x[0] for x in recommendations]
        ids_unique, unique_indices = np.unique(ids,return_index=True)
        recommendations = [(recommendations[idx][0],recommendations[idx][1],recommendations[idx][2], territory, category) for idx in unique_indices]
        # Re-rank recommendations
        rec_sorted = sorted(recommendations, key=operator.itemgetter(1), reverse=True)
        return rec_sorted[:N]

    #def make_savedir_path(self,savedir_path=None):
    #    '''If a savedir path is provided, do nothing. Else, make a savedir named after the model parameters'''
    #    if savedir_path==None:
    #        savedir_path = os.path.join(self.save_basedir,
    #                               'iter' + str(self.iterations) + '_alpha' + str(self.alpha) +
    #                               '_factors'+str(self.factors)+'_reg'+str(self.regularization).replace('.','')
    #                               +'_rseed'+str(self.rseed)+'_pctTest'+str(self.pct_test).replace('.',''))
    #        if not os.path.exists(savedir_path):
    #            os.makedirs(savedir_path)
    #        return savedir_path
    #    else:
    #        return savedir_path

    #def save_model(self, savedir, savebackup=True):
    #    print('Saving model...')
        # Save a backup of files used to build and train the recommender, for reproducibility, in case we need to retrain. Saved in a "backup" subfolder
    #    if savebackup:
    #        subdir_backup = os.path.join(savedir, 'backup')
    #        if not os.path.exists(subdir_backup):
    #            os.makedirs(subdir_backup)
    #        self.playlists_data.to_csv(os.path.join(subdir_backup, 'playlists_data.gzip'), compression='gzip')
    #        self.isrc_data.to_csv(os.path.join(subdir_backup, 'isrc_data.gzip'), compression='gzip')
    #        self.non_spotify_playlists_sample.to_csv(os.path.join(subdir_backup, 'non_spotify_playlists_sample.gzip'), compression='gzip')
    #        self.spotify_playlists_sample.to_csv(os.path.join(subdir_backup, 'spotify_playlists_sample.gzip'), compression='gzip')

        # Save trained model and all data necessary to make recommendations without having to re-train. Saved in a 'model' subfolder
        ## Make subdir if not exist
    #    subdir_model = os.path.join(savedir,'model')
    #    if not os.path.exists(subdir_model):
    #        os.makedirs(subdir_model)
        ## Save data
    #    recommender_params = {'pct_test': self.pct_test,
    #                          'alpha': self.alpha,
    #                          'factors': self.factors,
    #                          'regularization': self.regularization,
    #                          'iterations': self.iterations,
    #                          'rseed': self.rseed
    #                          }
    #    with open(os.path.join(subdir_model,'rec_params.json'), 'w') as f:
    #        json.dump(recommender_params,f)
    #    with open(os.path.join(subdir_model,'playlists_trainset.npz'), 'w') as f:
    #        sparse.save_npz(f, self.playlists_trainset, compressed=True)
    #    with open(os.path.join(subdir_model,'playlists_testset.npz'), 'w') as f:
    #        sparse.save_npz(f, self.playlists_testset, compressed=True)
    #    with open(os.path.join(subdir_model,'playlists_vecs'), 'w') as f:
    #        np.save(f, self.playlist_vecs, allow_pickle=False, fix_imports=False)
    #    with open(os.path.join(subdir_model,'isrc_vecs'), 'w') as f:
    #        np.save(f, self.isrc_vecs, allow_pickle=False, fix_imports=False)
    #    with open(os.path.join(subdir_model,'playlists'), 'w') as f:
    #        np.save(f,self.playlists, allow_pickle=False, fix_imports=False)
    #    with open(os.path.join(subdir_model,'playlists_isrc_altered'), 'w') as f:
    #        np.save(f,self.playlists_isrcs_altered, allow_pickle=False, fix_imports=False)
    #    with open(os.path.join(subdir_model,'isrcs'), 'w') as f:
    #        np.save(f, self.isrcs, allow_pickle=False, fix_imports=False)
    #    with open(os.path.join(subdir_model, 'model_implicit.p'), 'w') as f:
    #        pickle.dump(self.model, f)
    #    self.isrc_lookup.to_csv(os.path.join(subdir_model, 'isrc_lookup.csv'), sep='\t', header=True, index=False)
    #    self.holdout_set_lookup.to_csv(os.path.join(subdir_model, 'holdout_set_lookup.csv'), sep='\t', header=True, index=False)


    #def load_model(self, loaddir):
    #    print('Loading model...')
    #    subdir_model = os.path.join(loaddir, 'model')
    #    with open(os.path.join(subdir_model,'rec_params.json'), 'r') as f:
    #        recommender_params = json.load(f)
    #        for key in recommender_params:
    #            setattr(self, key, recommender_params[key])
    #    with open(os.path.join(subdir_model,'playlists_trainset.npz'), 'r') as f:
    #        self.playlists_trainset = sparse.load_npz(f)
    #    with open(os.path.join(subdir_model,'playlists_testset.npz'), 'r') as f:
    #        self.playlists_testset = sparse.load_npz(f)
    #    with open(os.path.join(subdir_model,'playlists_vecs'), 'r') as f:
    #        self.playlist_vecs = np.load(f) # Loads a binary file
    #    with open(os.path.join(subdir_model,'isrc_vecs'), 'r') as f:
    #        self.isrc_vecs = np.load(f) # Loads a binary file
    #    with open(os.path.join(subdir_model,'playlists'), 'r') as f:
    #        self.playlists = np.load(f).tolist()
    #    with open(os.path.join(subdir_model,'playlists_isrc_altered'), 'r') as f:
    #        self.playlists_isrcs_altered = np.load(f).tolist()
    #    with open(os.path.join(subdir_model,'isrcs'), 'r') as f:
    #        self.isrcs = np.load(f).tolist()
    #    with open(os.path.join(subdir_model, 'isrc_lookup.csv'), 'rb') as f:
    #        self.isrc_lookup = pd.read_csv(f, sep='\t', header=0)
    #    with open(os.path.join(subdir_model, 'holdout_set_lookup.csv'), 'rb') as f:
    #        self.holdout_set_lookup = pd.read_csv(f, sep='\t', header=0)
    #    with open(os.path.join(subdir_model, 'model_implicit.p'), 'r') as f:
    #        self.model = pickle.load(f)


    #def save_xls(self,list_dfs, xls_path):
    #    writer = pd.ExcelWriter(xls_path)
    #    for n, df in enumerate(list_dfs):
    #        df.to_excel(writer,'sheet%s' % n)
    #    writer.save()