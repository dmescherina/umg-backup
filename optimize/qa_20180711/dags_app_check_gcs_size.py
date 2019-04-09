#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 10 12:26:18 2018

@author: usvyat
"""


import boto
import sys
import gcs_oauth2_boto_plugin

def check_size_lzo(ds):

# URI scheme for Cloud Storage.

    CLIENT_ID = '719478121140-6o6uf33j6mfvf62fung43vr45pbkt13o.apps.googleusercontent.com'
    CLIENT_SECRET = 'bWgwLdsdjea4NXXq8s_IwcsS'

    GOOGLE_STORAGE = 'gs'

    dir_file= 'date_id={ds}/apollo_export_{ds}.lzo'.format(ds=ds)



    gcs_oauth2_boto_plugin.SetFallbackClientIdAndSecret(CLIENT_ID, CLIENT_SECRET)
    uri = boto.storage_uri('umg-comm-tech-dev/data/apollo/prod/'+ dir_file, GOOGLE_STORAGE)
    #print str(uri.get_acl())

    try:
        for entry in uri.get_bucket().get_acl().entries.entry_list:
            entry_id = entry.scope.id
            if not entry_id:
                entry_id = entry.scope.email_address
            print 'SCOPE: %s' % entry_id
            print 'PERMISSION: %s\n' % entry.permission
        key = uri.get_key()
        if key.size < 45379959:
            raise ValueError('umg lzo file is too small, investigate')
        else:
            print('for the date %s' % ds)
            print('umg lzo file for is %sMB' % round((key.size/1e6),2))

    except:
        print('Error with credentials')
        pass






if __name__ == "__main__":

     check_size_lzo(sys.argv[1])
