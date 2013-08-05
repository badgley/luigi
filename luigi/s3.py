# Copyright (c) 2013 Mortar Data
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import configuration
import itertools
import logging
import urlparse

from boto.s3.connection import S3Connection
from boto.s3.bucket import Bucket

from luigi.target import FileSystem, FileSystemTarget

# two different ways of marking a directory
# with a suffix in S3
S3_DIRECTORY_MARKER_SUFFIX_0 = '_$folder$'
S3_DIRECTORY_MARKER_SUFFIX_1 = '/'

logger = logging.getLogger('luigi-interface')

class S3Client(FileSystem):
    """
    boto-powered S3 client.
    """
        
    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None):
        if not aws_access_key_id:
            aws_access_key_id = configuration.get_config().get('s3', 'aws_access_key_id')
        if not aws_secret_access_key:
            aws_secret_access_key = configuration.get_config().get('s3', 'aws_secret_access_key')
        
        self.s3 = S3Connection(aws_access_key_id,
                               aws_secret_access_key,
                               is_secure=True)
        
    def exists(self, path):
        """
        """
        (bucket, key) = self.path_to_bucket_and_key(path)
        
        # grab and validate the bucket
        s3_bucket = self.s3.get_bucket(bucket, validate=True)
        
        # root always exists
        if self.is_root(key):
            logger.debug('Bucket root exists for path %s' % path)
            return True
        
        # file
        s3_key = s3_bucket.get_key(key)
        if s3_key:
            logger.debug('File exists for path %s' % path)
            return True
        
        # directory marker
        for suffix in (S3_DIRECTORY_MARKER_SUFFIX_0, S3_DIRECTORY_MARKER_SUFFIX_1):
            s3_dir_with_suffix_key = s3_bucket.get_key(key + suffix)            
            if s3_dir_with_suffix_key:
                logger.debug('Directory with suffix %s exists for path %s' % (suffix, path))
                return True
        
        # files with this prefix
        key_without_slash = self.remove_prepended_slash(key)
        s3_bucket_list_result = \
            list(itertools.islice(
                    s3_bucket.list(prefix=key_without_slash), 
                 1))
        if s3_bucket_list_result:
            logger.debug('Directory existence inferred; files exist under path prefix %s' % path)
            return True
        
        logger.debug('Path %s does not exist' % path)
        return False
    
    def remove(self, path):
        raise NotImplementedError('TODO: Implement me')
    
    def path_to_bucket_and_key(self, path):
         (scheme, netloc, path, query, fragment) = urlparse.urlsplit(path)
         return (netloc, 
                 path)
    
    def is_root(self, key):
        return (len(key) == 0) or (key == '/')
    
    def remove_prepended_slash(self, key):
        return key[1:] if key and key[0] == '/' else path

client = S3Client()
class S3Target(FileSystemTarget):
    
    fs = client  # underlying file system

    def __init__(self, path=None, format=None, is_tmp=False):
        if path is None:
            assert is_tmp
            path = tmppath()
        super(S3Target, self).__init__(path)
        self.format = format
        self.is_tmp = is_tmp
        (scheme, netloc, path, query, fragment) = urlparse.urlsplit(path)
        assert ":" not in path  # colon is not allowed in hdfs filenames

#    TODO: Implement me
#    def __del__(self):
#        #TODO: not sure is_tmp belongs in Targets construction arguments
#        if self.is_tmp and self.exists():
#            self.remove()
    
    def open(self, mode='r'):
        raise NotImplementedError('TODO: Implement me')
