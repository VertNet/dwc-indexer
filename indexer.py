#!/usr/bin/env python

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

__author__ = "John Wieczorek"
__contributors__ = "Aaron Steele, John Wieczorek"
__copyright__ = "Copyright 2016 vertnet.org"
__version__ = "indexer.py 2016-07-09T14:44+2:00"

# This module contains request handlers for admin APIs.
import logging
import os
import time
import webapp2
import index_utils
import cloudstorage as gcs
from datetime import datetime
from google.appengine.api import namespace_manager
from google.appengine.api import search
from google.appengine.api import taskqueue
from google.appengine.ext import ndb
from google.appengine.ext import db
from google.appengine.ext.db import metadata

# Note: This code uses the Mapreduce library 
# (from https://github.com/GoogleCloudPlatform/appengine-mapreduce) modified to 
# include the a custom GoogleCloudStorageLineInputReader. Be aware of this before 
# updating MapReduce.
from mapreduce import input_readers
from mapreduce import control

IS_DEV = os.environ.get('SERVER_SOFTWARE', '').startswith('Dev')
DEV_BUCKET = '/vn-harvest'

my_default_retry_params = gcs.RetryParams(initial_delay=0.2,
                                          max_delay=5.0,
                                          backoff_factor=2,
                                          max_retry_period=4)

gcs.set_default_retry_params(my_default_retry_params)

class IndexJob(ndb.Model):
    write_path = ndb.TextProperty()
    failed_logs = ndb.StringProperty(repeated=True)
    resource = ndb.StringProperty()
    bucket_name = ndb.StringProperty()
    files_list = ndb.StringProperty()
    namespace = ndb.StringProperty()
    done = ndb.BooleanProperty(default=False)
    failures = ndb.ComputedProperty(lambda self: len(self.failed_logs) > 1)


class ListIndexes(webapp2.RequestHandler):
    def get(self):
        """List all indexes for all namespaces."""
        # Doesn't show indexes created by indexer in other namespaces.
        # For that, you have to pass the namespace parameter.
        ns = map(self.request.get,['namespace'])[0]

        response = search.get_indexes(namespace=ns, fetch_schema=True)
        logging.info('RESPONSE %s' % response.results)
        if response.results:
            body = ''
            for index in response.results:
                body += 'Name: %s<br>' % index.name
                body += 'Namespace: %s<br>' % index.namespace
                body += 'Storage usage: %s<br>' % index.storage_usage
                body += 'Storage limit: %s<br>' % index.storage_limit
                body += 'Schema: %s<p>' % index.schema
        else:
            body = 'No indexes found.'
        self.response.out.write(body)

class BootstrapGcs(webapp2.RequestHandler):
    def get(self):  
        """Bootstraps local GCS with test data dwc files in dwc."""
        if not IS_DEV:
            self.response.out.write('Bootstrapping GCS is for the dev_server only.')
            return      
        write_retry_params = gcs.RetryParams(backoff_factor=1.1)
        for filename in os.listdir('dwc'):
            if filename == '.DS_Store':
                continue
            path = os.path.abspath('dwc/%s' % filename)
            gcs_path = '%s/%s' % (DEV_BUCKET, filename)
            data = open(path, 'r').read()
            gcs_file = gcs.open(
                gcs_path,
                'w',
                content_type='text/tab-separated-values',
                options={})
            gcs_file.write(data)
            gcs_file.close()
        time.sleep(5)
        self.redirect('http://localhost:8000/blobstore')


# Version using GoogleCloudStorageLineInputReader
class IndexGcsPath(webapp2.RequestHandler):
    """Index the files in a list in a GCS bucket.
       Example:
       http://indexer.vertnet-portal.appspot.com/index-gcs-path?namespace=index-2014-02-06t2&index_name=dwc&bucket_name=vertnet-harvesting&files_list=data/2015-05-29/uwymv_herp-9f48b5c4-1e8f-42e9-89d5-abcddffae55f/*&shard_count=1
       
       http://indexer.vertnet-portal.appspot.com/index-gcs-path
       ?namespace=index-2014-02-06t2
       &index_name=dwc
       &bucket_name=vertnet-harvesting
       &files_list=data/2015-05-29/uwymv_herp-9f48b5c4-1e8f-42e9-89d5-abcddffae55f/*
       &shard_count=1
       """
    def get(self):
        """Fires off an indexing MR job over files in GCS at supplied path."""
        # Note: To make this work, the Mapreduce library had to be modified to include the 
        # custom GoogleCloudStorageLineInputReader. Be aware of this if updating MapReduce.
        global __version__
        input_class = (input_readers.__name__ + "." +
                    input_readers.GoogleCloudStorageLineInputReader.__name__)
        shard_count = self.request.get_range('shard_count', default=8)
        processing_rate = self.request.get_range('processing_rate', default=100)
        now = datetime.now().isoformat().replace(':', '-')
        namespace = self.request.get('namespace', 'ns-' + now)
        index_name = self.request.get('index_name', 'dwc-' + now)
        bucket_name = self.request.get('bucket_name')
        files_list = self.request.get('files_list')

        mrid = control.start_map(
            files_list,
            "index_utils.build_search_index",
            input_class,
            {
                "input_reader": {
                    "bucket_name": bucket_name,
                    "objects": [files_list]
                },
                "bucket_name": bucket_name,
                "files_list": files_list,
                "namespace": namespace,
                "index_name": index_name,
                "processing_rate": processing_rate,
                "shard_count": shard_count
            },
            mapreduce_parameters={'done_callback': '/index-gcs-path-finalize'},
            shard_count=shard_count)

        body = 'Indexing resource: %s<br>' % files_list
        body += 'Version: %s<br>' % __version__
        body += 'Namespace: %s<br>' % namespace
        body += 'Index_name: %s<br>' % index_name
        body += 'Processing rate: %s<br>' % processing_rate
        body += 'Shard count: %s<br>' % shard_count
        body += 'mrid: %s<br>' % mrid
        logging.warning(body)
        self.response.out.write(body)

        IndexJob(id=mrid, write_path='write_path', bucket_name=bucket_name, 
                files_list=files_list, failed_logs=['NONE'], namespace=namespace).put()

    def finalize(self):
        """Finalizes indexing MR job by finalizing files on GCS."""
        mrid = self.request.headers.get('Mapreduce-Id')
        job = IndexJob.get_by_id(mrid)
        logging.info('FINALIZING IndexGcsPath (Cloud Storage Library) JOB %s' % job)
        if not job:
            return
        logging.info('Finalizing index job for resource %s' % job.resource)
        job.done = True
        job.put()
        logging.info('Index job finalized for resource %s' % job.resource)

class IndexDeleteResource(webapp2.RequestHandler):
    """Remove the records from an index for a gbifdatasetid.
       Example:
       http://indexer.vertnet-portal.appspot.com/index-delete-dataset?gbifdatasetid=b11cbb9e-8ee0-4d9a-8eac-da5d5ab53a31&index_name=dwc&namespace=index-2014-02-06t2

       http://indexer.vertnet-portal.appspot.com/index-delete-dataset
       ?gbifdatasetid=b11cbb9e-8ee0-4d9a-8eac-da5d5ab53a31
       &index_name=dwc
       &namespace=index-2014-02-06t2
       """
    def get(self):
        """Deletes documents matching resource, and, optionally icode and class."""
        index_name, namespace, resource, batch_size, ndeleted, \
            max_delete, dryrun, icode, classs = \
            map(self.request.get,
                ['index_name', 'namespace', 'resource', 'batch_size', 'ndeleted', 
                 'max_delete', 'dryrun', 'icode', 'classs'])
        
        body = 'Deleting resource:<br>'
        body += 'Namespace: %s<br>' % namespace
        body += 'Index_name: %s<br>' % index_name
        body += 'Resource: %s<br>' % resource
        body += 'InstitutionCode: %s<br>' % icode
        body += 'Class: %s<br>' % classs
        body += 'Batch size: %s<br>' % batch_size
        body += 'Max delete: %s<br>' % max_delete
        body += 'Dry run: %s<br>' % dryrun
        logging.warning(body)
        self.response.out.write(body)

        if dryrun:
            logging.info('\n==IndexDeleteResource(%s, %s, %s, %s, %s, %s, %s, %s, %s)==' % 
                        (resource, namespace, index_name, batch_size, ndeleted, 
                         max_delete, dryrun, icode, classs) )

        deleted_so_far=0
        if ndeleted is not None and ndeleted != '':
            deleted_so_far = int(ndeleted)
        bsize=200
        if batch_size is not None and batch_size != '':
            bsize = int(batch_size)
        if max_delete is None or max_delete == '':
            maxdel = deleted_so_far + bsize + 1
        else:
            maxdel = int(max_delete)
        
        # Set the batch size to not exceed the maximum number of docs to delete.
        if deleted_so_far + bsize > maxdel:
            bsize = maxdel-deleted_so_far
            
        querystr = 'resource:%s' % resource
        if icode is not None and len(icode)>0:
            querystr += ' institutioncode:%s' % icode
        if classs is not None and len(classs)>0:
            querystr += ' class:%s' % classs

        logging.info('Query: %s namespace: %s index: %s' % (querystr, namespace, index_name) )

        # Set task queue characteristics in queue.yaml to retry tasks that fail.
        # Define the query by using a Query object.
        query = search.Query(querystr, options=search.QueryOptions(limit=bsize, ids_only=True) )
        index = search.Index(index_name, namespace=namespace)
        docs = index.search(query)
        ids = [doc.doc_id for doc in docs]

        if len(ids) < 1:
            logging.info('No documents for resource=%s left to delete in %s.%s.' % 
                        (resource, namespace, index_name) )
            return

        logging.info('Deleting %s documents.\nFirst: %s\nLast:  %s' % 
                    ( len(ids), ids[0], ids[-1] ) )
        index.delete(ids)
        deleted_so_far = deleted_so_far + len(ids)
        
        params = dict(index_name=index_name, namespace=namespace, 
                      batch_size=batch_size, max_delete=max_delete, 
                      ndeleted=deleted_so_far, resource=resource, icode=icode, classs=classs)

        if dryrun:
            params['dryrun'] = 1

        if deleted_so_far < maxdel and len(ids)==bsize:
            body = 'Queuing task index-delete-resource with params %s<br>' % params
            logging.info(body)
            self.response.out.write(body)
            taskqueue.add(url='/index-delete-resource', params=params, 
                          queue_name="index-delete-resource")
        else:
            body = 'Finished index-delete-resource:<br>'
            body += 'Namespace: %s<br>' % namespace
            body += 'Index_name: %s<br>' % index_name
            body += 'Documents removed: %s<p>' % deleted_so_far
            logging.info(body)
            self.response.out.write(body)
                     
class IndexDeleteDataSet(webapp2.RequestHandler):
    def get(self):
        """Deletes documents matching gbifdatasetid, and, optionally icode and class."""
        index_name, namespace, gbifdatasetid, batch_size, ndeleted, \
            max_delete, dryrun, icode, classs = \
            map(self.request.get,
                ['index_name', 'namespace', 'gbifdatasetid', 'batch_size', 'ndeleted', 
                 'max_delete', 'dryrun', 'icode', 'classs'])
        
        body = 'Deleting resource:<br>'
        body += 'Namespace: %s<br>' % namespace
        body += 'Index_name: %s<br>' % index_name
        body += 'GBIFdatasetid: %s<br>' % gbifdatasetid
        body += 'InstitutionCode: %s<br>' % icode
        body += 'Class: %s<br>' % classs
        body += 'Batch size: %s<br>' % batch_size
        body += 'Max delete: %s<br>' % max_delete
        body += 'Dry run: %s<br>' % dryrun
        logging.info(body)
        self.response.out.write(body)

        if dryrun:
            logging.info('\n==IndexDeleteDataSet(%s, %s, %s, %s, %s, %s, %s, %s, %s)==' % 
                        (gbifdatasetid, namespace, index_name, batch_size, ndeleted, 
                         max_delete, dryrun, icode, classs) )

        deleted_so_far=0
        if ndeleted is not None and ndeleted != '':
            deleted_so_far = int(ndeleted)
        bsize=200
        if batch_size is not None and batch_size != '':
            bsize = int(batch_size)
        if max_delete is None or max_delete == '':
            maxdel = deleted_so_far + bsize + 1
        else:
            maxdel = int(max_delete)
        
        # Set the batch size to not exceed the maximum number of docs to delete.
        if deleted_so_far + bsize > maxdel:
            bsize = maxdel-deleted_so_far
            
        querystr = 'gbifdatasetid:%s' % gbifdatasetid
        if icode is not None and len(icode)>0:
            querystr += ' institutioncode:%s' % icode
        if classs is not None and len(classs)>0:
            querystr += ' class:%s' % classs

        logging.info('Query: %s namespace: %s index: %s' % (querystr, namespace, index_name) )

        # Set task queue characteristics in queue.yaml to retry tasks that fail.
        # Define the query by using a Query object.
        query = search.Query(querystr, options=search.QueryOptions(limit=bsize, ids_only=True) )
        index = search.Index(index_name, namespace=namespace)
        docs = index.search(query)
        ids = [doc.doc_id for doc in docs]

        if len(ids) < 1:
            logging.info('No documents for gbifdatasetid=%s left to delete in %s.%s.' % 
                        (gbifdatasetid, namespace, index_name) )
            return

        logging.info('Deleting %s documents.\nFirst: %s\nLast:  %s' % 
                    ( len(ids), ids[0], ids[-1] ) )
        index.delete(ids)
        deleted_so_far = deleted_so_far + len(ids)
        
        params = dict(index_name=index_name, namespace=namespace, 
                      batch_size=batch_size, max_delete=max_delete, 
                      ndeleted=deleted_so_far, gbifdatasetid=gbifdatasetid, icode=icode, classs=classs)

        if dryrun:
            params['dryrun'] = 1

        if deleted_so_far < maxdel and len(ids)==bsize:
            body = 'Queuing task index-delete-dataset with params %s<br>' % params
            logging.info(body)
            self.response.out.write(body)
            taskqueue.add(url='/index-delete-dataset', params=params, 
                          queue_name="index-delete-dataset")
        else:
            body = 'Finished index-delete-dataset:<br>'
            body += 'Namespace: %s<br>' % namespace
            body += 'Index_name: %s<br>' % index_name
            body += 'Documents removed: %s<p>' % deleted_so_far
            logging.info(body)
            self.response.out.write(body)
                     
class IndexClean(webapp2.RequestHandler):
    def get(self):
        """Removes up to max_delete documents from an index in batches of batch_size."""
        index_name, namespace, id, batch_size, ndeleted, max_delete, dryrun = \
            map(self.request.get,
                ['index_name', 'namespace', 'id', 'batch_size', 'ndeleted', 'max_delete', 
                 'dryrun'])
        index = search.Index(index_name, namespace=namespace)

        deleted_so_far=0
        if ndeleted is not None and ndeleted != '':
            deleted_so_far = int(ndeleted)
        bsize=200
        if batch_size is not None and batch_size != '':
            bsize = int(batch_size)
        if max_delete is None or max_delete == '':
            maxdel = deleted_so_far + bsize + 1
        else:
            maxdel = int(max_delete)
        
        to_delete = bsize
        if maxdel < deleted_so_far + bsize:
            to_delete = maxdel - deleted_so_far

        if dryrun:
          logging.info('\n==IndexClean(id=%s, %s, %s, %s, %s, %s, %s)==' 
                % (id, namespace, index_name, batch_size, ndeleted, max_delete, dryrun))

        if id:
            docs = index.get_range(start_id=id, ids_only=True, limit=to_delete+1,
                                   include_start_object=True).results
            if dryrun:
                logging.info('index.get_range(start_id=%s, ids_only=True, limit=%s, include_start_object=True)' 
                          % (id, to_delete+1) )
        else:
            docs = index.get_range(ids_only=True, limit=to_delete+1).results
            if dryrun:
                logging.info('index.get_range(ids_only=True, limit=%s)' % (to_delete+1) )

        if len(docs) < 1:
            if dryrun:
                logging.info('No documents left to index.' )
            return

        if dryrun:
            logging.info('Got %s documents.' % len(docs) )

        ids = [doc.doc_id for doc in docs]
        next_id = ids[-1]
        delete_these = []
        if len(ids) <= bsize :
            delete_these = ids
        else:
            delete_these = ids[:-1]

        body = 'Cleaning index. Deleting documents:<br>'
        body += 'Namespace: %s<br>' % namespace
        body += 'Index_name: %s<br>' % index_name
        body += 'Batch size: %s<br>' % bsize
        body += 'Maximum documents to delete: %s<br>' % maxdel
        body += 'Dryrun: %s' % dryrun
        self.response.out.write(body)
        logging.info(body)
        # Delete all but the last document, which is the key for where to start next.
        index.delete(delete_these)
   
        deleted_so_far = deleted_so_far + len(delete_these)
        
        params = dict(index_name=index_name, namespace=namespace, batch_size=batch_size, 
                    max_delete=max_delete, ndeleted=deleted_so_far, id=next_id)
        if dryrun:
            params['dryrun'] = 1

        if deleted_so_far < maxdel and len(delete_these)==bsize:
            logging.info('Queuing index-clean task with params %s' % (params) )
            taskqueue.add(url='/index-clean', params=params, queue_name="index-clean")
        else:
            logging.info('Finished index-clean on index %s.%s. Removed %s documents.' % 
                        (namespace, index_name, deleted_so_far) )
                     
class IndexFindRecord(webapp2.RequestHandler):
    def get(self):
        """Searches for a document with matching id."""
        index_name, namespace, id = \
            map(self.request.get,
                ['index_name', 'namespace', 'id'])
        index = search.Index(index_name, namespace=namespace)

        if id:
            doc = index.get(id)
            if doc:
                body = 'Found %s:<br>' % id
                body += 'Namespace: %s<br>' % namespace
                body += 'Index_name: %s<br>' % index_name
                body += 'Document: %s<br>' % doc
                logging.info(body)
                self.response.out.write(body)
            else:
                requestargs = self.request.arguments()
                requestargs.remove('index_name')
                requestargs.remove('namespace')
                requestargs.remove('id')
                missingid=requestargs[0]
                if missingid is not None:
                    id = '%s&%s' % (id,missingid)
                    doc = index.get(id)
                    body = 'Found %s:<br>' % id
                    body += 'Namespace: %s<br>' % namespace
                    body += 'Index_name: %s<br>' % index_name
                    body += 'Document id: %s<br>' % id
                    logging.info(body)
                    self.response.out.write(body)
                else:
                    body = 'Document not found: %s<br>' % id
                    body += 'Namespace: %s<br>' % namespace
                    body += 'Index_name: %s<br>' % index_name
                    body += 'Arguments: %s<br>' % self.request.arguments()
                    logging.info(body)
                    self.response.out.write(body)

class IndexDeleteRecord(webapp2.RequestHandler):
    def get(self):
        """Deletes a document with matching id."""
        index_name, namespace, id = \
            map(self.request.get,
                ['index_name', 'namespace', 'id'])
        index = search.Index(index_name, namespace=namespace)

        if id:
            doc = index.get(id)
            if doc:
                body = 'Deleting document:<br>'
                body += 'Namespace: %s<br>' % namespace
                body += 'Index_name: %s<br>' % index_name
                body += 'Document id: %s<br>' % id
                logging.info(body)
                self.response.out.write(body)
                index.delete(id)
            else:
                requestargs = self.request.arguments()
                requestargs.remove('index_name')
                requestargs.remove('namespace')
                requestargs.remove('id')
                missingid=requestargs[0]
                if missingid is not None:
                    id = '%s&%s' % (id,missingid)
                    doc = index.get(id)
                    body = 'Deleting document with & in it:<br>'
                    body += 'Namespace: %s<br>' % namespace
                    body += 'Index_name: %s<br>' % index_name
                    body += 'Document id: %s<br>' % id
                    body += 'Missing id part: %s<br>' % missingid
                    logging.info(body)
                    self.response.out.write(body)
                    index.delete(id)
                else:
                    body = 'Document not found:<br>'
                    body += 'Namespace: %s<br>' % namespace
                    body += 'Index_name: %s<br>' % index_name
                    body += 'Document id: %s<br>' % id
                    body += 'Arguments: %s<br>' % self.request.arguments()
                    logging.info(body)
                    self.response.out.write(body)

routes = [
    webapp2.Route(r'/list-indexes', handler='indexer.ListIndexes:get'),
    webapp2.Route(r'/bootstrap-gcs', handler='indexer.BootstrapGcs:get'),
    webapp2.Route(r'/index-gcs-path', handler='indexer.IndexGcsPath:get'),
    webapp2.Route(r'/index-gcs-path-finalize', handler='indexer.IndexGcsPath:finalize'),
    webapp2.Route(r'/index-delete-dataset', handler='indexer.IndexDeleteDataSet:get'),
    webapp2.Route(r'/index-delete-resource', handler='indexer.IndexDeleteResource:get'),
    webapp2.Route(r'/index-find-record', handler='indexer.IndexFindRecord:get'),

# index-clean is dangerous. Re-implement if really needed at some point.
#    webapp2.Route(r'/index-delete-record', handler='indexer.IndexDeleteRecord:get'),
    webapp2.Route(r'/index-clean', handler='indexer.IndexClean:get'),

    webapp2.Route(r'/index-delete-record', handler='indexer.IndexDeleteRecord:get'),]


handler = webapp2.WSGIApplication(routes, debug=IS_DEV)
