# This file is part of VertNet: https://github.com/VertNet/webapp
#
# VertNet is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# VertNet is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Foobar.  If not, see: http://www.gnu.org/licenses

# This module contains request handlers for admin APIs.
import logging
import os
import time
import webapp2

import utils
import json

import cloudstorage as gcs

from datetime import datetime

from google.appengine.api import files
from google.appengine.api import namespace_manager
from google.appengine.api import search
from google.appengine.api import taskqueue

from google.appengine.ext import ndb

from mapreduce import control
from mapreduce import input_readers

from google.appengine.ext import db
from google.appengine.ext.db import metadata

IS_DEV = os.environ.get('SERVER_SOFTWARE', '').startswith('Dev')
DEV_BUCKET = '/vn-harvest'

my_default_retry_params = gcs.RetryParams(initial_delay=0.2,
                                          max_delay=5.0,
                                          backoff_factor=2,
                                          max_retry_period=15)

gcs.set_default_retry_params(my_default_retry_params)


class IndexJob(ndb.Model):
    write_path = ndb.TextProperty()
    failed_logs = ndb.StringProperty(repeated=True)
    resource = ndb.StringProperty()
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


class IndexGcsPath(webapp2.RequestHandler):
    def get(self):
        """Fires off an indexing MR job over files in GCS at supplied path."""
        input_class = (input_readers.__name__ + "." + 
                       input_readers.FileInputReader.__name__)
        gcs_path = self.request.get('gcs_path')
        shard_count = self.request.get_range('shard_count', default=8)
        processing_rate = self.request.get_range('processing_rate', default=100)
        now = datetime.now().isoformat().replace(':', '-')
        namespace = self.request.get('namespace', 'ns-' + now)
        index_name = self.request.get('index_name', 'dwc-' + now)
        files_pattern = '/gs/%s' % gcs_path

        # TODO: Create file on GCS to log any failed index puts:

        mrid = control.start_map(
            gcs_path,
            "utils.build_search_index",
            input_class,
            {
                "input_reader": dict(
                    files=[files_pattern],
                    format='lines'),
                "resource": gcs_path,
                "namespace": namespace,
                "index_name": index_name,
                "processing_rate": processing_rate,
                "shard_count": shard_count
            },
            mapreduce_parameters={'done_callback': '/index-gcs-path-finalize'},
            shard_count=shard_count)

        body = 'Indexing resource:<br>'
        body += 'Namespace: %s<br>' % namespace
        body += 'Index_name: %s<br>' % index_name
        body += 'Resource: %s<br>' % gcs_path
        body += 'Processing rate: %s<br>' % processing_rate
        body += 'Shard count: %s<br>' % shard_count
        logging.info(body)
        self.response.out.write(body)

        IndexJob(id=mrid, resource=gcs_path, write_path='write_path', 
            failed_logs=['NONE'], namespace=namespace).put()

    def finalize(self):
        """Finalizes indexing MR job by finalizing files on GCS."""
        mrid = self.request.headers.get('Mapreduce-Id')
        job = IndexJob.get_by_id(mrid)
        logging.info('FINALIZING JOB %s' % job)
        if not job:
            return
        logging.info('Finalizing index job for resource %s' % job.resource)
        # files.finalize(job.write_path)
        job.done = True
        job.put()
        logging.info('Index job finalized for resource %s' % job.resource)

                     
class IndexDeleteResource(webapp2.RequestHandler):
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
        logging.info(body)
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
    webapp2.Route(r'/index-delete-resource', handler='indexer.IndexDeleteResource:get'),
    webapp2.Route(r'/index-find-record', handler='indexer.IndexFindRecord:get'),

# index-clean is dangerous. Re-implement if really needed at some point.
#    webapp2.Route(r'/index-delete-record', handler='indexer.IndexDeleteRecord:get'),
#    webapp2.Route(r'/index-clean', handler='indexer.IndexClean:get'),

    webapp2.Route(r'/index-delete-record', handler='indexer.IndexDeleteRecord:get'),]


handler = webapp2.WSGIApplication(routes, debug=IS_DEV)
