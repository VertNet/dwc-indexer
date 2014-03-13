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

import cloudstorage as gcs

from datetime import datetime

from google.appengine.api import files
from google.appengine.api import namespace_manager
from google.appengine.api import search
from google.appengine.api import taskqueue

from google.appengine.ext import ndb

from mapreduce import control
from mapreduce import input_readers

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
        response = search.get_indexes(fetch_schema=True)
        logging.info('RESPONSE %s' % response.results)
        if response.results:
            body = ''
            for index in response.results:
                body += 'Name: %s<br>' % index.name
                body += 'Namespace: %s<br>' % index.namespace
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
        input_class = (input_readers.__name__ + "." + input_readers.FileInputReader.__name__)
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
        index_name, namespace, id, resource = \
            map(self.request.get,
                ['index_name', 'namespace', 'id', 'resource'])
        index = search.Index(index_name, namespace=namespace)

        if id:
            docs = index.get_range(start_id=id, ids_only=True, limit=100,
                                   include_start_object=True).results
        else:
            docs = index.get_range(ids_only=True, limit=100).results
        if len(docs) < 1:
            return

        # Filter out doc_ids that don't contain resource:
        ids = [doc.doc_id for doc in docs if resource in doc.doc_id]

        if len(ids) < 1:  # Didn't find any matches in this batch.
            if len(docs) == 100:
                next_id = docs[-1].doc_id
            else:
                index.delete(id)
                return
        else:  # Matches found, delete them.
            blast, next_id = ids[:-1], ids[-1]
            index.delete(blast)
        params = dict(index_name=index_name, namespace=namespace, id=next_id,
                      resource=resource)
        if len(docs) >= 100:
            logging.info(params)
            logging.info('DOCS %s' % len(docs))
            logging.info('IDS %s' % len(ids))
            if blast and len(blast) == 1 and next_id == blast[0]:
                index.delete(next_id)
                return
            taskqueue.add(url='/index-delete-resource', params=params,
                          queue_name="index-delete-resource")


routes = [
    webapp2.Route(r'/list-indexes', handler='indexer.ListIndexes:get'),
    webapp2.Route(r'/bootstrap-gcs', handler='indexer.BootstrapGcs:get'),
    webapp2.Route(r'/index-gcs-path', handler='indexer.IndexGcsPath:get'),
    webapp2.Route(r'/index-gcs-path-finalize', handler='indexer.IndexGcsPath:finalize'),
    webapp2.Route(r'/index-delete-resource', handler='indexer.IndexDeleteResource:get'),
]

handler = webapp2.WSGIApplication(routes, debug=IS_DEV)
