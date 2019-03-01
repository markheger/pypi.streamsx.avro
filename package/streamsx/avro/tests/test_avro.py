from unittest import TestCase

import streamsx.avro as avro

from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester
from streamsx.topology.schema import CommonSchema, StreamSchema
import streamsx.spl.op as op
import streamsx.spl.toolkit
import streamsx.rest as sr
import datetime
import os
import json



def avro_test_schema_file():
    script_dir = os.path.dirname(os.path.realpath(__file__))
    return script_dir+'/test.avsc'

class TestParams(TestCase):

    def test_to_avro_params(self):
        topo = Topology()
        s = topo.source(JsonData('a', 1)).as_json()
        avro.json_to_avro(s, avro_test_schema_file(), embed_avro_schema=True, tuples_per_message=1000)
        avro.json_to_avro(s, avro_test_schema_file(), embed_avro_schema=True, bytes_per_message=1024)
        avro.json_to_avro(s, avro_test_schema_file(), embed_avro_schema=True, time_per_message=datetime.timedelta(seconds=5))
        avro.json_to_avro(s, avro_test_schema_file(), embed_avro_schema=True, time_per_message=5)
        avro.json_to_avro(s, avro_test_schema_file(), embed_avro_schema=True, time_per_message=15.0)


class TestAvro(TestCase):

    def test_hw_json(self):
        topo = Topology('test_hw_json')
        streamsx.spl.toolkit.add_toolkit(topo, self.avro_toolkit_home)

        avro_schema = '{"type" : "record", "name" : "hw_schema", "fields" : [{"name" : "a", "type" : "string"}]}'
        s = topo.source([{'a': 'Hello'}, {'a': 'World'}, {'a': '!'}]).as_json()
        
        # convert json to avro blob
        o = avro.json_to_avro(s, avro_schema)
        # convert avro blob to json
        res = avro.avro_to_json(o, avro_schema)
        res.print()

        tester = Tester(topo)
        tester.tuple_count(res, 3)
        #tester.run_for(60)
   
        # Run the test
        tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True)

    def test_hw_embed_schema(self):
        topo = Topology('test_hw_embed_schema')
        streamsx.spl.toolkit.add_toolkit(topo, self.avro_toolkit_home)
 
        avro_schema = '{"type" : "record", "name" : "hw_schema", "fields" : [{"name" : "a", "type" : "string"}]}'
        s = topo.source([{'a': 'Hello'}, {'a': 'World'}, {'a': '!'},{'a': 'Hello'}, {'a': 'World'}, {'a': '!'}]).as_json()
        
        # convert json to avro blob
        o = avro.json_to_avro(s, avro_schema, embed_avro_schema=True, tuples_per_message=3)
        # convert avro blob to json
        res = avro.avro_to_json(o)
        res.print()

        tester = Tester(topo)
        tester.tuple_count(res, 6)
        #tester.run_for(60)
   
        # Run the test
        tester.test(self.test_ctxtype, self.test_config, always_collect_logs=True)

class TestDistributed(TestAvro):
    def setUp(self):
        Tester.setup_distributed(self)
        self.avro_toolkit_home = os.environ["AVRO_TOOLKIT_HOME"]
        # setup test config
        self.test_config = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(self.test_config)
        self.test_config[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False  

class TestStreamingAnalytics(TestAvro):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=False)
        self.avro_toolkit_home = os.environ["AVRO_TOOLKIT_HOME"]

    @classmethod
    def setUpClass(self):
        # start streams service
        connection = sr.StreamingAnalyticsConnection()
        service = connection.get_streaming_analytics()
        result = service.start_instance()

class TestStreamingAnalyticsRemote(TestStreamingAnalytics):
    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
        self.avro_toolkit_home = os.environ["AVRO_TOOLKIT_HOME"]

    @classmethod
    def setUpClass(self):
        super().setUpClass()
