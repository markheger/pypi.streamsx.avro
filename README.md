# Python streamsx.avro package

This exposes SPL operators in the `com.ibm.streamsx.avro` toolkit as Python methods.

Package is organized using standard packaging to upload to PyPi.

The package is uploaded to PyPi in the standard way:
```
cd package
python setup.py sdist bdist_wheel upload -r pypi
```
Note: This is done using the `ibmstreams` account at pypi.org and requires `.pypirc` file containing the credentials in your home directory.

Package details: https://pypi.python.org/pypi/streamsx.avro

Documentation is using Sphinx and can be built locally using:
```
cd package/docs
make html
```
and viewed using
```
firefox package/docs/build/html/index.html
```

The documentation is also setup at `readthedocs.io`.

Documentation links:
* http://streamsxavro.readthedocs.io

## Test

Package can be tested with TopologyTester using the [Streaming Analytics](https://www.ibm.com/cloud/streaming-analytics) service.

Run the test with:

    ant test-sas

or

```
cd package
python3 -u -m unittest streamsx.avro.tests.test_avro.TestStreamingAnalytics
```

#### Remote build

For using the toolkit from the build service (**force_remote_build**) run the test with:

Run the test with:

    ant test-sas-remote

or

```
cd package
python3 -u -m unittest streamsx.avro.tests.test_avro.TestStreamingAnalyticsRemote
```

### Local Streams instance

Package can be tested with TopologyTester using a local and running Streams domain.
Make sure that the streams environment is set, the domain and instance is running and the environment variables:
STREAMS_USERNAME
STREAMS_PASSWORD
are setup.

Run the test with:

    ant test

or

```
cd package
python3 -u -m unittest streamsx.avro.tests.test_avro.TestDistributed
```
