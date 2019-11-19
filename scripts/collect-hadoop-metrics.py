from __future__ import absolute_import, print_function

import json
import sys

import boto3
import graphitesend
import requests
import six
import yaml
from yarn_api_client import HistoryServer

from edx.analytics.tasks.util.retry import retry


def query_metadata_service(path=''):
    """
    Gets the value at the specified path by querying the instance metadata service.

    Returns the textual response and does not attempt to deserialize the response body.  The instance metadata service[1]
    provides basic information about the given EC2 instance the endpoint is called from, providing things like instance ID,
    security group IDs, availability zone, block device mapping, and more.

    [1] - http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html
    """
    try:
        r = requests.get('http://169.254.169.254/latest/meta-data/' + path)
        return r.text
    except Exception as e:
        print("[collect-hadoop-metrics] exception when querying instance metadata service for path '{}': {}".format(path, e))
        return None


def get_cluster_region():
    """
    Gets the AWS region this cluster is running in.
    """
    node_az = query_metadata_service('placement/availability-zone')
    # node_az is the full AZ identifier i.e. us-east-1d so we want to take everything
    # but the last character, which gives us the region.  Historically, we have no reason
    # to believe that AZs will start to look like us-east-1tp or anything, so I think
    # this is a reasonably safe bet.
    if node_az is not None:
        return node_az[:-1]

    return None


def get_local_address_on_emr():
    """
    Gets the local/private IPv4 address of this instance, assuming we're on AWS.
    """
    return query_metadata_service('local-ipv4')


def get_instance_id():
    """
    Gets the instance ID of this instance.
    """
    return query_metadata_service('instance-id')


def get_job_flow_id():
    """
    Gets the job flow ID that this instance belongs to.

    We utilize the job flow information[1] put on disk when running an EMR cluster

    [1] - http://docs.aws.amazon.com/emr/latest/DeveloperGuide/Config_JSON.html
    """
    job_flow_id = None
    try:
        with open('/mnt/var/lib/info/job-flow.json', 'r') as job_flow_file:
            parsed = json.loads(job_flow_file.read())
            job_flow_id = parsed.get('jobFlowId', None)
    except:
        print("[collect-hadoop-metrics] Error reading job flow information.  Not on AWS/EMR?")

    return job_flow_id


def get_cluster_name():
    """
    Gets the name of the EMR cluster.
    """
    cluster_id = get_job_flow_id()
    if cluster_id is not None:
        try:
            emr = boto3.client('emr', region_name=get_cluster_region())
            cluster = emr.describe_cluster(ClusterId=cluster_id)

            return cluster['Cluster']['Name']
        except:
            print("[collect-hadoop-metrics] Error querying EMR API for cluster name")
            return None

    return None


def get_context():
    """
    Gets contextual information about where the script is being run.

    This includes things like the instance ID, and in the cases of special environments like EMR, the job flow ID.
    """
    return {
        'instance_id': get_instance_id(),
        'job_flow_id': get_job_flow_id(),
        'cluster_name': get_cluster_name(),
    }


def get_job_name_from_luigi_task_id(task_id):
    """
    Gets the name of the job based on the task ID that Luigi uses.

    This is usually in the form of TaskClassName(param=value, param=value, ...) and so we take the simple approach
    of chopping off whatever is in front of the parameters tuple.
    """
    return task_id.split('(')[0]


def get_prefix_from_counter_group_name(group_name):
    """
    Gets the simplified version of a counter group name, suitable for use with Graphite.

    Normally, Hadoop counter groups have names which correspond to the fully qualified Java class name
    which emitted the counters e.g. org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter.

    We're simply grabbing the last component in the full string, and then running it through a mapping
    to generate a simpler version.
    """
    group_name = group_name.split('.')[-1]

    class_name_to_simple_map = {
        'FileSystemCounter': 'fs',
        'JobCounter': 'job',
        'TaskCounter': 'task',
        'Shuffle Errors': 'shuffle',
        'FileInputFormatCounter': 'file_input',
        'FileOutputFormatCounter': 'file_output'
    }

    group_name = class_name_to_simple_map.get(group_name, group_name)

    # Last ditch effort to handle cases not covered here and attempt to pass a viable metric name back.
    group_name = group_name.replace(' ', '_').lower()

    return 'hadoop.counters.' + group_name


def facet_metrics(metrics, templates, context={}):
    """
    Facets the given metrics by creating copies based on the supplied templates.

    This function takes a dictionary of metric name -> metric value mappings, and for each of those metrics,
    will generate a new metric name (with the same value) by iterating through the list of templates and using
    both the original metric name and the context dictionary (tags, mostly, like instance ID of job flow ID)
    to format a new metric name.

    Thus, with a metric of "my.lil.metric", with the tag "foobar" having a value of "quux", and a template of
    "{foobar}.{metric_name}", we'd get "quux.my.lil.metric".

    Important note: if a tag/piece of context is null, but part of a template, it will get dropped.  That is to say,
    for someone running this not on EMR, we'd be missing the cluster name, job flow ID, etc.  When the metric is
    formatted, these would initially be 'None.' for each of those spots, but we remove those entries. If our template
    was:

        edx.analytics.emr.{cluster}.{flow}.hadoop.counters.foo.bar

    it would end up as:

        edx.analytics.emr.hadoop.counters.foo.bar

    instead.
    """
    output_metrics = []

    for metric, data in six.iteritems(metrics):
        # Merge our context with any tags for the metric.
        merged_context = context.copy()
        merged_context.update(data.get('context', {}))
        merged_context['metric'] = metric

        # For each "template", feed it in the context so it can be rendered, giving us our transformed metric name.
        data_value = data.get('value', 0)
        for template in templates:
            faceted_metric = template.format(**merged_context)
            faceted_metric = faceted_metric.replace('None.', '')
            output_metrics.append((faceted_metric, data_value))

    return output_metrics


@retry(timeout=300)
def collect_metrics(hs_address, metric_templates):
    """
    Collects Hadoop counters from all jobs on the local HistoryServer, transforming them for forwarding
    to Graphite, along with any configured faceting.

    This method will automatically retry (retry decorator), spending up to 300 seconds retrying.  This doesn't technically
    account for time spent sleeping which goes over the timeout, but is a close approximation.
    """
    # Load up the context for where we're running.
    context = get_context()

    print("[collect-hadoop-metrics] Context for this run:")
    for (k,v) in six.iteritems(context):
        print("                 {} => {}".format(k, v))

    formatted_metrics = {}

    # Grab all jobs from the history server.
    hs = HistoryServer(hs_address)
    response = hs.jobs()

    jobs = response.data.get('jobs', {})
    if jobs is None:
        print("[collect-hadoop-metrics] HistoryServer indicates no jobs have run.  Exiting.")
        sys.exit(0)

    job_instances = {}
    jobs = jobs.get('job', [])
    for job in jobs:
        # Grab details about the job so we get the full name.
        job_id = job['id']
        job_info = hs.job(job_id).data['job']
        job_name = get_job_name_from_luigi_task_id(job_info['name'])

        # Adjust job name to include a zero-based counter if we've seen it before.  This is kind of weird because
        # it only makes "sense" in the context of a given application i.e. to differentiate Sqoop import tasks
        # from one another, and it may change over time (size of list and thus the indexes) and so be not AS useful
        # for over-time analysis, but it suffices because the indexing will be accurate within a single job flow.
        job_index = job_instances.get(job_name, 0)
        job_instances[job_name] = job_index + 1

        job_counters = hs.job_counters(job['id']).data['jobCounters']

        for counter_group in job_counters.get('counterGroup', []):
            metric_prefix = get_prefix_from_counter_group_name(counter_group['counterGroupName'])

            for metric in counter_group.get('counter', []):
                # Pull out the counter value for both the map and reduce stages, as well as the total.
                metric_name = metric['name'].lower()
                for metric_type in ['total', 'map', 'reduce']:
                    metric_value = metric[metric_type + 'CounterValue']
                    full_metric_path = '.'.join([metric_prefix, metric_name, metric_type])

                    formatted_metrics[full_metric_path] = {
                        'value': metric_value,
                        'context': {
                            'job_id': job_id,
                            'job_name': job_name,
                            'job_index': job_index
                        }
                    }

    return facet_metrics(formatted_metrics, metric_templates, context)


if __name__ == "__main__":
    # Load our configuration.
    if len(sys.argv) < 2:
        print("[collect-hadoop-metrics] You must specify the configuration file to load!  Exiting.")
        sys.exit(0)

    conf_file = sys.argv[1]
    config = {}

    try:
        with open(conf_file, 'r') as f:
            config = yaml.load(f)
    except IOError:
        print("[collect-hadoop-metrics] Error reading configuration file or configuration does not exist!  Exiting.")
        sys.exit(0)

    input_config = config.get('input', {})
    output_config = config.get('output', {})

    metric_templates = input_config.get('templates', [])

    # Check the HistoryServer address specifically, because we can't do anything without it.
    hs_address = input_config.get('hs_address', None)
    if hs_address is None:
        hs_address = get_local_address_on_emr()
    if hs_address is None:
        print("[collect-hadoop-metrics] No HistoryServer address specified and unable to query instance metadata!  Exiting.")
        sys.exit(0)

    print("[collect-hadoop-metrics] Targeting HistoryServer at '{}'.".format(hs_address))

    graphite_config = output_config.get('graphite', {})
    graphite_host = graphite_config.get('host', 'localhost')
    graphite_port = graphite_config.get('port', 2003)
    graphite_prefix = graphite_config.get('prefix', 'edx.analytics.emr')

    try:
        metrics = collect_metrics(hs_address, metric_templates)

        stats_client = graphitesend.init(graphite_server=graphite_host, graphite_port=graphite_port, prefix=graphite_prefix, system_name='')
        stats_client.send_list(metrics)
    except Exception as ex:
        print("[collect-hadoop-metrics] Caught exception while running collection: {}".format(str(ex)))

    print("[collect-hadoop-metrics] Done.  Exiting.")
