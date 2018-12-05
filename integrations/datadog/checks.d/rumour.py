from urlparse import urljoin
import requests
import json
from checks import AgentCheck

__version__ = "0.0.1"

DEFAULT_RUMOUR_URL = 'http://rumour:8080'
CHECK_TIMEOUT      = 10

class RumourCheck(AgentCheck):
    '''
    Extract consumer offsets, topic offsets and offset lag from Rumour REST API
    '''
    def check(self, instance):
        base_url = instance.get("url", DEFAULT_RUMOUR_URL)
        clusters = instance.get("clusters")
        extra_tags = instance.get("tags", [])

        self._check_health(base_url, extra_tags)

        clusters = self._find_clusters(base_url, clusters)
        
        self.log.debug("Collecting Topic Offsets")
        self._topic_offsets(clusters, base_url, extra_tags)

        self.log.debug("Collecting Consumer Offsets")
        self._consumer_offsets(clusters, base_url, extra_tags)

    def _check_health(self, base_url, extra_tags):
        """
        Check the Rumour healthz endpoint
        """
        url  = urljoin(base_url, "/healthz")
        tags = ['instance:%s' % self.hostname] + extra_tags
        try:
            response = requests.get(url, timeout=CHECK_TIMEOUT)
            response.raise_for_status()
        except Exception as e:
            self.service_check('rumour.can_connect', AgentCheck.CRITICAL, 
                               tags=tags,
                               message=str(e))
            raise
        else:
            self.service_check('rumour.can_connect', AgentCheck.OK,
                               tags=tags,
                               message='Connection to %s was successful' % url)

    def _topic_offsets(self, clusters, base_url, extra_tags):
        """
        Retrieve the offsets for all topics in the clusters
        """
        for cluster in clusters:
            topics_path = "/v1/clusters/%s/topics" % (cluster)
            topics_list = self._rest_request_to_json(base_url, topics_path).get("topics", [])
            for topic in topics_list:
                topic_path = "%s/%s" % (topics_path, topic)
                offsets = self._rest_request_to_json(base_url, topic_path).get("offsets", [])
                
                for partition, offset in enumerate(offsets):
                    tags = ["topic:%s" % topic, "partition:%s" % partition, "cluster:%s" % cluster] + extra_tags
                    self.gauge("kafka.topic.offset", offset, tags=tags)

    def _consumer_offsets(self, clusters, base_url, extra_tags):
        """
        Retrieve the offsets for all consumer groups in the clusters
        """
        for cluster in clusters:
            consumers_path = "/v1/clusters/%s/consumers" % (cluster)
            consumers_list = self._rest_request_to_json(base_url, consumers_path).get("consumers", [])
            for consumer in consumers_list:
                consumer_path = "%s/%s" % (consumers_path, consumer)
                topics_list = self._rest_request_to_json(base_url, consumer_path).get("topics", [])                
                
                for topic_data in topics_list:
                    topic = topic_data["topic"]
                    for partition, offset_data in enumerate(topic_data.get("offsets", [])):
                        tags = ["topic:%s" % topic, "partition:%s" % partition, 
                                "cluster:%s" % cluster, "consumer:%s" % consumer] + extra_tags
                        self.gauge("kafka.consumer.offset", offset_data["offset"], tags=tags)
                        self.gauge("kafka.consumer.offset.lag", offset_data["lag"], tags=tags)

    def _find_clusters(self, base_url, target):
        """
        Find the available clusters in Rumour, return all clusters if
        target is not set.
        """
        available_clusters = self._rest_request_to_json(base_url, '/v1/clusters').get("clusters")

        if not available_clusters:
            raise Exception("There are no clusters in Rumour")

        if not target:
            return available_clusters
        else:
            clusters = []
            for name in target:
                if name in available_clusters:
                    clusters.append(name)
                else:
                    self.log.error("Cluster '%s' does not exist" % name)
            return clusters

    def _rest_request_to_json(self, base_url, object_path):
        '''
        Query the given URL and return the JSON response
        '''
        response_json = None
        service_check_tags = ['instance:%s' % self.hostname]
        url = urljoin(base_url, object_path)

        try:
            response = requests.get(url)
            response.raise_for_status()
            response_json = response.json()

            if response_json["error"]:
                self.log.error("Request failed: %s: %s" % (object_path, response_json["message"]))
                return {}

        except requests.exceptions.Timeout as e:
            self.log.error("Request timeout: {0}, {1}".format(url, e))
            raise

        except (requests.exceptions.HTTPError,
                requests.exceptions.InvalidURL,
                requests.exceptions.ConnectionError) as e:
            self.log.error("Request failed: {0}, {1}".format(url, e))
            raise

        except ValueError as e:
            self.log.error(str(e))
            raise

        else:
            self.log.debug('Connection to %s was successful' % url)

        return response_json