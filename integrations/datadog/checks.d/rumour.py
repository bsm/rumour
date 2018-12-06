from urlparse import urljoin

import requests
from datadog_checks.checks import AgentCheck

class RumourCheck(AgentCheck):
    TIMEOUT = 5
    DEFAULT_URL = 'http://rumour:8080'
    SERVICE_CHECK_NAME = 'rumour.can_connect'
    SOURCE_TYPE_NAME = 'rumour'

    '''
    Extract data from Rumour REST API
    '''
    def check(self, instance):
        self.check_health(instance)

        clusters = self.find_clusters(instance)

        self.log.debug("Collecting topic offsets")
        self.check_topic_offsets(instance, clusters)

        self.log.debug("Collecting consumer offsets")
        self.check_consumer_offsets(instance, clusters)

    def check_health(self, instance):
        self.rest(instance, '/healthz', service_check=True)

    def check_topic_offsets(self, instance, clusters):
        for cluster in clusters:
            endpoint = "/v1/clusters/%s/topics" % (cluster)
            topics = self.rest(instance, endpoint).get("topics", [])
            for topic in topics:
                topic_endpoint = "%s/%s" % (endpoint, topic)
                offsets = self.rest(instance, topic_endpoint).get("offsets", [])
                for partition, offset in enumerate(offsets):
                    tags = ["topic:%s" % topic, "partition:%s" % partition,
                            "cluster:%s" % cluster] + self.get_config_tags(instance)
                    self.gauge("kafka.topic.offset", offset, tags=tags)

    def check_consumer_offsets(self, instance, clusters):
        for cluster in clusters:
            endpoint = "/v1/clusters/%s/consumers" % (cluster)
            consumers = self.rest(instance, endpoint).get("consumers", [])
            for consumer in consumers:
                consumer_endpoint = "%s/%s" % (endpoint, consumer)
                topics = self.rest(instance, consumer_endpoint).get("topics", [])
                for topic_data in topics:
                    topic = topic_data["topic"]
                    for partition, offset_data in enumerate(topic_data.get("offsets", [])):
                        tags = ["topic:%s" % topic, "partition:%s" % partition,
                                "cluster:%s" % cluster, "consumer:%s" % consumer] + self.get_config_tags(instance)
                        self.gauge("kafka.consumer.offset", offset_data["offset"], tags=tags)
                        self.gauge("kafka.consumer.offset.lag", offset_data["lag"], tags=tags)


    def find_clusters(self, instance):
        known = self.rest(instance, '/v1/clusters').get("clusters")
        if not known:
            raise Exception("There are no known clusters")

        selected = instance.get('clusters')
        if not selected:
            return known

        clusters = []
        for name in selected:
            if name in known:
                clusters.append(name)
            else:
                self.log.error("Cluster '%s' does not exist" % name)
        return clusters

    def get_config_tags(self, instance):
        tags = instance.get('tags', [])
        return list(set(tags)) if tags else []

    def get_config_timeout(self, instance):
        return int(instance.get('timeout', self.TIMEOUT))

    def get_config_url(self, instance):
        base = instance.get('url', None)
        if base is None:
            raise Exception("A url must be specified")
        return base

    def rest(self, instance, endpoint, service_check=False):
        base = self.get_config_url(instance)
        url = urljoin(base, endpoint)

        try:
            r = requests.get(url, timeout=self.get_config_timeout(instance))
            r.raise_for_status()
        except requests.exceptions.Timeout as e:
            self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.CRITICAL, message="Request timeout: {0}, {1}".format(url, e))
            raise
        except requests.exceptions.HTTPError as e:
            data = r.json()
            message = str(e.message)
            if data.get("error", False):
                message = data.get("message", message)

            self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.CRITICAL, message=message)
            raise
        except Exception as e:
            self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.CRITICAL, message=str(e))
            raise

        if service_check:
            self.service_check(self.SERVICE_CHECK_NAME, AgentCheck.OK, message='Connection to %s was successful' % url)
            return None

        return r.json()
