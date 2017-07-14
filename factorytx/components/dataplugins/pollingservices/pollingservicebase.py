from abc import ABCMeta, abstractmethod
import os
from uuid import uuid4
from factorytx.utils import merge_schema_defaults
import logging
import shelve
from factorytx.Global import setup_log

logname = "Polling-Service"

class Resource(metaclass=ABCMeta):
    """Represents a file (or resource) on a remote system.

    :ivar transport: reference to the transport associated with the file.
    :ivar path: relative path of the file on the remote system. If the FileEntry
        represents a virtual file, the path should remain the same across
        subsequent calls to list_files (ie. if the remote system updates a.txt,
        the path should still remain the same instead of becoming a.v2.txt.)
    :ivar mtime: time the file was last modified represented as seconds since
        the Unix epoch.
    :ivar size: size of the file (or resource) in bytes.

    """
    @property
    @abstractmethod
    def basename(self):
        pass

    @abstractmethod
    def __eq__(self, other):
        pass

    @abstractmethod
    def __hash__(self):
        """ DO IT """
        pass

    @abstractmethod
    def __repr__(self):
        pass

    @abstractmethod
    def encode(self, encoding):
        pass


class PollingServiceBase(metaclass=ABCMeta):

    def __init__(self):
        super(PollingServiceBase, self).__init__()

    def load_parameters(self, schema, conf):
        self.log = setup_log(logname + '::' + conf['logname'], conf['log_level'])
        if conf is None:
            conf = {}
        conf_dict = {}
        conf_dict.update(conf)
        merge_schema_defaults(schema, conf_dict)
        if not 'name' in conf:
            self.log.error("This polling service doesn't have a Name", vars(self))
            conf['name'] = str(uuid4())[:8]
        print("Creating the resource dictionaries")
        self.options = conf_dict
        if not getattr(self, 'plugin_type', []):
            self.plugin_type = self.options['protocol']
        self.resources = {}
        self.last_registered = None

    def register_resource(self, resource):
        """ A RESOURCE MUST BE A SERIALIZEABLE DICTIONARY WHICH ADEQUATELY DEFINES THE PARAMETERS
            FOR THE RESOURCE. """
        # TODO: URL type identifier for a resource, not just random string
        self.log.debug("Registering %s", resource)
        self.log.debug("The resource dictionary has length %s", len(self.resources))
        resource_encoding = resource.encode('utf8')
        self.resources[resource_encoding] = resource_encoding
        resource.polling_service_name = self.options['name']
        return resource

    def get_resource(self, uid):
        """ Gets a resource by a uid.

        """
        if uid in self.resources:
            return self.resources[uid]
        else:
            return None

    def load_resource_data(self, uid):
        """ Loads a resource and returns a tuple of uid, resource_dict, resource_data.
            Hopefully resource_data will be an iterable/generator.


        """
        if uid in self.resources:
            resource = self.resources[uid]
            rse = self.load_resource(resource)
            if rse:
                return (uid, resource, rse)
            else:
                return (uid, resource, None)
        else:
            return (uid, None, None)

    def get_registered_resources(self):
        return self.resources.items()

    def get_len_resources(self):
        return len(self.resources)

    def poll(self):
        """ Polls my resources to try and find new unregistered resources.

        """
        resource_candidates = self.get_new_resources()
        new_resources = []
        for resource in resource_candidates:
            registered = self.register_resource(resource)
            self.last_registered = registered
            new_resources.append(registered)
        return new_resources

    def get_new_resources(self):
        """ Gets all of the new unregistered nicely munged resources as a list of dictionaries.
            Ordered from oldest to newest.
        """
        new_resources = self.get_available_resources()
        self.log.debug("The available resources are %s.", new_resources)
        partitioned_resources = self.partition_resources(new_resources)
        self.log.debug("Partitioned are %s", partitioned_resources)
        filtered_resources = self.chunk_resources(partitioned_resources)
        self.log.debug("Returning some Filtered %s", filtered_resources)
        no_overlap = self.remove_registered_overlap(filtered_resources)
        self.log.debug("The cleaned entries are %s", no_overlap)
        return no_overlap

    def chunk_resources(self, filtered_resources):
        """ Chunks the given resources out and returns a list of appropriately sized disjoint
            resources.

        """
        if getattr(self, 'max_resource_size', []):
            max_size = self.max_resource_size
        else:
            max_size = 0
        running_resources = []
        for resource in filtered_resources:
           chunks = self.chunk_resource(resource, max_size)
           running_resources += [chunks]
        return running_resources

    def filter_registered_resources(new_resources):
        """ Filters the new resources and removes any overlap with registered resources.

        """
        last_resource = self.last_registered
        present_resources = self.resources
        filtered = self.resource_difference(resources, present_resources, last_resource)
        return filtered

    def get_available_resources(self):
        """ Gets a list of new resources, returned from oldest to newest, that are available
            for processing.
        """
        unregistered = []
        all_resources = self.get_all_resources()
        self.log.info("The resources that are available number %s.", len(all_resources))
        self.log.debug("The resource registrations are %s", self.resources)
        for resource in all_resources:
            resource_name = resource.encode('utf-8')
            if not resource_name in self.resources:
                unregistered.append(resource)
            else:
                log.debug("The resource %s is registered with key %s", resource, self.resources[resource_name])
        self.log.debug("The resources that are available and unregistered number %s", len(unregistered))
        return unregistered

    def remove_registered_overlap(self, resources):
        present = self.resources
        last = self.last_registered
        return self.resource_difference(resources, present, last)

    def delete_resource_trace(self, resource_id):
        if resource_id in self.resources:
            del self.resources[resource_id]

    @abstractmethod
    def remove_resource(self, resource_id):
        """ Remove the persistence of a resource after sucessful transmission. """
        pass

    @abstractmethod
    def prepare_resource(self):
        """ The purpose of this method is to do any moving of the location of the specified resource
            in order to prepare it to be processed quickly and easily. e.g. SMB/FTP polling services
            will need to copy remote resources to a local place or load it into some buffer at the
            minimum.
        """
        pass

    @abstractmethod
    def return_resource_class(self):
        """ Return the class of the resource that this polling service uses. """
        pass

    @abstractmethod
    def get_all_resources(self):
        """ This can be as efficient or as inefficient as the developer chooses to make it,
            having access to all of the resource dictionaries for the data.
        """
        pass

    @abstractmethod
    def partition_resources(self, resources):
        """ Given the output of the get_available_resources_function, return a partition of the
            resources, from oldest to newest. """
        pass

    @abstractmethod
    def chunk_resource(self, resource, max_size):
        """ Given a resource returned by the get_available_resources function and cleaned by
            partition, breaks the resource down into multiple if the resource exceeds the max_size.
        """
        pass

    @abstractmethod
    def resource_difference(self, resources, present_resources, last_resource):
        """ Given a list of oldest-first disjoint get_available_resources, clean the list of
            any overlap with previously registered resources.
        """
        pass
