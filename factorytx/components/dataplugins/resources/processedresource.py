from factorytx.components.dataplugins.resources.resource import Resource
from abc import ABCMeta, abstractmethod


class ProcessedResource(Resource, metaclass=ABCMeta):

    @abstractmethod
    def factory_method(resource_ids, resource_data):
        pass
