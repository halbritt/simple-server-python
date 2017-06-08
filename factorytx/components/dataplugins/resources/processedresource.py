from factorytx.components.dataplugins.resources.resource import Resource
from abc import ABCMeta, abstractmethod


class ProcessedResource(Resource, metaclass=ABCMeta):

    transformable = True

    @abstractmethod
    def factory_method(resource_ids, resource_data):
        pass
