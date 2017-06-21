from factorytx.components.dataplugins.resources.resource import Resource
from abc import ABCMeta, abstractmethod


class ProcessedResource(Resource, metaclass=ABCMeta):

    transformable = True

    @abstractmethod
    def to_record_string(self):
        pass

    @abstractmethod
    def remove_trace(self):
        pass
