from factorytx.components.dataplugins.resources.resource import Resource
from abc import ABCMeta, abstractmethod


class RawResource(Resource, metaclass=ABCMeta):

    @abstractmethod
    def load_resource(self):
        pass
