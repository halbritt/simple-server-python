from factorytx.components.dataplugins.resources.rdp1payload import RDP1Payload
from factorytx.components.dataplugins.resources.fileentry import FileEntry

FILE_PROTOCOLS = {'rdp1': {'type': RDP1Payload, 'metafiles': ['headers', 'binaryattachment']},
                  'localfile': {'type': FileEntry, 'metafiles': []}}
