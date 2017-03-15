from datetime import datetime

from opcua import Client
import opcua.ua as ua

# needed to catch socket errors
import socket, time

from FactoryTx.components.dataplugins.DataPlugin import DataPlugin

log = None

from concurrent.futures import TimeoutError

class UAError(Exception):
    pass

def get_tags(nodes):
    tag_list = []
    for node in nodes:
        child_nodes = node.get_children()
        if child_nodes:
            print("Object: " + str(node))
            print("Object Children: " + str(child_nodes))
            tag_list.extend(get_tags(child_nodes))
        else:
            node = str(node)
            node = node[node.find('ns='):node.find('))')]
            if node and node != ',':
                print('Tag: ' + node)
                tag_list.append(node)
    return tag_list


class OPCUAService(DataPlugin):
    """
    This is the polling service for an OPC UA server.

    You need to have the following specified in the config:

    machines:
    - plugins:
      - type: opcua
        name: OPC UA Service
        config:
          machine: 'Machine 55434'
          version: '1.0.0'
          host: '10.8.10.73'
          port: 49320
          poll_rate: 1.5
          tags:
          - { name: 'Channel1.Device1.Tag1', exportname: Channel1Device1Tag1 }
          - { name: 'Channel1.Device1.Tag2', exportname: Channel1Device1Tag2 }
          outputdirectory: /path/to/data/dir/


    Note:
        To setup and use Ignition
        1. Install Ignition
            sudo add-apt-repository ppa:webupd8team/java
            sudo apt-get update
            sudo apt-get install oracle-java8-installer
            java -version (should be 1.8.something)

            wget https://s3.amazonaws.com/files.inductiveautomation.com/release/build7.8.2/2016030813/ignition-7.8.2-linux-x64-installer.run
            chmod +x ignition-7.8.2-linux-x64-installer.run
            sudo ./ignition-7.8.2-linux-x64-installer.run
        2. Setup an OPCUA server.  Either Kepware and you need to configure its DCOM or FreeOPCUA
            FreeOPCUA Setup:
            a. pip install freeopcua
            b. create a sample server.py:
                import opcua
                server = opcua.Server()
                server.set_endpoint("opc.tcp://127.0.0.1:49320")
                server.start()
                myvariable1 = objects.add_variable(2, "Tag1", 0)
                myvariable2 = objects.add_variable(3, "Tag2", 1)
        3. Configure Ignition go to webpage
           a. http://localhost:8088
           b. click on OPC Connections->Servers->Add Server.  Select the IP/Port as shown and follow the wizard to
           add your OPCUA server IP/Port to this Gateway.
        4. Optional to download the certificate (which is downloaded by default, but for performance you can
           pre-download.  If you don't specify the correct port this will cause a hang.) and include as part of
           the security_settings.  Click OPC-UA->Certificates and download the certificate.  Then change the
           security_settings to include the server certificate.
        5. Create a user and password.  Click on Security->Users,Roles.  Then select edit for "Manage Users and Roles
           for Profile 'opca-module'".  Click 'Add User' and set the username and password to be used in your config.
        6. Create your private and public key
           openssl req -nodes -new -x509 -keyout /opt/sightmachine/keys/private.pem -out /opt/sightmachine/keys/public.pem
        7. Edit your FactoryTx configuration and include the Username, Password, Port, security_settings
           changes you've made

        Using freeopcua 10.11
        8. One more change that's not included but required and at some point it will be part of OPCUA.  Edit the OPCUA
            library client.py file.  (this will be located in your python environment libraries) Change the line
            that reads ua.pack_bytes(bytes(password,'utf8')... and strip out the ",'utf8'".
        Uinsg freeopcua 10.12
        8. Have to modify the file /usr/local/lib/python2.7/dist-packages/opcua/client/client.py.  add the following
            line at the end of the function _encrypt_password "return data, uri"

    """

    __version__ = '1.0.0'

    ua_params = None
    logname = "OPCUAService"
    # socket_timeout = 600

    def __init__(self):
        super(OPCUAService, self).__init__()

    def connect(self):
        isIgnition = False

        import logging
        # get rid of the excessive opcua library logs
        opclog = logging.getLogger('opcua')
        opclog.setLevel(self.default_loglevel)

        socket.setdefaulttimeout(self.timeout)

        try:
            if hasattr(self, 'username') and hasattr(self, 'password') and hasattr(self, 'security_settings'):
                #  expectation that both are requshopenssl genrsa -des3 -out pired... empty "" string is valid?
                log.info('OPC UA Ignition Setup: {}:{}.  If this hangs, IP/Port is '
                         'incorrect'.format(self.host, self.port))
                self.client = Client('opc.tcp://{}:{}@{}:{}/iaopcua`'.format(self.username, self.password,
                                                                    self.host, self.port), timeout=self.timeout)
                self.client.set_security_string(self.security_settings)
                isIgnition = True

                if not hasattr(self, 'node_server_idx'):
                    self.node_server_idx = 1
                    log.info('Missing node_server_idx, defaulting to 1')
            else:
                log.info('OPC UA Server Setup: {}:{}'.format(self.host, self.port))
                self.client = Client('opc.tcp://{}:{}'.format(self.host, self.port), timeout=self.timeout)

        except Exception as e:
            log.error('Error creating OPC UA Client: opc.tcp://{}:{}.  {}'.format(self.host, self.port, e))
            raise e

        try:
            log.info('Connecting to OPC UA Server: {}:{}'.format(self.host, self.port))
            self.client.connect()
        except Exception as e:
            log.error('Cannot connect to OPC UA Server: {}'.format(e))
            raise e

        log.info('Successfully connected to host: {}'.format(self.host, self.port))

        try:
            self.ua_params = ua.ReadParameters()
            self.objs = self.client.get_objects_node()

            # hack to dump the tag list for an Ignition gateway
            if hasattr(self, 'tags_file') and isIgnition:
                self.tags = get_tags(self.objs.get_children())
                with open(self.tags_file, 'wb') as fd:
                    try:
                        for tag in self.tags:
                            fd.write("%s\n" % tag)
                    except ValueError as e:
                        self.log.error('Error while writing tag file {}'.format(e))

                log.info('Wrote found tags to file {}'.format(self.tags_file))
                return

            if not self.tags:
                log.error('Error tags must be set')

            tag_names = [tag['name'] for tag in self.tags]

            for tag in tag_names:
                if isIgnition:
                    itag = 'ns={};s={}'.format(self.node_server_idx, tag)
                    child = self.client.get_node(itag)
                elif hasattr(self, 'name_space'):
                    itag = 'ns={};s={}'.format(self.name_space, tag)
                    child = self.client.get_node(itag)
                else:
                    child = self.objs.get_child(tag)

                rv = ua.ReadValueId()
                rv.NodeId = child.nodeid
                rv.AttributeId = ua.AttributeIds.Value
                self.ua_params.NodesToRead.append(rv)
                time.sleep(0.25)

        except Exception as e:
            log.error('Error configuring tag "{}" to query.  Failed: {}'.format(tag, e))
            raise e
        log.info('Successfully built NodesToRead for {} tags'.format(len(self.tags)))
        return super(OPCUAService, self).connect()

    def read(self):
        result = []
        timestamp = datetime.now()

        try:
            res = self.objs.server.read(self.ua_params)

            for idx, tag in enumerate(self.tags):
                result.append((tag['name'], res[idx].Value.Value))

        except UAError as e:
            log.warn('Error in UA library (likely socket error)')
            self.reconnect()
        except socket.error as e:
            log.warn('Socket communication error with OPC server.  Will try to reconnect. {}'.format(e))
            self.reconnect()
        except IOError as e:
            log.warn('IO Error with OPC server.  Will try to reconnect. {}'.format(e))
            self.reconnect()
        except TimeoutError as e:
            log.warn('Communication error with OPC server.  Will try to reconnect. {}'.format(e))
            self.reconnect()
        except Exception as e:
            log.warn('Error in OPC read on {}: {}'.format(self._getSource(), e))
            self.reconnect()

        # If all values returned from opcua server are None, record is not emitted
        if all(value[1] is None for value in result):
            log.info('All values returned are None, will not emit record.')
            return []

        result = dict(result)
        if hasattr(self, 'multi_source'):
            records = self._multi_source_generation(timestamp, result)
        else:
            # records = [(timestamp, result, False)]
            records = [{'timestamp': timestamp, 'result': result}]

        return records

    def save_raw(self, records):
        #todo
        pass

    def load_raw(self, source):
        #todo
        pass

    def process(self, record):
        # t, probe, source = record
        t = record['timestamp']
        probe = record['result']
        source = record.get('source')

        data = {}
        data['timestamp'] = t
        mapped = {}
        for tag in self.tags:
            if tag['name'] in probe:
                mapped[tag.get('exportname', tag['name'].split(".")[-1])] = {'value': probe[tag['name']]}

        data['fieldvalues'] = mapped
        data['poll_rate'] = self.poll_rate

        if hasattr(self, 'counter_rate'):
            data['counter'] = int(((t - datetime(1970,1,1)).total_seconds()) / self.counter_rate)

        # indicates multi_source is being used so the source must be changed
        if source:
            data['source'] = source  # change so different data.source appears in sslogs
            self.source = source  # change source to avoid warnings

        return data

    def _multi_source_generation(self, timestamp, result):
        """
        Is called by the read function if self.multi_source present

        Duplicates record for each source in self.multi_source so that duplicate sslogs are produced

        :param timestamp: datetime.datetime
        :param result: dict of fieldvalues
        :return: list of records to process
        """
        records = []
        for source in self.multi_source:
            records += [{'timestamp': timestamp, 'result': result, 'source': source}]

        return records
