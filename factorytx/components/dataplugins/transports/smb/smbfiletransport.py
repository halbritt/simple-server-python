#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
    Notes: to support SMB transport, please follow the setup procedures below:
    1. Install samba.
        On Ubuntu: sudo apt-get install samba
    2. Create a user account with password
        On Ubuntu: sudo adduser <username>
          Password : <password>
    3. Add the user <username> to Samba's smbpasswd
        On Ubuntu: smbpasswd -a <username>
    4. Locate the shared folder and add the below config to the bottom of the /etc/samba/smb.conf file
         [test]
            comment = test
            browseable = yes
            path = <path to your test directory>
            guest ok = no
            read only = no
            create mask = 0766
            directory mask = 0766
            valid users = <username>
    5.  Make sure your <path to your shared directory> allows 'other' read/write access to the files and the
        parent directory as well.  This allows for 'read/list' as well as 'delete'.
"""
import os
import logging
from math import floor
from smb.SMBConnection import SMBConnection

from FactoryTx.components.dataplugins.transports.base import FileEntry, BaseTransport

# Bitmask for basic file attributes
# [MS-CIFS]: 2.2.1.2.4
attribytemask = {
    'SMB_FILE_ATTRIBUTE_NORMAL': 0x00,
    'SMB_FILE_ATTRIBUTE_READONLY': 0x01,
    'SMB_FILE_ATTRIBUTE_HIDDEN': 0x02,
    'SMB_FILE_ATTRIBUTE_SYSTEM': 0x04,
    'SMB_FILE_ATTRIBUTE_VOLUME': 0x08,
    'SMB_FILE_ATTRIBUTE_DIRECTORY': 0x10,
    'SMB_FILE_ATTRIBUTE_ARCHIVE': 0x20,
    'SMB_SEARCH_ATTRIBUTE_READONLY': 0x0100,
    'SMB_SEARCH_ATTRIBUTE_HIDDEN': 0x0200,
    'SMB_SEARCH_ATTRIBUTE_SYSTEM': 0x0400,
    'SMB_SEARCH_ATTRIBUTE_DIRECTORY': 0x1000,
    'SMB_SEARCH_ATTRIBUTE_ARCHIVE': 0x2000
}


class SMBFileTransport(BaseTransport):
    """
        That is child class which shall implement Samba file management functionality
        list_files(dirPath) - list files in a directory (potentially recursively)
        copy_file(filePath) – fetch a remote file into working space and return the stats
        delete_file(filePath) – remove a remote file
    """
    def __init__(self):
        super(SMBFileTransport, self).__init__()
        self.conn = None
        self._connected = False

    def loadParameters(self, schema, conf):
        super(SMBFileTransport, self).loadParameters(schema, conf)

    def connect(self):
        try:
            self.conn = SMBConnection(self.username, self.password, self.source, self.remote_name,
                                      domain=self.domain, use_ntlm_v2=self.use_ntlm_v2, is_direct_tcp=self.is_direct_tcp)
            logging.getLogger("SMB.SMBConnection").setLevel(logging.WARNING)
            self._connected = self.conn.connect(self.remote_ip, self.remote_port, timeout=self.timeout)
            return self._connected
        except Exception as e:
            self.log.error('Error while create SMB connections to {}@{}. {}'.format(self.remote_name,
                                                                                    self.remote_ip,
                                                                                    e))
            return False

    def disconnect(self):
        if self._connected:
            self.conn.close()
        self._connected = False

    @property
    def connected(self):
        return self._connected

    def list_files(self):
        """Returns a list of FileEntry objects representing all files currently
        available via the transport.

        :returns: a list of FileEntry objects.
        :raises Exception: if the transport was not able to list all files on
            the remote system.

        """
        if not self._connected and not self.connect():
            raise Exception('Error SMB not connected')

        try:
            res = self.conn.listPath(service_name=self.shared_folder,
                                     path=self.dir_path,
                                     pattern=self.pattern,
                                     timeout=self.timeout)
            remote_entries = []
            for r in res:
                if r.isDirectory:
                    continue

                file_entry = FileEntry(
                    transport=self,
                    path=os.path.join(self.dir_path, r.filename),
                    mtime=int(floor(r.last_write_time)),
                    size=r.file_size
                    )

                remote_entries.append(file_entry)
            return remote_entries
        except Exception as e:
            # XXX: Use log.exception if we get an unknown exception, or
            #      log.warning if it's a SMB-specific exception.
            self.log.error('Error while get list_files. {}'.format(e.message))
            self.disconnect()
            raise

    def copy_file(self, remote_file_entry, local_path):
        """Copies a remote file to the specified local path, preserving the
            last modification time.

            :param remote_file_entry: FileEntry object representing the file to fetch.
            :param local_path: path to save the file to. Leading directory
                components must already exist. If a file already exists at the
                path it will be overwritten.
            :returns: None
            :raises Exception: if copying the file did not succeed.
        """
        # TODO: Just use exceptions FFS.
        if not self._connected and not self.connect():
            raise Exception('Error SMB not connected')

        try:
            with open(local_path, 'wb') as fp:
                fileinfo = self.conn.retrieveFile(service_name=self.shared_folder,
                                                              path=remote_file_entry.path,
                                                              file_obj=fp)
                # can't identify an error if an invalid filename is provide, there is no way to know if the
                # retrieve file will fail
                # if fileinfo[0] == 0:
                #    raise Exception('Error retrieveFile failed, possible invalid input file')

            return
        except Exception as e:
            # XXX: Use log.exception if we get an unknown exception, or
            #      log.warning if it's a SMB-specific exception.
            self.log.error('Error {}, while copying SMB file {}:{}'.format(e,
                                                                           self.shared_folder,
                                                                           remote_file_entry.path))
            self.disconnect()
            raise

    def delete_file(self, remote_file_entry):
        """Removes a remote file.
            :param remote_file_entry: FileEntry object representing the file to remove.
            :returns: None
            :raises Exception: if deleting the file did not succeed.
        """
        if not self._connected and not self.connect():
            self.log.error('Error while delete_file, not connected')
            raise Exception('Error SMB not connected')

        try:
            self.conn.deleteFiles(service_name=self.shared_folder,
                                  path_file_pattern=remote_file_entry.path,
                                  timeout=self.timeout)
            return
        except Exception as e:
            # so IF the samba share is setup 'read-only' the read will succeed but the delete will fail
            self.log.error('Error while deleting file. {}.  Check if read-only is enabled on this '
                           'share, enable write/delete file permissions on the parent directory and/or'
                           'the files themselves'.format(e.message))
            self.disconnect()
            raise

    #
    # methods below were left as reference only in case of future need
    #

    # deprecated
    def create_file(self, file_path, fileobj):
        if not self._connected and not self.connect():
            return False

        try:
            rec_bytes = self.conn.storeFile(service_name=self.shared_folder, path=file_path,
                                            file_obj=fileobj, timeout=self.timeout)
            return True
        except Exception as e:
            self.disconnect()
            self.log.error('Error while creating file. {}'.format(e.message))
            return False

    # deprecated
    def stat_file(self, file_path):
        """ it shall do the same like os.stat() -> (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime)
            file_size, last_access_time, last_write_time, create_time, last_attr_change_time,
            alloc_size, isReadOnly, file_attributes
            but it return back (size, atime, mtime, ctime, latime, asize, isreadonly, fattr)
        """
        if not self._connected and not self.connect():
            return

        f = self.conn.getAttributes(service_name=self.shared_folder, path=file_path, timeout=30)
        # TODO. Need to make it easy
        if f and not f.isDirectory:
            size = f.file_size
            atime = f.last_access_time
            mtime = f.last_write_time
            ctime = f.create_time
            latime = f.last_attr_change_time
            asize = f.alloc_size
            isreadonly = f.isReadOnly
            fattr = f.file_attributes
            return size, atime, mtime, ctime, latime, asize, isreadonly, fattr
        return

    # deprecated
    def __getsmbfileattr(self, file_attributes):
        # TODO. Still under development
        res = []
        for k, v in attribytemask.items():
            if file_attributes & v:
                res.append(k)
        return res

