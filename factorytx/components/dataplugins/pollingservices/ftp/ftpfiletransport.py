#!/usr/bin/python
# -*- coding: utf-8 -*-

import fnmatch
import stat
from math import floor

from ftputil import FTPHost
from ftputil.error import TemporaryError
from FactoryTx.transports.base import FileEntry, BaseTransport

#
# Tested against: vsftpd
#
# To use the FTP transport with vfstpd do the following:
#   1. sudo apt-get install vsftpd
#   2. sudo vim /etc/vsftpd.conf
#       uncomment/change line 'local_enable=YES'
#       uncomment/change line 'write_enable=YES'
#       uncomment/change line 'chroot_local_user=YES'
#       uncomment/change line 'chroot_list_enable=NO'
#   3. Now either do one of the following since we need write permission:
#       A. chmod a-w /home/<user>
#       B. add line: 'allow_writeable_chroot=YES'
#   4. service vsftpd restart
#

class FTPFileTransport(BaseTransport):
    """
        That is child class which shall implement FTP file management functionality
        statFile(filePath) – get info about a remote file
        copyFile(filePath) – fetch a remote file into working space and return the stats
        deleteFile(filePath) – remove a remote file
        listPath(dirPath) - list files in a directory (potentially recursively)
    """
    __version__ = '1.0.0'

    def __init__(self):
        super(FTPFileTransport, self).__init__()
        self.conn = None
        self._connected = False

    def loadParameters(self, schema, conf):
        super(FTPFileTransport, self).loadParameters(schema, conf)

    def connect(self):
        try:
            self.conn = FTPHost(self.remote_ip, self.username, self.password)
            # not supported in 2.2.3 port=self.remote_port)
            self._connected = True
        except Exception as e:
            self.log.error('Error while create connections. {}'.format(e))
        return self._connected

    def disconnect(self):
        if self.connected:
            self.conn.close()
            self.conn = None
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
            raise Exception('Error FTP not connected')

        try:
            # unfortunately there is no limitaiton on the fetch size, this could be dangerous and slow
            res = self.conn.listdir(self.remote_path)
            remote_entries = []
            for r in res:
                # does the pattern match the filename and is it a file, check in this order, its less expensive
                if fnmatch.fnmatch(r, self.pattern):

                    # the ftp path is always forward slash, but we need the full path, to stat for it
                    ftppath = self.remote_path + "/" + r

                    # get the stat for the path
                    fstat = self.conn.lstat(ftppath)

                    # is it a file
                    if stat.S_ISREG(fstat.st_mode):
                        file_entry = FileEntry(
                            transport=self,
                            path=ftppath,
                            mtime=int(floor(fstat.st_mtime)),
                            size=fstat.st_size
                        )
                        remote_entries.append(file_entry)
                    # else its a link or directory
                # else it doesnt match the pattern
            return remote_entries
        except TemporaryError as te:
            self.log.warning('Warning while get list_files. {}'.format(str(te)))
            return
        except Exception as e:
            # XXX: Use log.exception if we get an unknown exception, or
            #      log.warning if it's a FTP-specific exception.
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
            raise Exception('Error FTP not connected')

        try:
            self.conn.download(source=remote_file_entry.path, target=local_path)
            return
        except Exception as e:
            # XXX: Use log.exception if we get an unknown exception, or
            #      log.warning if it's a FTP-specific exception.
            self.log.error('Error {}, while copying FTP file {}:{}'.format(e,
                                                                           self.remote_path,
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
            raise Exception('Error FTP not connected')

        try:
            self.conn.remove(remote_file_entry.path)
            return
        except Exception as e:
            #
            self.log.error('Error while deleting file. {}.  Check if read-only is enabled on this '
                           'FTP, check/enable write/delete file permissions on the parent directory and/or'
                           'the files themselves'.format(e.message))
            self.disconnect()
            raise

