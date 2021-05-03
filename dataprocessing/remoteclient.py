from os import system
from paramiko import SSHClient, AutoAddPolicy, RSAKey, SFTPClient
from paramiko.auth_handler import AuthenticationException, SSHException
from scp import SCPClient, SCPException
import logging as logger


class RemoteClient:
    """Client to interact with a remote host via SSH & SCP."""

    def __init__(self, host, user, ssh_key_filepath, show_progress=False):
        self.host = host
        self.user = user
        self.ssh_key_filepath = ssh_key_filepath
        self.client = None
        self.scp: SCPClient = None
        self.conn = None
        self.sftp: SFTPClient = None
        self.show_progress = show_progress

    def _get_ssh_key(self):
        """
        Fetch locally stored SSH key.
        """
        try:
            self.ssh_key = RSAKey.from_private_key_file(self.ssh_key_filepath)
            logger.info(f'Found SSH key at self {self.ssh_key_filepath}')
        except SSHException as error:
            logger.error(error)
        return self.ssh_key

    def _connect(self):
        """Open connection to remote host."""
        if self.conn is None:
            try:
                self.client = SSHClient()
                self.client.load_system_host_keys()
                self.client.set_missing_host_key_policy(AutoAddPolicy())
                self.client.connect(
                    self.host,
                    username=self.user,
                    key_filename=self.ssh_key_filepath,
                    look_for_keys=True,
                    timeout=5000
                )

                # Define progress callback that prints the current percentage completed for the file
                def progress(filename, size, sent):
                    print("%s's progress: %.2f%%   \r" % (filename, float(sent) / float(size) * 100), end='\x1b[1K\r')

                progress_val = progress if self.show_progress else None
                self.scp = SCPClient(self.client.get_transport(), progress=progress_val, socket_timeout=15.0)
                self.sftp = SFTPClient.from_transport(self.client.get_transport())
            except AuthenticationException as error:
                logger.error(f'Authentication failed: \
                    did you remember to create an SSH key? {error}')
                raise error
        return self.client

    def disconnect(self):
        """Close ssh connection."""
        if self.client:
            self.client.close()
        if self.scp:
            self.scp.close()
        if self.sftp:
            self.sftp.close()

    def execute_commands(self, commands):
        """
        Execute multiple commands in succession.

        :param commands: List of unix commands as strings.
        :type commands: List[str]
        """
        self.conn = self._connect()
        for cmd in commands:
            stdin, stdout, stderr = self.client.exec_command(cmd)
            stdout.channel.recv_exit_status()
            response = stdout.readlines()
            for line in response:
                logger.info(f'INPUT: {cmd} | OUTPUT: {line}')

    def list_dir(self, dir_path):
        self.conn = self._connect()
        return self.sftp.listdir(dir_path)

    def file(self, filename, mode='r', bufsize=-1):
        self.conn = self._connect()
        return self.sftp.open(filename, mode=mode, bufsize=bufsize)

    def download_file(self, file, local_path=''):
        """Download file from remote host."""
        self.conn = self._connect()
        self.scp.get(file, local_path=local_path)
