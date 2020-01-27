import paramiko


class Client:
    def __init__(self, password, user, ip, port):
        self.password = password
        self.user = user
        self.ip = ip
        self.port = port

    def ssh_initialize(self):
        """
        Initialize the SSH connection
        """
        ssh = paramiko.SSHClient()
        try:
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(hostname=self.ip, port=self.port, username=self.user, password=self.password) # connects to the server
            print("SSH Connection established root@{0}".format(self.ip))
            return ssh
        except paramiko.ssh_exception.AuthenticationException as e:
            print("Unable to establish SSH connection "
                         "to root@{0}:\n{1}".format(self.ip, e))
            return "cant connect"
