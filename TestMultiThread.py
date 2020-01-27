import threading
import logging
import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import SSHClient

ip = ""
password = ""
user = ""
port = 22

# directory on this machine
watchdog_directory = "C:/Users/modris/Desktop/dokumenti - raimonds/HPCDataProcessing/copiedFiles"

# directory on this machine needed only if not using watchdog
copy_from_directory = "C:/Users/modris/Desktop/dokumenti - raimonds/HPCDataProcessing/copiedFiles"
# directory on remote server
send_to_directory = "/home/test/files1"
# exclude subdirectories like:
exclude_directories = ["C:/Users/modris/Desktop/dokumenti - raimonds/HPCDataProcessing/copiedFiles\\Test\\test",
                       "other"]

# directory for saving sent or received file names like :"/home/data/sentFileNames.txt"
sent_file_names_directory = "C:/Users/modris/PycharmProjects/HPCBataProcessing/HPC/LatestSample/sentFileNames.txt"


class Watcher:
    DIRECTORY_TO_WATCH = watchdog_directory

    def __init__(self):
        self.observer = Observer()

    def run(self):
        event_handler = Handler()
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except:
            self.observer.stop()
            print("Error")

        self.observer.join()


class Handler(FileSystemEventHandler):

    @staticmethod
    def on_any_event(event):
        if event.is_directory:
            return None
        # every time file is being created this event will execute and give the path to the file created.
        elif event.event_type == 'created':
            print("Received created event - %s" % event.src_path)
            send_file_by_watchdog(event)  # to send the file that just was created under given watch directory


def get_file_name_from_event(event):
    path = event.src_path
    # path.replace("/", " ")
    path = path.replace('\\', " ")
    get_name = path.split(' ')
    file_name = get_name[-1]
    print(file_name)
    return file_name


# sends just created file to a remote server on a new channel
def send_file_by_watchdog(event):
    file_name = get_file_name_from_event(event)
    path = event.src_path
    send = True
    new_path = path.replace(file_name, '')
    for no_dir in exclude_directories:
        if new_path == no_dir + "\\":
            send = False
    if send:
        counter = compare_file_names(file_name)
        if counter == 0:
            logging.info("starting thread for " + path)
            save_file_name(file_name)
            x = threading.Thread(target=send_file, args=(new_path, send_to_directory, file_name))
            x.start()
    # thread = [new_thread(from_directory=path, to_directory=send_to_directory, file_name=file_name, action="send")]


# for control
def main():
    format_type = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format_type, level=logging.INFO,
                        datefmt="%H:%M:%S")
    # to launch the watch dog uncomment next two lines
    if ssh_client == "cant connect":
        print("You have entered wrong credentials")
    else:
        w = Watcher()

        w.run()
        # request_center()
        # close all the connections
        ssh_client.close()


# call actions, define directories or commands
def request_center():
    # commands for exec_command()
    # copy file from server to my machine
    # cmd = "scp -P " + client.port + " " + client.user + "@" + client.ip + ":" + remote_directory + " " + local_directory +""
    # copy file from my machine to server
    # cmd2 = "scp -P 1234 " + from_directory + " " + user2 + "@" + ip2 + ":" + to_directory # file to different server/socket

    transfer_new_files(copy_from_directory, send_to_directory)
    # clear_remote_directory(send_to_directory)


# uncomment the line for type of action you want to make
def transfer_new_files(from_directory, to_directory):

    send_files_to_server(from_directory, to_directory)
    # copy_files_from_server(from_directory, to_directory)
    return True


# sends all files from given directory and subdirectories to the other given directory on multiple channels
def send_files_to_server(from_directory, to_directory):
    for cur_dir, directories, files in os.walk(from_directory):
        send = True
        for no_dir in exclude_directories:
            if cur_dir == no_dir:
                send = False
        cur_dir = cur_dir + "\\"
        threads = []

        if send:
            for file_name in files:
                counter = compare_file_names(file_name)
                if counter == 0:
                    logging.info("starting thread for " + cur_dir + "/" + file_name)
                    save_file_name(file_name)
                    x = threading.Thread(target=send_file, args=(cur_dir, to_directory, file_name))
                    threads.append(x)
                    x.start()

            for t in threads:
                t.join()


# sends the file to the remote host on a new channel
def send_file(from_directory, to_directory, file_name):
    sftp = ssh_client.open_sftp()
    try:
        sftp.put(from_directory + "/" + file_name, to_directory + "/" + file_name)
        logging.info("Thread finishing for " + from_directory + file_name)
    except FileNotFoundError:
        rewrite_line(file_name)  # if transfer failed remove file name from the list
    except IOError:
        rewrite_line(file_name)
    sftp.close()


# copies all files from given directory and sub-directories using multiple channels
def copy_files_from_server(from_directory, to_directory):
    all_directories = get_list_of_subdirectories(from_directory)
    threads = []
    copy = True
    for directory in all_directories:
        for no_dir in exclude_directories:
            if directory == no_dir:
                copy = False
        if copy:
            all_files = get_list_of_file_names(directory)
            for file_name in all_files:
                counter = compare_file_names(file_name)
                if counter == 0:
                    logging.info("starting thread for " + from_directory + "/" + file_name)
                    save_file_name(file_name)
                    x = threading.Thread(target=copy_file, args=(from_directory, to_directory, file_name))
                    threads.append(x)
                    x.start()
            for t in threads:
                t.join()


#def new_thread(from_directory, to_directory, file_name, action):
#    counter = compare_file_names(file_name)
#    x = None
#    if counter == 0:
#        logging.info("starting thread for " + from_directory + "/" + file_name)
#        save_file_name(file_name)
#       if action == "copy":
#            wthreads.append(threading.Thread(target=copy_file, args=(from_directory, to_directory, file_name)).start())
#        elif action == "send":
#           wthreads.append(threading.Thread(target=send_file, args=(from_directory, to_directory, file_name)).start())


# gets the file from remote host on new channel
def copy_file(from_directory, to_directory, file_name):
    sftp = ssh_client.open_sftp()
    try:
        sftp.get(from_directory + "/" + file_name, to_directory + "/" + file_name)
        logging.info("Thread finishing for " + from_directory + "/" + file_name)
    except FileNotFoundError:
        rewrite_line(file_name)
        print("file was not found")
    except IOError:
        rewrite_line(file_name)  # if transfer failed remove file name from the list

    sftp.close()


# returns list of all file names from given host directory
def get_list_of_file_names(directory):
    only_file_names = "ls -p " + directory + " | grep -v /"
    path_files = execute_command(only_file_names)
    new_file_set = {file.replace("\n", '') for file in path_files}
    return new_file_set


# checks if directory exist, if not make the directory(useful for copying files to my machine)
def check_if_directory_exist(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


# checks if file exist, if not make the file(useful for copying files to my machine)
def check_if_file_exist(directory):
    if not os.path.exists(directory):
        with open(directory, 'w+') as f:
            f.close()
    else:
        return True


# returns a list of subdirectories from the remote host
def get_list_of_subdirectories(starting_directory):
    get_path = execute_command("find " + starting_directory + " -type d")
    new_path_set = {path.replace("\n", '') for path in get_path}
    return new_path_set


# compares the given file name with file names in sentFileNames.txt file
def compare_file_names(file_name):
    check_if_file_exist(sent_file_names_directory)
    with open('sentFileNames.txt', 'r', buffering=2000000) as f:
        content = f.readlines()
        f.close()
        counter = 0
        for line in content:
            if file_name + "\n" == line:
                counter = counter + 1
    return counter


# saves the name of file that is going to be sent to client
def save_file_name(file_name):
    output = open('sentFileNames.txt', 'a')  # file output
    output.write(file_name + "\n")
    output.close()


# removes the file name from all file name list
def rewrite_line(temp_name):
    with open('sentFileNames.txt', 'r') as rf:
        data = rf.readlines()
        with open('sentFileNames.txt', 'w') as final:
            for d in data:
                if not d == temp_name + "\n":
                    final.writelines(d)

    final.close()


# removes file from the remote directory
def remove_file_from_directory(path, filename):
    sftp = ssh_client.open_sftp()
    sftp.remove((path + "/" + filename))
    sftp.close()


# removes all files from directories
def clear_remote_directory(directory):
    files = get_list_of_file_names(directory)
    for file in files:
        remove_file_from_directory(directory, file)


# deletes given directory on remote host only if it is empty
def delete_directory(directory):
    sftp = ssh_client.open_sftp()
    sftp.rmdir(directory)
    sftp.close()


# new function get_put() that can transfer from remote to remote only need to implement transferring to different remote
# (not really useful now)
def get_put(self, remote_path, remote_path2, callback=None, confirm=True):
    file_size = self.stat(remote_path).st_size
    with self.open(remote_path, "rb") as fl:
        self.putfo(fl, remote_path2, file_size, callback, confirm)


# execute specific string of commands/command on the servers secure shell and return the result
def execute_command(cmd):
    lines = []
    stdin, stdout, stderr = ssh_client.exec_command(cmd, timeout=10)
    for line in stdout:  # received output
        lines.append(line)
    ssh_client.ssh_output = stdout.read()
    ssh_client.ssh_error = stderr.read()
    if ssh_client.ssh_error:
        print("Problem occurred while running command:" + cmd + " The error is " + ssh_client.ssh_error.decode("utf-8"))
        result_flag = False
    else:
        print("Command execution completed successfully", cmd)
    return lines


# defining a client and data for connecting
client = SSHClient.Client(password, user, ip, port)
# initializing clients connection to server
ssh_client = client.ssh_initialize()


if __name__ == "__main__":
    main()
