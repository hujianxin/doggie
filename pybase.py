import re
import os
import logging
import subprocess

logging.basicConfig()


class PyBaseException(Exception):
    def __init__(self, message):
        self.message = message


class Cell(object):
    pass


class Status(object):
    def __init__(self, servers, dead, average_load):
        self.__servers = servers
        self.__dead = dead
        self.__average_load = average_load

    @property
    def servers(self):
        return self.__servers

    @property
    def dead(self):
        return self.__dead

    @property
    def average_dead(self):
        return self.__average_load


class PyBase(object):
    def __init__(self, shell):
        self.__shell = shell
        if not os.path.exists(shell):
            raise PyBaseException("Wrong hbase shell path")
        self.__logger = logging.getLogger(__name__)
        self.__logger.setLevel(logging.DEBUG)

    def __do(self, cmd):
        query = subprocess.Popen(["echo", cmd], stdout=subprocess.PIPE)
        result = subprocess.Popen(
            [self.__shell, "shell"], stdin=query.stdout, stdout=subprocess.PIPE
        )
        query.stdout.close()
        output = []
        line = result.stdout.readline()
        while line:
            output.append(line.strip())
            line = result.stdout.readline()
        result.stdout.close()
        return output

    def do(self, cmd):
        return self.__do(cmd)

    # -------------general------------- #
    def status(self):
        status_pattern = re.compile(
            r"(\d+?) servers, (\d+?) dead, (\d+?)\.(\d+?) average load"
        )
        status_cmd = 'status'
        result = self.__do(status_cmd)
        for item in result:
            matched_item = status_pattern.match(item)
            if matched_item:
                servers = int(matched_item.group(1))
                dead = int(matched_item.group(2))
                average_load_1 = matched_item(3)
                average_load_2 = matched_item(4)
                average_load = float("{}.{}".format(average_load_1, average_load_2))
                return Status(servers, dead, average_load)

    def version(self):
        pass

    def whoami(self):
        pass

    # -------------dml------------- #
    def scan(self, cmd):
        output = self.__do(cmd)
        print("Output: ", output)

    def get(self):
        pass

    def put(self):
        pass

    # -------------tools------------- #
    # -------------ddl------------- #
    # -------------namespace------------- #
    # -------------snapshot------------- #
    # -------------replication------------- #
    # -------------quotas------------- #
    # -------------security------------- #
    # -------------visibility labels------------- #
