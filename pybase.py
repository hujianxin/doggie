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
    pass


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
        pass

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
