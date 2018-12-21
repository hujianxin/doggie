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

    def __str__(self):
        return "{} servers, {} dead, {} average load".format(
            self.__servers, self.__dead, self.__average_load
        )


class Version(object):
    def __init__(self, version, revision, date_time):
        self.__version = version
        self.__revision = revision
        self.__date_time = date_time

    @property
    def version(self):
        return self.__version

    @property
    def revision(self):
        return self.__revision

    @property
    def date_time(self):
        return self.__date_time

    def __str__(self):
        return "{}, {}, {}".format(self.__version, self.__revision, self.__date_time)


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
        pattern = re.compile(
            r"(\d+?) servers, (\d+?) dead, (\d+?)\.(\d+?) average load"
        )
        result = self.__do("status")
        for item in result:
            matched_item = pattern.match(item)
            if matched_item:
                servers = int(matched_item.group(1))
                dead = int(matched_item.group(2))
                average_load_1 = matched_item.group(3)
                average_load_2 = matched_item.group(4)
                average_load = float("{}.{}".format(average_load_1, average_load_2))
                return Status(servers, dead, average_load)
        raise PyBaseException("no result")

    def version(self):
        pattern = re.compile(
            r"(.*?), (.*?), (\S\S\S \S\S\S \d\d \d\d:\d\d:\d\d \w+? \d\d\d\d)"
        )
        result = self.__do("version")
        for item in result:
            matched_item = pattern.match(item)
            if matched_item:
                return Version(
                    matched_item.group(1), matched_item.group(2), matched_item.group(3)
                )
        raise PyBaseException("no result")

    def whoami(self):
        pattern = re.compile(r"(.*?)@(.*?)")
        result = self.__do("whoami")
        for item in result:
            matched_item = pattern.match(item)
            if matched_item:
                return matched_item.group(0)
        raise PyBaseException("no result")

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
