import re
import os
import logging
import subprocess

logging.basicConfig()


class PyBaseException(Exception):
    def __init__(self, message):
        self.message = message


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
        start, stop = False, False
        while line:
            self.__logger.debug("Query content: " + line.strip())
            if not start and not stop:
                if line.strip() == "Commit Succeeded":
                    start = True
                    result.stdout.readline()
                    line = result.stdout.readline()
                continue
            elif not stop:
                if (
                    re.match(r"\d+? row\(s\) in \d+?\.\d+? seconds", line.strip())
                    is not None
                ):
                    stop = True
                    continue
            else:
                line = result.stdout.readline()
                output.append(line.strip())
            if stop:
                self.__logger.debug("Breaking at: " + line.strip())
                break
        result.stdout.close()
        return output

    def do(self, cmd):
        output = self.__do(cmd)
        print("Output: ", output)
