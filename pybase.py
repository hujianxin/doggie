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
        commit_succeeded, start, stop = False, False, False
        while line:
            content = line.strip()
            self.__logger.debug("Query content: " + content)
            if not start and not stop and not commit_succeeded:
                if content == 'Commit Succeeded':
                    commit_succeeded = True
                line = result.stdout.readline()
                continue
            elif not start and not stop:
                if content == '':
                    start = True
                    line = result.stdout.readline()
                    continue
            elif not stop:
                if re.match(r'\d row\(s\) in \d+?.\d+? seconds', content) is not None:
                    stop = True
                    continue
                line = result.stdout.readline()
                output.append(line.strip())
            if stop:
                self.__logger.debug("Breaking at: " + content)
                break
        result.stdout.close()
        return output

    def do(self, cmd):
        output = self.__do(cmd)
        print("Output: ", output)
