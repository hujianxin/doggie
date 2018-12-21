import os
import subprocess


class PyBaseException(Exception):
    def __init__(self, message):
        self.message = message


class PyBase(object):
    def __init__(self, shell):
        self.__shell = shell
        if not os.path.exists(shell):
            raise PyBaseException("Wrong hbase shell path")

    def __do(self, cmd):
        with subprocess.Popen(["echo", cmd], stdout=subprocess.PIPE) as query:
            with subprocess.Popen(
                [self.__shell, "shell"], stdin=query.stdout, stdout=subprocess.PIPE
            ) as result:
                output = []
                line = result.stdout.readline()
                while line:
                    output.append(line.strip())
                    line = result.stdout.readline()
                result.stdout.close()
                return output

    def do(self):
        output = self.__do("list")
        print(output)
