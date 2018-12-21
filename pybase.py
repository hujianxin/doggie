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
        query = subprocess.Popen(["echo", cmd], stdout=subprocess.PIPE)
        result = subprocess.Popen(
            [self.__shell, "shell"], stdin=query.stdout, stdout=subprocess.PIPE
        )
        query.stdout.close()
        output = []
        line = result.stdout.readline()
        start, stop = False, False
        while line:
            tmp = line.strip()
            if not start and not stop:
                if tmp == '':
                    start = True
                continue
            elif not stop:
                if tmp == '':
                    stop = True
                    continue
                output.append(line.strip())
                line = result.stdout.readline()
            if stop:
                break
        result.stdout.close()
        return output

    def do(self, cmd):
        output = self.__do(cmd)
        print(output)
