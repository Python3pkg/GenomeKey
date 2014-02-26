from cosmos.lib.ezflow.tool import Tool
import os

class Bunzip2(Tool):
    name = "Bunzip"
    mem_req = 4*1024
    cpu_req = 2
    time_req = 100
    inputs = ['bz2']
    outputs = ['*']

    def cmd(self,i,s,p):
        """
        """
        return 'bunzip2 -c {i[bz2]} > $OUT.*'

class Gunzip(Tool):
    inputs = ['gz']

    def cmd(self,i,s,p):
        return """gunzip {i[gz][0]}"""