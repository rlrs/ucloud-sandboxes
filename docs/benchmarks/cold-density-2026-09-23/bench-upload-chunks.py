from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from threading import Thread
from io import BytesIO
import hashlib,json,time,urllib3,os
from ucloud_sandboxes.http_server import RequestBodyStream
body=os.urandom(4*1024*1024);expected=hashlib.sha256(body).hexdigest().encode()
class Handler(BaseHTTPRequestHandler):
 def do_PUT(self):
  left=int(self.headers['Content-Length']);h=hashlib.sha256()
  while left:
   chunk=self.rfile.read(min(left,65536));assert chunk;h.update(chunk);left-=len(chunk)
  result=h.hexdigest().encode();self.send_response(200);self.send_header('Content-Length',str(len(result)));self.end_headers();self.wfile.write(result)
 def log_message(self,*args):pass
class Server(ThreadingHTTPServer):request_queue_size=4096
class Reader(RequestBodyStream):
 calls=0
 def read(self,*args):self.calls+=1;return super().read(*args)
server=Server(('127.0.0.1',0),Handler);Thread(target=server.serve_forever,daemon=True).start()
for size in [16384,65536,65536,16384]:
 pool=urllib3.PoolManager(maxsize=64,block=True,retries=False,blocksize=size)
 def run(i):
  source=Reader(BytesIO(body),len(body));r=pool.request('PUT',f'http://127.0.0.1:{server.server_port}/file',body=source,headers={'Content-Length':str(len(body)),'Connection':'close'},retries=False);assert r.data==expected;return source.calls
 start=time.monotonic();cpu=time.process_time()
 with ThreadPoolExecutor(max_workers=32) as ex:counts=list(ex.map(run,range(128)))
 print(json.dumps(dict(blocksize=size,seconds=time.monotonic()-start,cpu_seconds=time.process_time()-cpu,body_reads=sum(counts))),flush=True);pool.clear()
server.shutdown();server.server_close()
