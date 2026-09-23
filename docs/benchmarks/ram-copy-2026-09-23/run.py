import os,json,pathlib,subprocess,shutil
root=pathlib.Path('/home/ucloud/ram-copy-owned')
root.mkdir(mode=0o700)
img=root/'backing.img'
with img.open('xb') as f:f.truncate(4*1024**3)
loop=subprocess.check_output(['losetup','--find','--show','--direct-io=on',str(img)],text=True).strip()
xfs=root/'xfs';ram=root/'ram';xfs.mkdir();ram.mkdir()
mounted=[]
try:
 subprocess.run(['mkfs.xfs','-f',loop],check=True,stdout=subprocess.DEVNULL)
 subprocess.run(['mount','-t','xfs','-o','noatime',loop,str(xfs)],check=True);mounted.append(xfs)
 subprocess.run(['mount','-t','tmpfs','-o','size=1536m,noswap,mode=0700','ram-copy-qualification',str(ram)],check=True);mounted.append(ram)
 source=xfs/'memory.img'
 with source.open('xb') as f:
  for i in range(64):f.write(os.urandom(8*1024**2))
  f.truncate(1024**3);f.flush();os.fsync(f.fileno())
 subprocess.run(['fallocate','--punch-hole','--keep-size','--offset',str(512*1024**2),'--length',str(512*1024**2),str(source)],check=True)
 with source.open('rb') as src, (ram/'probe').open('xb') as dst:
  try:out={'copy_file_range_bytes':os.copy_file_range(src.fileno(),dst.fileno(),4096)}
  except OSError as e:out={'copy_file_range_errno':e.errno,'copy_file_range_error':str(e)}
  print(json.dumps(out),flush=True)
 (ram/'probe').unlink()
 for cache in ('warm','evicted'):
  for mode in ('buffer256k','buffer1m','sendfile','sendfile','buffer1m','buffer256k'):
   if cache=='evicted':
    with source.open('rb') as f:os.posix_fadvise(f.fileno(),0,0,os.POSIX_FADV_DONTNEED)
   cmd=['systemd-run','--quiet','--pipe','--wait','--collect','--unit=ram-copy-qualification','-p','CPUQuota=100%','-p','CPUQuotaPeriodSec=100ms','-p','MemoryMax=2G','/home/ucloud/ram-copy-benchmark',str(source),str(ram/'active.img'),mode]
   r=json.loads(subprocess.check_output(cmd,text=True));r['source_cache']=cache;print(json.dumps(r),flush=True)
finally:
 for mount in reversed(mounted):subprocess.run(['umount',str(mount)],check=True)
 actual=subprocess.check_output(['losetup','-n','-O','BACK-FILE',loop],text=True).strip()
 assert pathlib.Path(actual).resolve()==img.resolve()
 subprocess.run(['losetup','-d',loop],check=True)
 shutil.rmtree(root)
