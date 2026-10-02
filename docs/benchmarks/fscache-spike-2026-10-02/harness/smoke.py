import json, sys, time
sys.path.insert(0, "/root/s11")
import s11lib as L
img = [i for i in json.load(open("/root/s11/images.json"))["images"] if i["name"] == sys.argv[2]][0]
cfg = L.image_config(img["prepared_reference"])[0]
print("workdir", (cfg.get("config") or {}).get("WorkingDir"), "env", (cfg.get("config") or {}).get("Env"))
if sys.argv[1] == "fscache":
    n = L.Nydusd(threads=8, log="nydusd-smoke.log"); n.start()
    mnt, t = L.fscache_attach(img["name"])
else:
    b = L.NbdBackend(); b.start(); f = L.NbdFrontend(); mnt, t = f.attach(img["prepared_reference"])
print("attach", t, mnt)
sb = L.Sandbox("smoke-" + sys.argv[1], mnt, cfg)
print("start", sb.start())
for label, argv, cwd in L.cold_commands(cfg):
    print(label, sb.exec(argv, cwd))
print("ls", sb.exec(["sh", "-c", "ls / | head; cat /etc/os-release | head -2; touch /tmp/x /root/y && echo wrote"]))
sb.stop()
L.unmount_all(L.MNT_DIR); L.unmount_all(L.ENVSTORE_ROOT); L.unmount_all(L.ENVIO_ROOT)
if sys.argv[1] == "fscache": n.kill(15)
else: b.kill()
