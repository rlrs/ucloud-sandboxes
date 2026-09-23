import urllib.request,urllib.parse,json
queries={
 'seconds_by_phase':'sum by (phase,status) (increase(ucloud_platform_postgres_duration_seconds_sum[10m]))',
 'count_by_phase':'sum by (phase,status) (increase(ucloud_platform_postgres_duration_seconds_count[10m]))',
 'p95_by_phase':'histogram_quantile(0.95,sum by (phase,le,status) (increase(ucloud_platform_postgres_duration_seconds_bucket[10m])))',
 'seconds_by_operation':'sum by (operation,phase,status) (increase(ucloud_platform_postgres_duration_seconds_sum[10m]))',
}
for name,q in queries.items():
 with urllib.request.urlopen('http://127.0.0.1:8428/api/v1/query?'+urllib.parse.urlencode({'query':q,'time':'2026-09-23T16:00:00Z'}),timeout=10) as f:r=json.load(f)
 print(json.dumps({'name':name,'data':r}))
