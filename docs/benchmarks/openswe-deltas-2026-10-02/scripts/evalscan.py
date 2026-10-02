"""Stream GAIR/OpenSWE openswe_oss.jsonl; keep eval data for selected tasks and git-usage stats for all."""
import json, re, sys, collections
sel = {t["task"] for t in json.load(open("tasks.json"))}
HD = re.compile(r"<<-?\s*'?(EOF[_A-Za-z0-9]*)'?\n.*?\n\1\n", re.S)
pats = {
    'git_apply': r'\bgit\s+apply\b',
    'git_checkout_rev_paths': r'\bgit\s+checkout\s+[0-9a-f]{7,40}\s+(--\s+)?\S',
    'git_checkout_any': r'\bgit\s+checkout\b',
    'git_reset': r'\bgit\s+reset\b',
    'git_diff': r'\bgit\s+diff\b',
    'git_status': r'\bgit\s+status\b',
    'git_stash': r'\bgit\s+stash\b',
    'git_log_show': r'\bgit\s+(log|show|rev-parse|describe|rev-list|blame)\b',
    'git_clean': r'\bgit\s+clean\b',
    'git_any': r'\bgit\s+\w',
    'patch_cmd': r'(^|\s)patch\s+-p',
    'pip_install': r'\bpip\s+install\b',
    'build_ext': r'build_ext|pip\s+install\s+(-e|--editable)|setup\.py\s+(develop|build)|meson|cmake|make\b',
    'pytest': r'\bpytest\b|-m\s+pytest',
    'network_fetch': r'\b(curl|wget|git\s+clone|git\s+fetch)\b',
}
pats = {k: re.compile(v, re.M) for k, v in pats.items()}
stats = collections.Counter(); n = 0; keep = {}
base_in_eval = 0
for line in sys.stdin:
    try:
        j = json.loads(line)
    except Exception:
        stats['bad_line'] += 1; continue
    n += 1
    e = j.get('eval_script') or ''
    body = HD.sub('<<HEREDOC>>\n', e)
    for k, p in pats.items():
        if p.search(body):
            stats[k] += 1
    if j.get('base_commit') and j['base_commit'] in body:
        base_in_eval += 1
    if j.get('FAIL_TO_PASS'): stats['has_f2p'] += 1
    if j.get('PASS_TO_PASS'): stats['has_p2p'] += 1
    if (j.get('install_config') or {}).get('test_cmd'): stats['has_test_cmd'] += 1
    if j.get('image_name') in sel:
        keep[j['image_name']] = {k: j.get(k) for k in ('instance_id', 'repo', 'base_commit', 'FAIL_TO_PASS', 'PASS_TO_PASS',
                                                       'install_config', 'eval_script', 'test_patch', 'patch', 'image_name')}
    if n % 2000 == 0:
        print(n, len(keep), file=sys.stderr, flush=True)
stats['rows'] = n; stats['base_commit_in_eval_body'] = base_in_eval
json.dump({'stats': stats}, open('evalscan-stats.json', 'w'), indent=1)
json.dump(keep, open('evalscan-selected.json', 'w'), indent=1)
print(json.dumps(stats), len(keep))
