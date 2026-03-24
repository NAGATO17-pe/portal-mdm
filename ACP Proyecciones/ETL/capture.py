import subprocess
with open('error_log.txt', 'w', encoding='utf-8') as f:
    result = subprocess.run(['py', 'pipeline.py'], capture_output=True, text=True, encoding='utf-8', errors='replace')
    f.write(result.stdout or '')
    f.write(result.stderr or '')
