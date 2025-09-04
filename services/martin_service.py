import subprocess


def restart_martin():
    # 'reload' will invoke ExecReload (TERM => restart behavior); 'restart' is fine too.
    cmd = ["sudo", "/bin/systemctl", "restart", "martin"]
    try:
        out = subprocess.run(cmd, check=True, capture_output=True, text=True)
        return {"ok": True, "stdout": out.stdout}
    except subprocess.CalledProcessError as e:
        return {"ok": False, "stdout": e.stdout, "stderr": e.stderr, "code": e.returncode}
