import os
import signal
import subprocess

def find_an_kill():
    try:
        output = subprocess.check_output(["pgrep", "-f", "python"]).decode().strip()
        for pid in output.split('\n'):
            if pid and int(pid) != os.getpid():
                try:
                    os.kill(int(pid), signal.SIGKILL)
                    print(f"Killed {pid}")
                except Exception as e:
                    pass
    except Exception as e:
        print(e)

find_an_kill()
