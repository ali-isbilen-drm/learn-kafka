import platform
import subprocess

OS_NAME = platform.system()


def run(cmd, ignore_error=True):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True)


if __name__ == "__main__":
    print("=== Stopping traccar-kafka-lab ===")

    print("[1/3] Stopping Traccar service...")
    if OS_NAME == "Windows":
        res = run('powershell -Command "Stop-Service traccar"')
        if res.returncode != 0:
            print("  [!] Run in an Admin PowerShell if Traccar is still running: Stop-Service traccar")
    elif OS_NAME == "Linux":
        run("sudo systemctl stop traccar")
    elif OS_NAME == "Darwin":
        run("brew services stop traccar")

    print("[2/3] Stopping Kafka containers...")
    run("podman stop kafka-ui")
    run("podman stop kafka")

    if OS_NAME in ("Windows", "Darwin"):
        print("[3/3] Stopping Podman VM to reclaim RAM...")
        run("podman machine stop")
    else:
        print("[3/3] Native Linux — Podman VM shutdown not needed.")

    print("\nShutdown complete.")