import subprocess
import sys
import os
import re
from pathlib import Path


CHECKS = [
    [sys.executable, "-m", "py_compile", "server.py"],
    [sys.executable, "-m", "py_compile", "scripts/api_contract_smoke.py"],
    ["node", "--check", "analytics.js"],
    ["node", "--check", "app.js"],
    ["node", "--check", "crosswalk.js"],
    ["node", "--check", "research.js"],
    ["node", "--check", "scripts/browser_smoke.js"],
    [sys.executable, "-m", "unittest", "discover", "-s", "tests"],
]

FORBIDDEN_FRONTEND_SINKS = [
    (re.compile(r"\.innerHTML\b"), "innerHTML"),
    (re.compile(r"\.outerHTML\b"), "outerHTML"),
    (re.compile(r"\binsertAdjacentHTML\s*\("), "insertAdjacentHTML"),
    (re.compile(r"\beval\s*\("), "eval"),
    (re.compile(r"\bnew\s+Function\b"), "new Function"),
    (re.compile(r"\bdocument\.write(?:ln)?\s*\("), "document.write"),
    (re.compile(r"\b(?:localStorage|sessionStorage)\b"), "browser storage"),
    (re.compile(r"\bpostMessage\s*\("), "postMessage"),
]

FRONTEND_SCAN_FILES = [
    "app.js",
    "analytics.js",
    "crosswalk.js",
    "research.js",
]


def bundled_node_modules() -> Path:
    return Path.home() / ".cache" / "codex-runtimes" / "codex-primary-runtime" / "dependencies" / "node" / "node_modules"


def run_frontend_security_scan() -> int:
    failures = []
    for filename in FRONTEND_SCAN_FILES:
        path = Path(filename)
        if not path.exists():
            continue
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            for pattern, label in FORBIDDEN_FRONTEND_SINKS:
                if pattern.search(line):
                    failures.append(f"{filename}:{line_number}: forbidden frontend sink: {label}")
    if failures:
        print("\n".join(failures))
        return 1
    return 0


def main() -> int:
    for command in CHECKS:
        print(f"> {' '.join(command)}")
        result = subprocess.run(command)
        if result.returncode != 0:
            return result.returncode
    print("> frontend security sink scan")
    result = run_frontend_security_scan()
    if result != 0:
        return result
    node_modules = bundled_node_modules()
    if (node_modules / "playwright").exists():
        env = os.environ.copy()
        env["NODE_PATH"] = str(node_modules)
        command = ["node", "scripts/browser_smoke.js"]
        print(f"> {' '.join(command)}")
        result = subprocess.run(command, env=env)
        if result.returncode != 0:
            return result.returncode
    else:
        print("> skip browser smoke: bundled Playwright not available")
    if os.environ.get("RUN_LIVE_API_SMOKE") == "1":
        command = [sys.executable, "scripts/api_contract_smoke.py"]
        print(f"> {' '.join(command)}")
        result = subprocess.run(command)
        if result.returncode != 0:
            return result.returncode
    else:
        print("> skip live api smoke: set RUN_LIVE_API_SMOKE=1 when local server is running")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
