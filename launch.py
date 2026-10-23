"""Mac launcher. Starts or opens the one local workbench process."""

import os
from pathlib import Path
import subprocess


def main():
    os.umask(0o077)
    project = Path(__file__).resolve().parent
    root = project.parents[1] / 'work' / 'telegram-workbench-runtime'
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    root.chmod(0o700)
    python = root / 'venv' / 'bin' / 'python'
    if not python.is_file():
        raise SystemExit('本机运行环境缺失，请回到 Codex 修复工作台安装。')
    with (root / 'server.log').open('a', encoding='utf-8') as log:
        subprocess.Popen([str(python), '-B', str(project / 'server.py')], cwd=project,
                         stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)


if __name__ == '__main__':
    main()
