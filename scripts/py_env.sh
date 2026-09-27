# scripts/py_env.sh — 공유 Python 인터프리터 선택 (source 전용, 실행 금지)
#
# 배경: homebrew python3가 3.14로 승격되면서 openpyxl/pandas 등 파이프라인
#       의존성이 기본 python3에서 사라짐(3.14 미설치). 의존성이 설치된
#       인터프리터를 찾아 PATH 맨 앞에 둬서 bare `python3` 호출이 그걸 잡게 한다.
# 사용: 각 러너가 repo 루트로 cd 한 뒤  `source scripts/py_env.sh`
for _pycand in \
    /Library/Frameworks/Python.framework/Versions/3.12/bin/python3 \
    /usr/local/bin/python3 \
    /opt/homebrew/bin/python3 \
    "$(command -v python3 2>/dev/null)"; do
    if [ -n "$_pycand" ] && [ -x "$_pycand" ] && "$_pycand" -c "import openpyxl, pandas" >/dev/null 2>&1; then
        export PATH="$(dirname "$_pycand"):$PATH"
        echo "  [py_env] Python 고정: $_pycand ($("$_pycand" --version 2>&1))"
        break
    fi
done
unset _pycand
