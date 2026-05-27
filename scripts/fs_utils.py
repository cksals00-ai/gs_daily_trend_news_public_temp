"""
fs_utils.py — macOS NFD 유니코드 파일명 정규화 유틸리티

macOS HFS+/APFS는 파일명을 NFD(분해형)로 저장하지만,
Python 문자열 리터럴은 NFC(조합형)이므로 한글 키워드 매칭이 실패함.

사용법 (스크립트 최상단에 한 줄 추가):
    import fs_utils  # 자동으로 os.listdir/os.scandir NFC 패치 적용

또는 개별 함수 사용:
    from fs_utils import nfc, safe_basename
"""
import os
import re
import sys
import unicodedata
from pathlib import Path


def nfc(s: str) -> str:
    """문자열을 NFC(조합형)로 정규화"""
    return unicodedata.normalize('NFC', s)


def safe_basename(filepath) -> str:
    """os.path.basename() + NFC 정규화"""
    return nfc(os.path.basename(str(filepath)))


def nfc_path_name(p) -> str:
    """Path 객체의 name을 NFC로 반환 (Path 자체는 변경하지 않음)"""
    return nfc(p.name) if hasattr(p, 'name') else nfc(str(p))


def nfc_match(pattern: str, filename) -> 're.Match | None':
    """NFC 정규화된 파일명에 대해 regex 매칭"""
    name = nfc(str(filename))
    return re.search(pattern, name)


# ─── 자동 패치: import fs_utils 만으로 적용 ───

_patched = False

def _apply_nfc_patch():
    """os.listdir 결과를 자동으로 NFC 정규화하는 monkey-patch"""
    global _patched
    if _patched:
        return
    _patched = True

    # 1) os.listdir 패치
    _orig_listdir = os.listdir
    def _nfc_listdir(path='.'):
        return [nfc(f) for f in _orig_listdir(path)]
    os.listdir = _nfc_listdir

    # 2) os.scandir 패치 — Path.glob/iterdir/walk 모두 이걸 경유
    _orig_scandir = os.scandir
    class _NFCDirEntry:
        """os.DirEntry 래퍼 — name만 NFC, 나머지는 원본 위임"""
        __slots__ = ('_entry', 'name')
        def __init__(self, entry):
            self._entry = entry
            self.name = nfc(entry.name)
        @property
        def path(self):
            # path도 NFC 정규화 (디렉터리 부분은 원본 유지하고 name만 교체)
            parent = os.path.dirname(self._entry.path)
            return os.path.join(parent, self.name)
        def is_dir(self, **kw):   return self._entry.is_dir(**kw)
        def is_file(self, **kw):  return self._entry.is_file(**kw)
        def is_symlink(self):     return self._entry.is_symlink()
        def stat(self, **kw):     return self._entry.stat(**kw)
        def inode(self):          return self._entry.inode()
        def __fspath__(self):     return self.path

    class _NFCScandirIterator:
        """os.scandir context manager 래퍼"""
        def __init__(self, path='.'):
            self._it = _orig_scandir(path)
        def __enter__(self):
            return self
        def __exit__(self, *args):
            self._it.close()
        def __iter__(self):
            for entry in self._it:
                yield _NFCDirEntry(entry)
        def close(self):
            self._it.close()

    def _nfc_scandir(path='.'):
        return _NFCScandirIterator(path)
    os.scandir = _nfc_scandir

    # 3) pathlib._NormalAccessor 패치 — pathlib은 os.listdir/scandir의
    #    C 함수 직접 참조를 갖고 있어서 os 모듈 패치만으로 부족
    try:
        import pathlib
        pathlib._NormalAccessor.listdir = staticmethod(_nfc_listdir)
        pathlib._NormalAccessor.scandir = staticmethod(_nfc_scandir)
    except (AttributeError, ImportError):
        pass  # Python 버전에 따라 _NormalAccessor가 없을 수 있음


# import 시 자동 적용
_apply_nfc_patch()
