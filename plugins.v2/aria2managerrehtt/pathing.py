from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple


STORAGE_SCHEMAS = {"local", "alipan", "u115", "rclone", "alist", "smb"}


def strip_storage_prefix(path: str) -> str:
    text = str(path or "")
    prefix, separator, remainder = text.partition(":")
    if separator and prefix.lower() in STORAGE_SCHEMAS:
        return remainder
    return text


def replace_path_prefix(path: str, source: str, target: str) -> Optional[str]:
    if not str(path or "").strip():
        return None
    path_text = Path(strip_storage_prefix(path)).as_posix()
    source_text = Path(strip_storage_prefix(source.strip())).as_posix()
    target_text = Path(strip_storage_prefix(target.strip())).as_posix()
    if not source_text or not target_text:
        return None
    if path_text == source_text:
        return target_text
    prefix = f"{source_text.rstrip('/')}/"
    if path_text.startswith(prefix):
        suffix = path_text[len(prefix):]
        return (Path(target_text) / suffix).as_posix()
    return None


class PathMapper:
    def __init__(self, mappings: Sequence[Tuple[str, str]] = ()):
        self.mappings = tuple(mappings or ())

    def to_downloader(self, path: Path | str) -> str:
        normalized = strip_storage_prefix(str(path))
        if not normalized:
            return ""
        for moviepilot_path, downloader_path in self.mappings:
            mapped = replace_path_prefix(
                normalized, moviepilot_path, downloader_path
            )
            if mapped:
                return mapped
        return Path(normalized).as_posix()

    def to_moviepilot(self, path: Path | str) -> str:
        normalized = strip_storage_prefix(str(path))
        if not normalized:
            return ""
        for moviepilot_path, downloader_path in self.mappings:
            mapped = replace_path_prefix(
                normalized, downloader_path, moviepilot_path
            )
            if mapped:
                return mapped
        return Path(normalized).as_posix()


@dataclass
class PreparedDeletion:
    paths: List[Path] = field(default_factory=list)
    roots: List[Path] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def valid(self) -> bool:
        return not self.errors


@dataclass
class DeleteResult:
    success: bool
    deleted: List[str] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


class SafeFileDeleter:
    def __init__(self, allowed_roots: Iterable[Path | str]):
        roots: List[Path] = []
        for value in allowed_roots or []:
            text = strip_storage_prefix(str(value or "")).strip()
            if not text:
                continue
            resolved = Path(text).resolve(strict=False)
            if resolved == Path(resolved.anchor):
                continue
            if resolved not in roots:
                roots.append(resolved)
        self.allowed_roots = roots

    @staticmethod
    def _under_root(path: Path, root: Path) -> bool:
        try:
            path.relative_to(root)
            return path != root
        except ValueError:
            return False

    def prepare(self, paths: Iterable[Path | str]) -> PreparedDeletion:
        prepared = PreparedDeletion(roots=list(self.allowed_roots))
        if not self.allowed_roots:
            prepared.errors.append("未配置可用的MoviePilot本地下载根目录")
            return prepared
        candidates: List[Path] = []
        for value in paths or []:
            text = strip_storage_prefix(str(value or "")).strip()
            if not text:
                continue
            candidate = Path(text)
            if not candidate.is_absolute():
                prepared.errors.append(f"拒绝删除非绝对路径：{candidate}")
                continue
            resolved = candidate.resolve(strict=False)
            if not any(self._under_root(resolved, root) for root in self.allowed_roots):
                prepared.errors.append(f"路径不在允许的下载目录内：{candidate}")
                continue
            if candidate not in candidates:
                candidates.append(candidate)
        prepared.paths = candidates
        return prepared

    def execute(self, prepared: PreparedDeletion) -> DeleteResult:
        if not prepared.valid:
            return DeleteResult(success=False, errors=list(prepared.errors))
        deleted: List[str] = []
        skipped: List[str] = []
        errors: List[str] = []
        parents: List[Path] = []
        for path in prepared.paths:
            try:
                resolved = path.resolve(strict=False)
                if not any(
                    self._under_root(resolved, root)
                    for root in prepared.roots
                ):
                    errors.append(f"删除前路径校验失败：{path}")
                    continue
                if not path.exists() and not path.is_symlink():
                    skipped.append(path.as_posix())
                    continue
                if not path.is_file() and not path.is_symlink():
                    errors.append(f"拒绝删除非文件路径：{path}")
                    continue
                path.unlink()
                deleted.append(path.as_posix())
                parents.append(path.parent)
            except OSError as err:
                errors.append(f"删除文件失败 {path}: {err}")

        for parent in sorted(
            set(parents), key=lambda item: len(item.parts), reverse=True
        ):
            current = parent
            while any(self._under_root(current, root) for root in prepared.roots):
                try:
                    if current.is_symlink():
                        break
                    current.rmdir()
                except OSError:
                    break
                current = current.parent
        return DeleteResult(
            success=not errors,
            deleted=deleted,
            skipped=skipped,
            errors=errors,
        )
