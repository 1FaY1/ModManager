import hashlib
import json
import re
import zipfile

try:
    import tomllib
except ModuleNotFoundError:
    tomllib = None

def get_file_hash(path):
    try:
        sha1 = hashlib.sha1()
        with open(path, 'rb') as f:
            while True:
                data = f.read(65536)
                if not data:
                    break
                sha1.update(data)
        return sha1.hexdigest()
    except Exception:
        return None


_MC_RE = re.compile(
    r"\b((?:2[0-9]\.\d{1,2}|1\.(?:1[4-9]|2\d))(?:\.\d{1,2})?)\b"
)


def _is_mc_version(v: str) -> bool:
    """True если строка выглядит как версия MC (не лоадера/мода)."""
    return bool(_MC_RE.fullmatch(v))


def _extract_versions(value):
    """
    Извлекает MC-версии из строки/списка/словаря.
    Игнорирует верхние границы диапазонов (перед ними стоит '<' или ',').
    """
    if not value:
        return set()
    if isinstance(value, str):
        result = set()
        for m in _MC_RE.finditer(value):
            start = m.start()
            trimmed_prefix = value[:start].rstrip()
            if trimmed_prefix.endswith("<") or trimmed_prefix.endswith(","):
                continue
            result.add(m.group(1))
        return result
    if isinstance(value, (list, tuple, set)):
        result = set()
        for item in value:
            result.update(_extract_versions(item))
        return result
    if isinstance(value, dict):
        result = set()
        for item in value.values():
            result.update(_extract_versions(item))
        return result
    return set()


def _parse_mods_toml(raw_bytes: bytes) -> tuple:
    """Разбирает mods.toml / neoforge.mods.toml (одинаковый TOML-формат).
    Возвращает (mod_ids, mc_versions)."""
    mod_ids = set()
    mc_versions = set()
    if tomllib is None:
        return mod_ids, mc_versions
    toml_data = tomllib.loads(raw_bytes.decode("utf-8", errors="ignore"))
    for mod_entry in toml_data.get("mods", []):
        mod_id = mod_entry.get("modId")
        if mod_id:
            mod_ids.add(str(mod_id).lower())
    dependencies = toml_data.get("dependencies", {})
    if isinstance(dependencies, dict):
        for dep_group in dependencies.values():
            for dep in dep_group:
                dep_id = str(dep.get("modId", "")).lower()
                if dep_id == "minecraft":
                    mc_versions.update(_extract_versions(dep.get("versionRange", "")))
    return mod_ids, mc_versions


def read_archive_metadata(path: str) -> dict:
    """
    Возвращает метаданные мода из jar/zip:
    - loaders: fabric/forge/neoforge/quilt
    - mc_versions: обнаруженные версии minecraft
    - mod_ids: id модов (для детекта адаптеров)
    """
    meta: dict = {"loaders": set(), "mc_versions": set(), "mod_ids": set()}
    if not zipfile.is_zipfile(path):
        return meta

    try:
        with zipfile.ZipFile(path, "r") as zf:
            names = set(zf.namelist())

            if "fabric.mod.json" in names:
                meta["loaders"].add("fabric")
                with zf.open("fabric.mod.json") as fp:
                    data = json.loads(fp.read().decode("utf-8", errors="ignore"))
                mod_id = data.get("id")
                if mod_id:
                    meta["mod_ids"].add(str(mod_id).lower())
                depends = data.get("depends", {})
                if isinstance(depends, dict):
                    if "minecraft" in depends:
                        meta["mc_versions"].update(_extract_versions(depends["minecraft"]))
                    if "fabricloader" in depends:
                        meta["loaders"].add("fabric")

            mods_toml_path = "META-INF/mods.toml"
            if mods_toml_path in names:
                meta["loaders"].add("forge")
                with zf.open(mods_toml_path) as fp:
                    mod_ids, mc_versions = _parse_mods_toml(fp.read())
                meta["mod_ids"].update(mod_ids)
                meta["mc_versions"].update(mc_versions)

            neoforge_toml_path = "META-INF/neoforge.mods.toml"
            if neoforge_toml_path in names:
                meta["loaders"].add("neoforge")
                with zf.open(neoforge_toml_path) as fp:
                    mod_ids, mc_versions = _parse_mods_toml(fp.read())
                meta["mod_ids"].update(mod_ids)
                meta["mc_versions"].update(mc_versions)
    except Exception:
        return meta

    return meta
