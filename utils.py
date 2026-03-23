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


MC_VERSION_RE = re.compile(r"\d+\.\d+(?:\.\d+)?")


def _extract_versions(value):
    if not value:
        return set()
    if isinstance(value, str):
        return set(MC_VERSION_RE.findall(value))
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


def read_archive_metadata(path):
    """
    Возвращает метаданные мода из jar/zip:
    - loaders: fabric/forge/neoforge/quilt
    - mc_versions: обнаруженные версии minecraft
    - mod_ids: id модов (для детекта адаптеров)
    """
    meta = {"loaders": set(), "mc_versions": set(), "mod_ids": set()}
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
                if tomllib is not None:
                    with zf.open(mods_toml_path) as fp:
                        toml_data = tomllib.loads(fp.read().decode("utf-8", errors="ignore"))
                    for mod_entry in toml_data.get("mods", []):
                        mod_id = mod_entry.get("modId")
                        if mod_id:
                            meta["mod_ids"].add(str(mod_id).lower())
                    dependencies = toml_data.get("dependencies", {})
                    if isinstance(dependencies, dict):
                        for dep_group in dependencies.values():
                            for dep in dep_group:
                                dep_id = str(dep.get("modId", "")).lower()
                                if dep_id == "minecraft":
                                    meta["mc_versions"].update(_extract_versions(dep.get("versionRange", "")))

            if "META-INF/neoforge.mods.toml" in names:
                meta["loaders"].add("neoforge")
    except Exception:
        return meta

    return meta
