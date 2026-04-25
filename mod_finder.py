import sys
import os
import json
import concurrent.futures
import shutil
import logging
import time
import random
import re
import importlib.util
import subprocess
from datetime import datetime, timezone
from collections import Counter, defaultdict
from functools import partial

from utils import get_file_hash, read_archive_metadata

REQUIRED_PACKAGES = {
    "requests": "requests>=2.31",
    "PyQt6": "PyQt6>=6.6",
}


def ensure_runtime_dependencies():
    missing = [pkg for module_name, pkg in REQUIRED_PACKAGES.items() if importlib.util.find_spec(module_name) is None]
    if not missing:
        return

    print(f"[ModManager] Не найдены зависимости: {', '.join(missing)}. Устанавливаю автоматически...")
    pip_cmd = [sys.executable, "-m", "pip", "install", *missing]
    subprocess.check_call(pip_cmd)


ensure_runtime_dependencies()

import requests
from PyQt6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout,
    QLineEdit, QPushButton, QComboBox, QTableWidget,
    QTableWidgetItem, QFileDialog, QMessageBox,
    QLabel, QProgressBar, QHeaderView, QDialog, QAbstractItemView,
    QToolButton, QMenu
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QColor, QIcon, QAction

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("app.log", encoding="utf-8"), logging.StreamHandler()]
)


def resource_path(relative_path):
    """ Функция для поиска иконки внутри собранного EXE """
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)


VERSION = "1.8"
MODRINTH_API = "https://api.modrinth.com/v2"
HEADERS = {"User-Agent": f"MyMinecraftManager/{VERSION}"}
WORKER_THREADS = 8
CONFIG_FILE = "mod_manager_config.json"
MAX_RETRIES = 3
RETRYABLE_STATUS_CODES = {429, 503}
ADAPTER_ID_MARKERS = {"connector", "sinytra_connector"}
ADAPTER_NAME_MARKERS = ("sinytra", "connector")
SCAN_TYPE_RULES = {
    "Моды": {"subdir": "mods", "extensions": (".jar",), "project_type": "mod"},
    "Шейдеры": {"subdir": "shaderpacks", "extensions": (".zip",), "project_type": "shader"},
    "Текстур-паки": {"subdir": "resourcepacks", "extensions": (".zip",), "project_type": "resourcepack"},
}
PROJECT_TYPE_LOADER_LABELS = {
    "mod": "Загрузчик:",
    "shader": "Движок:",
    "resourcepack": "Платформа:"
}
_VERSION_RE = re.compile(r"(\d+|[a-zA-Z]+)")


def _version_key(raw_version):
    if not raw_version:
        return ()
    parts = []
    for part in _VERSION_RE.findall(raw_version):
        if part.isdigit():
            parts.append((0, int(part)))
        else:
            parts.append((1, part.lower()))
    return tuple(parts)


def is_version_newer(latest_version, current_version):
    return _version_key(latest_version) > _version_key(current_version)


def discover_scan_targets(selected_path):
    selected_path = os.path.abspath(selected_path)
    targets = []
    has_known_subdirs = False
    for title, rule in SCAN_TYPE_RULES.items():
        candidate = os.path.join(selected_path, rule["subdir"])
        if os.path.isdir(candidate):
            has_known_subdirs = True
            targets.append({
                "title": title,
                "folder": candidate,
                "extensions": rule["extensions"],
                "project_type": rule["project_type"]
            })

    if has_known_subdirs:
        return targets, selected_path

    folder_name = os.path.basename(selected_path).lower()
    for title, rule in SCAN_TYPE_RULES.items():
        if folder_name == rule["subdir"]:
            return [{
                "title": title,
                "folder": selected_path,
                "extensions": rule["extensions"],
                "project_type": rule["project_type"]
            }], os.path.dirname(selected_path)

    return [{
        "title": "Моды",
        "folder": selected_path,
        "extensions": (".jar",),
        "project_type": "mod"
    }], os.path.dirname(selected_path)


def request_with_retry(session, method, url, *, max_retries=MAX_RETRIES, retry_statuses=None, **kwargs):
    retry_statuses = retry_statuses or RETRYABLE_STATUS_CODES
    last_response = None
    last_exception = None

    for attempt in range(max_retries):
        try:
            response = session.request(method, url, **kwargs)
            last_response = response
            if response.status_code not in retry_statuses:
                return response

            retry_after = response.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                sleep_s = min(float(retry_after), 5.0)
            else:
                sleep_s = (0.4 * (2 ** attempt)) + random.uniform(0, 0.2)
            time.sleep(sleep_s)
        except requests.RequestException as exc:
            last_exception = exc
            sleep_s = (0.4 * (2 ** attempt)) + random.uniform(0, 0.2)
            time.sleep(sleep_s)

    if last_response is None and last_exception is not None:
        raise last_exception
    return last_response


def _install_global_exception_hook():
    original_hook = sys.excepthook

    def handle_exception(exc_type, exc_value, exc_traceback):
        logging.exception("Необработанная ошибка приложения", exc_info=(exc_type, exc_value, exc_traceback))
        try:
            QMessageBox.critical(
                None,
                "Критическая ошибка",
                "Произошла непредвиденная ошибка.\n"
                "Подробности записаны в app.log."
            )
        except Exception:
            pass
        original_hook(exc_type, exc_value, exc_traceback)

    sys.excepthook = handle_exception


class DownloadThread(QThread):
    progress = pyqtSignal(int)
    finished = pyqtSignal(str)
    error = pyqtSignal(str)

    def __init__(self, url, save_path):
        super().__init__()
        self.url, self.save_path = url, save_path
        self.session = requests.Session()

    def run(self):
        try:
            with request_with_retry(
                self.session,
                "GET",
                self.url,
                stream=True,
                headers=HEADERS,
                timeout=20
            ) as r:
                r.raise_for_status()
                total = int(r.headers.get("content-length", 0))
                downloaded = 0
                with open(self.save_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            if total: self.progress.emit(int(downloaded * 100 / total))
            self.finished.emit(self.save_path)
        except Exception as e:
            self.error.emit(str(e))


class ModSearchWorker(QThread):
    results_ready = pyqtSignal(list, bool)

    def __init__(self, query, loader, mc_ver, project_type="mod"):
        super().__init__()
        self.query, self.loader, self.mc_ver = query.strip(), loader, mc_ver
        self.project_type = (project_type or "mod").strip().lower()
        self.session = requests.Session()

    def run(self):
        try:
            clean_loader = self.loader.strip().lower()
            clean_ver = self.mc_ver.strip()
            loader_filter = clean_loader if clean_loader and clean_loader != "авто" else ""
            version_filter = clean_ver if clean_ver and clean_ver != "Авто" else ""

            facets = [f'["project_type:{self.project_type}"]']
            if loader_filter:
                facets.append(f'["categories:{loader_filter}"]')
            if version_filter:
                facets.append(f'["versions:{version_filter}"]')

            params = {
                "query": f'"{self.query}"',
                "limit": 25,
                "index": "relevance",
                "facets": f"[{','.join(facets)}]"
            }

            r = request_with_retry(
                self.session,
                "GET",
                f"{MODRINTH_API}/search",
                params=params,
                headers=HEADERS,
                timeout=10
            )
            r.raise_for_status()
            hits = r.json().get("hits", [])

            def sorting_key(hit):
                title = hit['title'].strip().lower()
                query = self.query.strip().lower()

                if title == query:
                    return 0
                if title.startswith(query):
                    return 1
                return 2

            hits.sort(key=sorting_key)

            hits = hits[:15]
            results = []

            def fetch_ver(hit):
                v_params = {}
                if loader_filter:
                    v_params["loaders"] = f'["{loader_filter}"]'
                if version_filter:
                    v_params["game_versions"] = f'["{version_filter}"]'

                try:
                    vr = request_with_retry(
                        self.session,
                        "GET",
                        f"{MODRINTH_API}/project/{hit['project_id']}/version",
                        params=v_params,
                        headers=HEADERS,
                        timeout=5
                    )
                    if vr.status_code == 200:
                        versions = vr.json()
                        if versions:
                            selected_v = None

                            for v in versions:
                                if v.get("version_type") == "release":
                                    selected_v = v
                                    break

                            if not selected_v:
                                for v in versions:
                                    if v.get("version_type") == "beta":
                                        selected_v = v
                                        break

                            if not selected_v:
                                selected_v = versions[0]

                            files = selected_v.get("files") or []
                            if not files:
                                return None
                            primary_file = files[0]

                            return {
                                "title": hit["title"],
                                "author": hit["author"],
                                "version": selected_v["version_number"],
                                "project_id": hit["project_id"],
                                "source_url": f"https://modrinth.com/project/{hit['project_id']}",
                                "url": primary_file.get("url", ""),
                                "filename": primary_file.get("filename", ""),
                                "status": "Доступен",
                                "needs_update": False
                            }
                except (requests.RequestException, ValueError, KeyError) as e:
                    logging.error("Ошибка получения версии: %s", e)
                return None

            with concurrent.futures.ThreadPoolExecutor(max_workers=WORKER_THREADS) as ex:
                futures = [ex.submit(fetch_ver, h) for h in hits]
                for f in concurrent.futures.as_completed(futures):
                    res = f.result()
                    if res:
                        results.append(res)

            self.results_ready.emit(results, True)

        except (requests.RequestException, ValueError, KeyError) as e:
            logging.error("Search error: %s", e)
            self.results_ready.emit([], False)


class FolderScannerWorker(QThread):
    progress = pyqtSignal(int)
    result_ready = pyqtSignal(dict)
    mod_found = pyqtSignal(dict)
    finished = pyqtSignal()

    def __init__(self, targets, loader, game_version, check_updates=False):
        super().__init__()
        self.targets = targets or []
        self.loader = (loader or "").strip().lower()
        self.mc_ver = (game_version or "").strip()
        self.check_updates = check_updates
        self.session = requests.Session()

    def run(self):
        if not self.targets:
            self.finished.emit()
            return

        hash_to_entries = {}
        adapter_present = False
        detected_versions = set()
        detected_loaders = set()
        loader_counter = Counter()
        version_counter = Counter()

        for target in self.targets:
            folder = target["folder"]
            if not os.path.exists(folder):
                continue
            extensions = tuple(ext.lower() for ext in target["extensions"])
            for f in os.listdir(folder):
                if not f.lower().endswith(extensions):
                    continue
                path = os.path.join(folder, f)
                if not os.path.isfile(path):
                    continue
                f_hash = get_file_hash(path)
                if not f_hash:
                    continue
                entry = {"filename": f, "folder": folder, "target": target}
                hash_to_entries.setdefault(f_hash, []).append(entry)
                if f.lower().endswith(".jar"):
                    archive_meta = read_archive_metadata(path)
                    detected_versions.update(archive_meta.get("mc_versions", set()))
                    detected_loaders.update(archive_meta.get("loaders", set()))
                    loader_counter.update(archive_meta.get("loaders", set()))
                    version_counter.update(archive_meta.get("mc_versions", set()))
                    mod_ids = archive_meta.get("mod_ids", set())
                    if mod_ids.intersection(ADAPTER_ID_MARKERS):
                        adapter_present = True
                    lower_name = f.lower()
                    if all(marker in lower_name for marker in ADAPTER_NAME_MARKERS):
                        adapter_present = True

        if not hash_to_entries:
            self.finished.emit()
            return

        try:
            r = request_with_retry(
                self.session,
                "POST",
                f"{MODRINTH_API}/version_files",
                json={"hashes": list(hash_to_entries.keys()), "algorithm": "sha1"},
                headers=HEADERS, timeout=15
            )
            if r.status_code == 200:
                recognized = r.json()

                def get_project_title(project_id):
                    try:
                        p_res = request_with_retry(
                            self.session,
                            "GET",
                            f"{MODRINTH_API}/project/{project_id}",
                            headers=HEADERS,
                            timeout=8
                        )
                        if p_res.status_code == 200:
                            return p_res.json().get("title") or project_id
                    except requests.RequestException as exc:
                        logging.warning("Не удалось получить title проекта %s: %s", project_id, exc)
                    return project_id

                project_loader_hints = defaultdict(set)
                project_version_hints = defaultdict(set)
                for data in recognized.values():
                    pid = data.get("project_id")
                    if not pid:
                        continue
                    for loader_name in data.get("loaders", []) or []:
                        project_loader_hints[pid].add(loader_name)
                    for game_ver in data.get("game_versions", []) or []:
                        project_version_hints[pid].add(game_ver)

                def find_latest_release(project_id, project_type):
                    params = {}
                    primary_loader = loader_counter.most_common(1)[0][0] if loader_counter else None
                    primary_version = version_counter.most_common(1)[0][0] if version_counter else None
                    hint_loaders = project_loader_hints.get(project_id, set())
                    hint_versions = project_version_hints.get(project_id, set())

                    loaders_for_query = set()
                    if self.loader != "авто":
                        loaders_for_query.add(self.loader)
                    else:
                        if hint_loaders:
                            loaders_for_query.update(hint_loaders)
                        elif project_type == "mod" and primary_loader:
                            loaders_for_query.add(primary_loader)
                        elif project_type == "mod":
                            loaders_for_query.update(detected_loaders)
                    if project_type == "mod" and adapter_present:
                        loaders_for_query.update({"fabric", "forge"})
                    loaders_for_query = {ldr for ldr in loaders_for_query if ldr}
                    if loaders_for_query:
                        params["loaders"] = json.dumps(sorted(loaders_for_query))

                    versions_for_query = set()
                    if self.mc_ver != "Авто":
                        versions_for_query.add(self.mc_ver)
                    else:
                        if hint_versions:
                            versions_for_query.update(hint_versions)
                        elif project_type == "mod" and primary_version:
                            versions_for_query.add(primary_version)
                        elif project_type == "mod":
                            versions_for_query.update(detected_versions)
                    if versions_for_query:
                        params["game_versions"] = json.dumps(sorted(versions_for_query))

                    vr = request_with_retry(
                        self.session,
                        "GET",
                        f"{MODRINTH_API}/project/{project_id}/version",
                        params=params if params else None,
                        headers=HEADERS,
                        timeout=10
                    )
                    vr.raise_for_status()
                    versions = vr.json()
                    if not versions and params:
                        vr = request_with_retry(
                            self.session,
                            "GET",
                            f"{MODRINTH_API}/project/{project_id}/version",
                            headers=HEADERS,
                            timeout=10
                        )
                        vr.raise_for_status()
                        versions = vr.json()
                    if not versions:
                        return None

                    def pick_best_version(candidates):
                        if not candidates:
                            return None
                        return max(candidates, key=lambda item: item.get("date_published") or "")

                    release_versions = [v for v in versions if v.get("version_type") == "release"]
                    beta_versions = [v for v in versions if v.get("version_type") == "beta"]
                    selected = (
                        pick_best_version(release_versions)
                        or pick_best_version(beta_versions)
                        or pick_best_version(versions)
                    )

                    version_files = selected.get("files") or []
                    if not version_files:
                        return None
                    return {
                        "version": selected.get("version_number", "—"),
                        "url": version_files[0].get("url"),
                        "filename": version_files[0].get("filename")
                    }

                project_ids = {
                    data.get("project_id") for data in recognized.values() if data.get("project_id")
                }
                project_type_map = {}
                for data in recognized.values():
                    pid = data.get("project_id")
                    ptype = data.get("project_type")
                    if pid and ptype:
                        project_type_map[pid] = ptype
                latest_map = {}
                title_map = {}

                if self.check_updates and project_ids:
                    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKER_THREADS) as ex:
                        latest_futures = {
                            ex.submit(find_latest_release, pid, project_type_map.get(pid, "mod")): pid
                            for pid in project_ids
                        }
                        for future in concurrent.futures.as_completed(latest_futures):
                            pid = latest_futures[future]
                            try:
                                latest_map[pid] = future.result()
                            except Exception as e:
                                logging.error("Ошибка получения последней версии %s: %s", pid, e)
                                latest_map[pid] = None

                        title_futures = {ex.submit(get_project_title, pid): pid for pid in project_ids}
                        for future in concurrent.futures.as_completed(title_futures):
                            pid = title_futures[future]
                            try:
                                title_map[pid] = future.result()
                            except (requests.RequestException, ValueError, KeyError):
                                title_map[pid] = pid

                scanned_project_ids = set()
                installed_project_ids = {
                    data.get("project_id") for data in recognized.values() if data.get("project_id")
                }
                known_version_project_map = {
                    data.get("id"): data.get("project_id")
                    for data in recognized.values()
                    if data.get("id") and data.get("project_id")
                }
                resolved_dependency_version_map = {}

                for f_hash, v_data in recognized.items():
                    project_id = v_data.get("project_id")
                    current_version = v_data.get("version_number", "—")
                    matched_entries = hash_to_entries.get(f_hash, [])
                    latest_data = latest_map.get(project_id) if project_id else None

                    for entry in matched_entries:
                        filename = entry["filename"]
                        folder = entry["folder"]
                        scan_title = entry["target"]["title"]
                        needs_update = False
                        status = "Загружен"
                        out_version = current_version
                        out_url = None
                        out_filename = filename

                        if self.check_updates:
                            if latest_data and latest_data.get("version"):
                                latest_version = latest_data["version"]
                                out_version = latest_version
                                out_url = latest_data.get("url")
                                out_filename = latest_data.get("filename") or filename
                                if latest_version != current_version:
                                    needs_update = True
                                    status = f"Обновление: {current_version} → {latest_version}"
                                else:
                                    status = "Актуально"
                            else:
                                status = "Не удалось проверить"

                        if self.check_updates and project_id:
                            title = title_map.get(project_id, project_id)
                        else:
                            title = filename

                        mod_info = {
                            "title": title,
                            "scan_type": scan_title,
                            "version": out_version,
                            "status": status,
                            "project_id": project_id,
                            "source_url": f"https://modrinth.com/project/{project_id}" if project_id else "",
                            "filename": out_filename,
                            "display_name": filename,
                            "source_folder": folder,
                            "url": out_url,
                            "needs_update": needs_update,
                        }
                        self.result_ready.emit(mod_info)
                        self.mod_found.emit(mod_info)

                    dependencies = v_data.get("dependencies") or []
                    for dependency in dependencies:
                        if dependency.get("dependency_type") != "required":
                            continue
                        dependency_project_id = self._resolve_dependency_project_id(
                            dependency,
                            known_version_project_map,
                            resolved_dependency_version_map
                        )
                        if not dependency_project_id:
                            continue
                        if dependency_project_id in installed_project_ids:
                            continue
                        if dependency_project_id in scanned_project_ids:
                            continue
                        dep_data = self._fetch_dependency_data(dependency_project_id)
                        if dep_data:
                            scanned_project_ids.add(dependency_project_id)
                            self.result_ready.emit(dep_data)
                            self.mod_found.emit(dep_data)
        except (requests.RequestException, ValueError, OSError) as e:
            logging.error(f"Ошибка сканирования: {e}")

        self.finished.emit()

    def _fetch_dependency_data(self, project_id):
        try:
            project_response = request_with_retry(
                self.session,
                "GET",
                f"{MODRINTH_API}/project/{project_id}",
                headers=HEADERS,
                timeout=8
            )
            if project_response.status_code != 200:
                return None
            project_data = project_response.json()

            params = {}
            if self.loader and self.loader != "авто":
                params["loaders"] = json.dumps([self.loader])
            if self.mc_ver and self.mc_ver != "Авто":
                params["game_versions"] = json.dumps([self.mc_ver])

            version_response = request_with_retry(
                self.session,
                "GET",
                f"{MODRINTH_API}/project/{project_id}/version",
                params=params if params else None,
                headers=HEADERS,
                timeout=10
            )
            if version_response.status_code != 200:
                return None
            versions = version_response.json()
            if not versions and params:
                version_response = request_with_retry(
                    self.session,
                    "GET",
                    f"{MODRINTH_API}/project/{project_id}/version",
                    headers=HEADERS,
                    timeout=10
                )
                if version_response.status_code != 200:
                    return None
                versions = version_response.json()
            if not versions:
                return None

            selected_version = next((v for v in versions if v.get("version_type") == "release"), None)
            if not selected_version:
                selected_version = next((v for v in versions if v.get("version_type") == "beta"), None)
            if not selected_version:
                selected_version = versions[0]

            files = selected_version.get("files") or []
            if not files:
                return None
            primary_file = next((f for f in files if f.get("primary")), files[0])

            project_slug = project_data.get("slug") or project_id
            return {
                "title": project_data.get("title") or project_id,
                "version": selected_version.get("version_number", "—"),
                "url": primary_file.get("url"),
                "filename": primary_file.get("filename"),
                "project_id": project_id,
                "source_url": f"https://modrinth.com/project/{project_slug}",
                "status": "Требуется установка",
                "is_dependency": True,
                "is_missing_dependency": True,
                "needs_update": False
            }
        except (requests.RequestException, ValueError, KeyError) as e:
            logging.error("Ошибка при получении зависимости %s: %s", project_id, e)
            return None

    def _resolve_dependency_project_id(self, dependency, known_map, cache):
        project_id = dependency.get("project_id")
        if project_id:
            return project_id

        version_id = dependency.get("version_id")
        if not version_id:
            return None
        if version_id in known_map:
            return known_map[version_id]
        if version_id in cache:
            return cache[version_id]

        try:
            response = request_with_retry(
                self.session,
                "GET",
                f"{MODRINTH_API}/version/{version_id}",
                headers=HEADERS,
                timeout=8
            )
            if response.status_code != 200:
                cache[version_id] = None
                return None
            version_data = response.json()
            resolved_project_id = version_data.get("project_id")
            cache[version_id] = resolved_project_id
            return resolved_project_id
        except (requests.RequestException, ValueError, KeyError) as e:
            logging.error("Ошибка определения project_id зависимости %s: %s", version_id, e)
            cache[version_id] = None
            return None


class FolderSelectDialog(QDialog):
    folder_selected = pyqtSignal(str)

    def __init__(self, title="Выбор папки", parent=None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setFixedSize(350, 200)
        self.setAcceptDrops(True)
        layout = QVBoxLayout()
        self.lbl = QLabel(text="Перетащите папку сюда\nили нажмите кнопку")
        self.lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        btn = QPushButton("Открыть проводник")
        btn.clicked.connect(self.browse)
        layout.addWidget(self.lbl)
        layout.addWidget(btn)
        self.setLayout(layout)
        self.setStyleSheet("QLabel { border: 2px dashed #aaa; padding: 20px; }")

    def browse(self):
        f = QFileDialog.getExistingDirectory(self, "Выбрать папку")
        if f: self.folder_selected.emit(f); self.accept()

    def dragEnterEvent(self, e):
        if e.mimeData().hasUrls(): e.accept()

    def dropEvent(self, e):
        path = e.mimeData().urls()[0].toLocalFile()
        if os.path.isdir(path): self.folder_selected.emit(path); self.accept()


class AppUpdateWorker(QThread):
    update_found = pyqtSignal(str)

    def __init__(self):
        super().__init__()
        self.session = requests.Session()

    def run(self):
        repo_url = "https://api.github.com/repos/1FaY1/ModManager/releases/latest"
        try:
            response = request_with_retry(self.session, "GET", repo_url, timeout=10, headers=HEADERS)
            if response.status_code == 200:
                data = response.json()
                remote_tag = data.get("tag_name", "")
                remote_version = remote_tag.lower().replace("v", "").strip()
                current_version = VERSION.lower().replace("v", "").strip()

                logging.info("Update check: local=%s, remote=%s", current_version, remote_version)

                if is_version_newer(remote_version, current_version):
                    self.update_found.emit(remote_version)
            else:
                logging.warning("GitHub API returned status %s", response.status_code)
        except requests.RequestException as e:
            logging.error("Update check error: %s", e)


class ApiDataWorker(QThread):
    """Поток для загрузки тегов с Modrinth, чтобы окно не 'белело' при старте"""
    data_loaded = pyqtSignal(list, dict)
    error_occurred = pyqtSignal(str)

    def __init__(self):
        super().__init__()
        self.session = requests.Session()

    def run(self):
        try:
            v_resp = request_with_retry(self.session, "GET", f"{MODRINTH_API}/tag/game_version", timeout=10)
            v_resp.raise_for_status()
            v_res = v_resp.json()
            versions = [v['version'] for v in v_res if v.get('version_type') == 'release']

            l_resp = request_with_retry(self.session, "GET", f"{MODRINTH_API}/tag/loader", timeout=10)
            l_resp.raise_for_status()
            l_res = l_resp.json()
            loaders_by_type = defaultdict(set)
            for loader in l_res:
                name = loader.get("name")
                if not name:
                    continue
                supported = loader.get("supported_project_types", []) or []
                for project_type in supported:
                    loaders_by_type[str(project_type).lower()].add(name.capitalize())

            normalized = {
                key: sorted(values) for key, values in loaders_by_type.items()
            }

            self.data_loaded.emit(versions, normalized)
        except (requests.RequestException, ValueError, KeyError) as e:
            self.error_occurred.emit(str(e))


class ModManagerApp(QWidget):
    def ask_for_update(self, remote_version):
        reply = QMessageBox.question(
            self, "Обновление доступно",
            f"Доступна новая версия v{remote_version}!\n"
            f"У вас установлена v{VERSION}.\n\n"
            "Хотите перейти на страницу скачивания?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        if reply == QMessageBox.StandardButton.Yes:
            import webbrowser
            webbrowser.open("https://github.com/1FaY1/ModManager/releases")

    def save_config(self):
        """Централизованное сохранение настроек в JSON файл"""
        try:
            with open(CONFIG_FILE, 'w') as conf:
                json.dump({
                    "download_folder": self.download_folder,
                    "backup_folder": self.backup_folder
                }, conf)
        except OSError as e:
            logging.error("Ошибка сохранения конфига: %s", e)

    def __init__(self):
        super().__init__()
        self.setStyleSheet("background-color: #1e1e1e; color: #ffffff;")
        self.setWindowTitle(f"Mod Manager Pro v{VERSION}")

        icon_path = resource_path("icon.ico")
        self.setWindowIcon(QIcon(icon_path))

        self.resize(1100, 650)
        self.http_session = requests.Session()
        self.mods_folder, self.download_folder, self.backup_folder = "", "", ""
        self.instance_root = ""
        self.scan_targets = []
        self.active_project_type = "mod"
        self.available_versions = []
        self.loaders_by_project_type = {}
        self.auto_loader_hint = ""
        self.auto_version_hint = ""
        self.active_downloads = []
        self.updated_mods = []
        self.pending_batch_updates = 0
        self.batch_total_updates = 0
        self.max_parallel_downloads = 4
        self.batch_action_queue = []
        self.active_batch_downloads = 0

        self._init_ui()
        self.load_settings()
        self.status_lbl.setText("Загрузка данных API...")
        self.api_worker = ApiDataWorker()
        self.api_worker.data_loaded.connect(self._on_api_data_ready)
        self.api_worker.error_occurred.connect(lambda err: logging.error(f"Ошибка API: {err}"))
        self.api_worker.start()

        self.update_worker = AppUpdateWorker()
        self.update_worker.update_found.connect(self.ask_for_update)
        self.update_worker.start()

    def _on_api_data_ready(self, versions, loaders_by_type):
        self.available_versions = list(versions)
        self.loaders_by_project_type = dict(loaders_by_type or {})
        self._refresh_loader_options()

    def _resolve_active_project_type(self):
        if len(self.scan_targets) == 1:
            return (self.scan_targets[0].get("project_type") or "mod").lower()
        return "mod"

    def _refresh_loader_options(self):
        project_type = (self.active_project_type or "mod").lower()
        loaders = self.loaders_by_project_type.get(project_type) or self.loaders_by_project_type.get("mod") or []

        self.version_box.clear()
        self.loader_box.clear()
        self.loader_label.setText(PROJECT_TYPE_LOADER_LABELS.get(project_type, "Загрузчик:"))
        self.version_box.addItem("Авто")
        self.loader_box.addItem("Авто")
        self.version_box.addItems(self.available_versions)
        self.loader_box.addItems(loaders)
        self.status_lbl.setText("Готово к работе")

    def load_settings(self):
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r') as f:
                    config = json.load(f)
                    self.download_folder = config.get("download_folder", "")
                    self.backup_folder = config.get("backup_folder", "")
                    if self.download_folder:
                        self.status_lbl.setText(f"Загрузка в: {self.download_folder}")
            except (OSError, ValueError, json.JSONDecodeError) as e:
                logging.error(f"Не удалось загрузить настройки: {e}")

    def _init_ui(self):
        self.setStyleSheet("""
            QLineEdit { padding: 8px; font-size: 14px; border: 1px solid #bbb; border-radius: 4px; }
            QPushButton { height: 32px; font-weight: bold; padding: 0 10px; }
            QComboBox { height: 32px; min-width: 110px; border: 1px solid #bbb; border-radius: 4px; }
            QTableWidget { gridline-color: #eee; border: 1px solid #ddd; }
            #MenuBtn { border: none; background: transparent; font-size: 20px; color: #555; }
        """)
        layout = QVBoxLayout(self)
        nav = QHBoxLayout()

        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Поиск новых модов на Modrinth...")
        self.search_input.returnPressed.connect(self.start_search)
        nav.addWidget(self.search_input, stretch=1)

        self.search_btn_ui = QPushButton("🔍 Найти")
        self.search_btn_ui.clicked.connect(self.start_search)
        nav.addWidget(self.search_btn_ui)

        self.loader_box = QComboBox()
        self.version_box = QComboBox()
        self.loader_label = QLabel("Загрузчик:")
        nav.addWidget(self.loader_label)
        nav.addWidget(self.loader_box)
        nav.addWidget(QLabel("Версия:"))
        nav.addWidget(self.version_box)

        scan_dir_btn = QPushButton("📂 Выбрать сборку")
        scan_dir_btn.clicked.connect(self.select_scan_folder)
        nav.addWidget(scan_dir_btn)
        layout.addLayout(nav)

        self.table = QTableWidget(0, 6)
        self.table.setHorizontalHeaderLabels(["Мод / Файл", "Источник", "Версия", "Статус", "Прогресс", "Действие"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.table.setMouseTracking(True)
        self.table.cellClicked.connect(self.open_source_link)
        self.table.cellEntered.connect(self._handle_cell_entered)

        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.table.setFocusPolicy(Qt.FocusPolicy.NoFocus)

        layout.addWidget(self.table)

        bottom = QHBoxLayout()
        self.menu_btn = QPushButton("⋮")
        self.menu_btn.setObjectName("MenuBtn")
        self.menu_btn.clicked.connect(self.select_download_folder)

        self.status_lbl = QLabel("Готов к работе")
        self.status_lbl.setStyleSheet("color: #7f8c8d; font-size: 11px;")

        bottom.addWidget(self.menu_btn)
        bottom.addWidget(self.status_lbl)
        bottom.addStretch()

        self.scan_btn = QPushButton("🔄 Проверить обновления")
        self.scan_btn.clicked.connect(self.scan_folder)
        self.scan_btn.setEnabled(False)

        self.update_all_btn = QToolButton()
        self.update_all_btn.setObjectName("UpdateAllBtn")
        self.update_all_btn.setText("⬇️ Обновить всё")
        self.update_all_btn.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonTextOnly)
        self.update_all_btn.setPopupMode(QToolButton.ToolButtonPopupMode.MenuButtonPopup)
        self.update_all_btn.setStyleSheet(
            "QToolButton#UpdateAllBtn { background-color: #2ecc71; color: white; padding: 6px 12px; }"
        )
        self.update_all_btn.clicked.connect(self.update_all_mods)
        self.update_all_btn.hide()

        update_menu = QMenu(self)

        self.backup_before_update_action = QAction("Резервное копирование перед обновлением", self)
        self.backup_before_update_action.setCheckable(True)
        self.backup_before_update_action.setChecked(False)
        self.backup_before_update_action.setStatusTip("Сохраняет старые версии файлов в папку 'backups' перед заменой")
        self.backup_before_update_action.setToolTip(
            "Безопасное обновление: копия старого мода сохранится автоматически")
        update_menu.addAction(self.backup_before_update_action)

        update_menu.addSeparator()

        self.select_backup_dir_action = QAction("⋮ Выбрать папку для бэкапов...", self)
        self.select_backup_dir_action.triggered.connect(self.select_custom_backup_folder)
        update_menu.addAction(self.select_backup_dir_action)

        self.update_all_btn.setMenu(update_menu)

        bottom.addWidget(self.scan_btn)
        bottom.addWidget(self.update_all_btn)
        layout.addLayout(bottom)

    def open_source_link(self, row, column):
        if column != 1:
            return
        item = self.table.item(row, column)
        if not item:
            return
        source_url = item.data(Qt.ItemDataRole.UserRole)
        if not source_url:
            return
        import webbrowser
        webbrowser.open(source_url)

    def _handle_cell_entered(self, row, col):
        if col == 1:
            self.table.setCursor(Qt.CursorShape.PointingHandCursor)
        else:
            self.table.setCursor(Qt.CursorShape.ArrowCursor)

    def select_custom_backup_folder(self):
        f = QFileDialog.getExistingDirectory(self, "Выберите папку для сохранения бэкапов")
        if f:
            self.backup_folder = f
            self.status_lbl.setText(f"Папка бэкапов: {f}")
            self.save_config()

    def set_loading(self, loading, msg=""):
        """Визуальный индикатор работы."""
        if loading:
            self.setCursor(Qt.CursorShape.WaitCursor)
            self.status_lbl.setText(f"⌛ {msg}...")
            self.scan_btn.setEnabled(False)
            self.search_btn_ui.setEnabled(False)
        else:
            self.setCursor(Qt.CursorShape.ArrowCursor)
            self.status_lbl.setText("✅ Готово")
            if self.mods_folder: self.scan_btn.setEnabled(True)
            self.search_btn_ui.setEnabled(True)

    def select_scan_folder(self):
        d = FolderSelectDialog("Выберите папку с вашими модами", self)
        d.folder_selected.connect(self._set_scan_path)
        d.exec()

    def _set_scan_path(self, path):
        self.scan_targets, self.instance_root = discover_scan_targets(path)
        if not self.scan_targets:
            return

        self.active_project_type = self._resolve_active_project_type()
        self._refresh_loader_options()
        self.auto_loader_hint, self.auto_version_hint = self._detect_instance_hints(self.scan_targets)

        self.mods_folder = self.scan_targets[0]["folder"]
        self.scan_btn.setEnabled(True)
        self.table.setRowCount(0)
        self.update_all_btn.hide()
        targets_summary = ", ".join(t["title"] for t in self.scan_targets)
        self.status_lbl.setText(f"Выбрано: {targets_summary}")
        self.scanner = FolderScannerWorker(self.scan_targets, self.loader_box.currentText(), self.version_box.currentText(),
                                           check_updates=False)
        self.scanner.result_ready.connect(self._handle_scanner_result)
        self.scanner.start()

    def _detect_instance_hints(self, targets):
        loader_counter = Counter()
        version_counter = Counter()
        for target in targets:
            if target.get("project_type") != "mod":
                continue
            folder = target.get("folder")
            if not folder or not os.path.isdir(folder):
                continue
            for name in os.listdir(folder):
                if not name.lower().endswith(".jar"):
                    continue
                meta = read_archive_metadata(os.path.join(folder, name))
                loader_counter.update(meta.get("loaders", set()))
                version_counter.update(meta.get("mc_versions", set()))

        loader_hint = loader_counter.most_common(1)[0][0] if loader_counter else ""
        version_hint = version_counter.most_common(1)[0][0] if version_counter else ""
        return loader_hint, version_hint

    def scan_folder(self):
        if not self.scan_targets:
            return
        self.table.setRowCount(0)
        self.update_all_btn.hide()
        self.set_loading(True, "Проверка обновлений")
        self.scanner = FolderScannerWorker(self.scan_targets, self.loader_box.currentText(),
                                           self.version_box.currentText(), check_updates=True)
        self.scanner.result_ready.connect(self._handle_scanner_result)
        self.scanner.finished.connect(self._handle_scanner_finished)
        self.scanner.start()

    def _handle_scanner_result(self, res):
        if self.sender() is not self.scanner:
            return
        self.add_mod_to_table(res)

    def _handle_scanner_finished(self):
        if self.sender() is not self.scanner:
            return
        self.set_loading(False)

    def select_download_folder(self):
        """Выбор папки, куда качать новые моды (по нажатию на ⋮ внизу)"""
        f = QFileDialog.getExistingDirectory(self, "Куда скачивать моды?")
        if f:
            self.download_folder = f
            self.status_lbl.setText(f"Загрузка в: {f}")
            self.save_config()


    def start_search(self):
        q = self.search_input.text().strip()
        if not q: return
        selected_loader = self.loader_box.currentText()
        selected_version = self.version_box.currentText()
        worker_loader = selected_loader
        worker_version = selected_version

        if selected_loader == "Авто" and self.auto_loader_hint:
            worker_loader = self.auto_loader_hint.capitalize()
        if selected_version == "Авто" and self.auto_version_hint:
            worker_version = self.auto_version_hint

        self.table.setRowCount(0)
        self.update_all_btn.hide()
        self.set_loading(True, "Поиск модов")
        self.worker = ModSearchWorker(q, worker_loader, worker_version, project_type=self.active_project_type)

        def on_done(res, ok):
            if ok:
                for r in res: self.add_mod_to_table(r)
            self.set_loading(False)

        self.worker.results_ready.connect(on_done)
        self.worker.start()

    def add_mod_to_table(self, res):
        row = self.table.rowCount()
        self.table.insertRow(row)

        is_dep = res.get("is_dependency", False)
        title_text = f"  ↳ [Зависимость] {res['title']}" if is_dep else res["title"]

        items = [
            (0, title_text),
            (1, "🔗 Modrinth" if res.get("source_url") else (res.get("author") or res.get("scan_type", "—"))),
            (2, res.get("version", "—")),
            (3, res.get("status", "Неизвестно"))
        ]

        for col, text in items:
            item = QTableWidgetItem(text)
            item.setToolTip(text)

            if col == 0:
                item.setTextAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)
                item.setData(Qt.ItemDataRole.UserRole + 1, "dependency" if is_dep else "mod")
                if is_dep:
                    item.setForeground(QColor("#bdc3c7"))
            else:
                item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

            if col == 0:
                real_filename = res.get("display_name") or res.get("filename") or res["title"]
                item.setData(Qt.ItemDataRole.UserRole, {
                    "filename": real_filename,
                    "source_folder": res.get("source_folder")
                })

            if col == 3:
                if res.get("needs_update"):
                    item.setForeground(QColor("#e67e22"))
                    self.update_all_btn.show()
                elif "Актуально" in res.get("status", "") or "Загружен" in res.get("status", ""):
                    item.setForeground(QColor("#27ae60"))

            if col == 1 and res.get("source_url"):
                item.setData(Qt.ItemDataRole.UserRole, res.get("source_url"))
                item.setToolTip(f"{res.get('source_url')}\n(нажмите для открытия)")
                item.setForeground(QColor("#3498db"))

            self.table.setItem(row, col, item)

        pbar_container = QWidget()
        pbar_layout = QHBoxLayout(pbar_container)
        pbar = QProgressBar()
        pbar.setFixedHeight(14)
        pbar.setTextVisible(False)
        pbar_layout.addWidget(pbar)
        pbar_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
        pbar_layout.setContentsMargins(5, 0, 5, 0)
        self.table.setCellWidget(row, 4, pbar_container)

        allow_dependency_download = (not is_dep) or bool(res.get("is_missing_dependency"))
        if res.get("url") and allow_dependency_download:
            btn_container = QWidget()
            btn_layout = QHBoxLayout(btn_container)
            btn_text = "Обновить" if res.get("needs_update") else "Скачать"
            btn = QPushButton(btn_text)
            btn.setFixedWidth(100)
            btn.setProperty("project_id", res.get("project_id"))
            btn.setProperty("source_folder", res.get("source_folder"))
            btn.clicked.connect(partial(
                self.download,
                row,
                res["url"],
                res["filename"],
                bool(res.get("needs_update"))
            ))
            btn_layout.addWidget(btn)
            btn_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
            btn_layout.setContentsMargins(0, 0, 0, 0)
            self.table.setCellWidget(row, 5, btn_container)

        self.table.scrollToBottom()

    def update_all_mods(self):
        removed_duplicates = self._cleanup_duplicate_versions_before_batch()
        if removed_duplicates > 0:
            self.status_lbl.setText(
                f"🧹 Удалено старых дубликатов модов: {removed_duplicates}"
            )
            QApplication.processEvents()

        main_update_rows = self._collect_update_rows(include_dependencies=False)
        dependency_rows = self._collect_update_rows(include_dependencies=True, dependency_only=True)

        if not main_update_rows and not dependency_rows:
            if removed_duplicates > 0:
                QMessageBox.information(
                    self,
                    "Очистка завершена",
                    f"Удалено старых дубликатов: {removed_duplicates}.\nНовых обновлений не найдено."
                )
            else:
                QMessageBox.information(self, "Обновление", "Нет модов, требующих обновления.")
            return

        update_rows = list(main_update_rows)

        if dependency_rows:
            msg = QMessageBox(self)
            msg.setWindowTitle("Обязательные зависимости")
            msg.setText(f"Обнаружено {len(dependency_rows)} обязательных зависимостей для установки.")
            msg.setInformativeText("Скачать их вместе с остальными обновлениями?")
            btn_yes = msg.addButton("Скачать всё", QMessageBox.ButtonRole.AcceptRole)
            btn_no = msg.addButton("Только моды", QMessageBox.ButtonRole.RejectRole)
            btn_cancel = msg.addButton("Отмена", QMessageBox.ButtonRole.RejectRole)

            msg.exec()

            if msg.clickedButton() == btn_cancel:
                return
            elif msg.clickedButton() == btn_no:
                update_rows = list(main_update_rows)
            else:
                update_rows = list(main_update_rows) + list(dependency_rows)

        if not update_rows:
            return

        if removed_duplicates > 0:
            self.status_lbl.setText(
                f"🧹 Удалено старых дубликатов модов: {removed_duplicates}. Запускаю обновление..."
            )
            QApplication.processEvents()

        self.batch_total_updates = len(update_rows)
        self.pending_batch_updates = len(update_rows)
        self.update_all_btn.setEnabled(False)
        self.set_loading(True, f"Обновление модов 0/{self.batch_total_updates}")

        if self.backup_before_update_action.isChecked():
            if not self.mods_folder:
                QMessageBox.warning(self, "Ошибка", "Не выбрана рабочая папка.")
                self.pending_batch_updates = 0
                self.batch_total_updates = 0
                self.update_all_btn.setEnabled(True)
                self.set_loading(False)
                return

            base_for_backup = self.instance_root or self.mods_folder
            target_backup_dir = self.backup_folder or os.path.join(base_for_backup, "backups")
            os.makedirs(target_backup_dir, exist_ok=True)

            mods_to_backup = [
                (filename, source_folder)
                for _, btn, filename, source_folder in update_rows
                if filename and btn and btn.text() == "Обновить"
            ]
            for filename, source_folder in mods_to_backup:
                src = os.path.join(source_folder or self.mods_folder, filename)
                if os.path.exists(src):
                    try:
                        shutil.copy2(src, os.path.join(target_backup_dir, filename))
                    except Exception as e:
                        logging.error(f"Ошибка бэкапа {filename}: {e}")
                QApplication.processEvents()

        self.batch_action_queue = list(update_rows)
        self.active_batch_downloads = 0
        self._pump_batch_downloads()

    def _mark_batch_download_done(self, ok):
        if self.pending_batch_updates <= 0:
            return

        self.pending_batch_updates -= 1
        completed = self.batch_total_updates - self.pending_batch_updates
        prefix = "✅" if ok else "⚠️"
        self.status_lbl.setText(f"{prefix} Обновление модов: {completed}/{self.batch_total_updates}")

        if self.pending_batch_updates == 0:
            removed_after_batch = self._cleanup_duplicate_versions_before_batch()
            if removed_after_batch > 0:
                self.status_lbl.setText(f"🧹 Финальная очистка: удалено дублей {removed_after_batch}")
            self.batch_total_updates = 0
            self.batch_action_queue = []
            self.active_batch_downloads = 0
            self.update_all_btn.setEnabled(True)
            self.set_loading(False)

    def _get_action_button(self, row):
        container = self.table.cellWidget(row, 5)
        if not container:
            return None
        return container.findChild(QPushButton)

    def _collect_update_rows(self, include_dependencies=False, dependency_only=False):
        updates = []
        for row in range(self.table.rowCount()):
            btn = self._get_action_button(row)
            if not btn:
                continue
            item = self.table.item(row, 0)
            role = item.data(Qt.ItemDataRole.UserRole + 1) if item else "mod"
            is_dependency = role == "dependency"

            if dependency_only and not is_dependency:
                continue
            if not include_dependencies and is_dependency:
                continue
            if is_dependency:
                if btn.text() != "Скачать":
                    continue
            else:
                if btn.text() != "Обновить":
                    continue

            payload = item.data(Qt.ItemDataRole.UserRole) if item else {}
            filename = payload.get("filename") if isinstance(payload, dict) else payload
            source_folder = payload.get("source_folder") if isinstance(payload, dict) else None
            updates.append((row, btn, filename, source_folder))
        return updates

    def _pump_batch_downloads(self):
        while self.batch_action_queue and self.active_batch_downloads < self.max_parallel_downloads:
            _, btn, _, _ = self.batch_action_queue.pop(0)
            if not btn:
                continue
            btn.setProperty("batch_run", True)
            self.active_batch_downloads += 1
            btn.click()
            QApplication.processEvents()

    @staticmethod
    def _parse_iso_datetime(raw_value):
        if not raw_value:
            return datetime.min.replace(tzinfo=timezone.utc)
        try:
            return datetime.fromisoformat(str(raw_value).replace("Z", "+00:00"))
        except ValueError:
            return datetime.min.replace(tzinfo=timezone.utc)

    @staticmethod
    def _derive_mod_family_key(filename):
        name_without_ext = os.path.splitext(filename)[0].lower()
        match = re.search(r"[-_.]v?\d", name_without_ext)
        if match:
            return name_without_ext[:match.start()].strip("-_. ")
        return name_without_ext

    def _collect_hash_index(self, folder):
        hash_to_files = defaultdict(list)
        jar_files = [f for f in os.listdir(folder) if f.lower().endswith(".jar")]
        for mod_file in jar_files:
            mod_path = os.path.join(folder, mod_file)
            if not os.path.isfile(mod_path):
                continue
            file_hash = get_file_hash(mod_path)
            if file_hash:
                hash_to_files[file_hash].append(mod_file)
        return hash_to_files, jar_files

    def _fetch_recognized_files(self, hashes):
        if not hashes:
            return {}
        response = request_with_retry(
            self.http_session,
            "POST",
            f"{MODRINTH_API}/version_files",
            json={"hashes": list(hashes), "algorithm": "sha1"},
            headers=HEADERS,
            timeout=20
        )
        if response.status_code != 200:
            return {}
        return response.json()

    def _remove_file_list(self, folder, filenames, keep_name):
        removed = 0
        for name in filenames:
            if name == keep_name:
                continue
            old_path = os.path.join(folder, name)
            if not os.path.exists(old_path):
                continue
            try:
                os.remove(old_path)
                removed += 1
                logging.info("Удалена дублирующая версия %s (оставлен %s)", name, keep_name)
            except OSError as exc:
                logging.error("Не удалось удалить %s: %s", name, exc)
        return removed

    def _cleanup_duplicate_versions_in_folder(self, folder):
        if not folder or not os.path.isdir(folder):
            return 0

        hash_to_files, jar_files = self._collect_hash_index(folder)
        if len(jar_files) < 2 or not hash_to_files:
            return 0

        try:
            recognized = self._fetch_recognized_files(hash_to_files.keys())
        except requests.RequestException as exc:
            logging.error("Не удалось очистить дубликаты в %s: %s", folder, exc)
            return 0

        grouped = defaultdict(list)
        for file_hash, data in recognized.items():
            project_id = data.get("project_id")
            if not project_id:
                continue
            published = self._parse_iso_datetime(data.get("date_published"))
            version_number = data.get("version_number", "")
            for file_name in hash_to_files.get(file_hash, []):
                path = os.path.join(folder, file_name)
                if os.path.exists(path):
                    grouped[project_id].append((file_name, published, version_number, os.path.getmtime(path)))

        removed_count = 0
        for versions in grouped.values():
            if len(versions) < 2:
                continue
            keep_name, _, _, _ = max(
                versions,
                key=lambda entry: (entry[1], _version_key(entry[2]), entry[3])
            )
            removed_count += self._remove_file_list(folder, [name for name, *_ in versions], keep_name)
        return removed_count

    def _cleanup_duplicate_versions_before_batch(self, update_rows=None):
        folders = set()
        if update_rows:
            for _, btn, _, source_folder in update_rows:
                if btn and btn.text() == "Обновить":
                    folders.add(source_folder or self.mods_folder)
        else:
            for target in self.scan_targets:
                if target.get("project_type") == "mod" and target.get("folder"):
                    folders.add(target["folder"])
            if self.mods_folder and not folders:
                folders.add(self.mods_folder)

        return sum(self._cleanup_duplicate_versions_in_folder(folder) for folder in folders)

    def _cleanup_project_duplicates_in_folder(self, folder, project_id, keep_filename):
        if not folder or not os.path.isdir(folder) or not project_id:
            return 0

        hash_to_files, jar_files = self._collect_hash_index(folder)
        if len(jar_files) < 2 or not hash_to_files:
            return 0

        removed_count = 0
        recognized_entries = []
        try:
            recognized = self._fetch_recognized_files(hash_to_files.keys())
            for file_hash, data in recognized.items():
                if data.get("project_id") != project_id:
                    continue
                published = self._parse_iso_datetime(data.get("date_published"))
                version_number = data.get("version_number", "")
                for file_name in hash_to_files.get(file_hash, []):
                    path = os.path.join(folder, file_name)
                    if os.path.exists(path):
                        recognized_entries.append(
                            (file_name, published, version_number, os.path.getmtime(path))
                        )
        except requests.RequestException as exc:
            logging.error("Не удалось сделать post-cleanup для %s: %s", project_id, exc)

        if recognized_entries:
            keep_name, _, _, _ = max(
                recognized_entries,
                key=lambda entry: (
                    1 if entry[0] == keep_filename else 0,
                    entry[1],
                    _version_key(entry[2]),
                    entry[3]
                )
            )
            return self._remove_file_list(folder, [name for name, *_ in recognized_entries], keep_name)

        keep_family = self._derive_mod_family_key(keep_filename)
        family_candidates = []
        for mod_file in jar_files:
            if self._derive_mod_family_key(mod_file) != keep_family:
                continue
            path = os.path.join(folder, mod_file)
            if os.path.exists(path):
                family_candidates.append((mod_file, os.path.getmtime(path)))

        if len(family_candidates) < 2:
            return 0

        family_candidates.sort(
            key=lambda item: (1 if item[0] == keep_filename else 0, item[1]),
            reverse=True
        )
        keep_name = family_candidates[0][0]
        return self._remove_file_list(folder, [name for name, _ in family_candidates], keep_name)

    def download(self, row, url, filename, needs_update):
        btn = self._get_action_button(row)
        if not btn:
            return

        project_id = btn.property("project_id")
        source_folder = btn.property("source_folder")
        is_batch_run = bool(btn.property("batch_run"))
        is_update = btn.text() == "Обновить"
        save_dir = (source_folder or self.mods_folder) if (is_update or not self.download_folder) else self.download_folder

        if not save_dir:
            QMessageBox.warning(self, "!", "Выберите папку!")
            return

        files_to_delete_after_download = []

        if project_id and is_update and needs_update:
            try:
                file_ext = os.path.splitext(filename)[1].lower()
                candidate_files = [f for f in os.listdir(save_dir) if f.lower().endswith(file_ext) and f != filename]
                hash_to_file = {}
                for existing_file in candidate_files:
                    existing_path = os.path.join(save_dir, existing_file)
                    file_hash = get_file_hash(existing_path)
                    if file_hash:
                        hash_to_file.setdefault(file_hash, []).append(existing_file)
                    QApplication.processEvents()

                recognized_processed = False
                if hash_to_file:
                    r = request_with_retry(
                        self.http_session,
                        "POST",
                        f"{MODRINTH_API}/version_files",
                        json={"hashes": list(hash_to_file.keys()), "algorithm": "sha1"},
                        headers=HEADERS,
                        timeout=15
                    )
                    if r.status_code == 200:
                        recognized = r.json()
                        for file_hash, data in recognized.items():
                            if data.get("project_id") == project_id:
                                old_files = hash_to_file.get(file_hash, [])
                                for old_file in old_files:
                                    if old_file == filename:
                                        continue
                                    files_to_delete_after_download.append(old_file)
                        recognized_processed = True

                if not recognized_processed:
                    v_res = request_with_retry(
                        self.http_session,
                        "GET",
                        f"{MODRINTH_API}/project/{project_id}/version",
                        headers=HEADERS,
                        timeout=5
                    )
                    if v_res.status_code == 200:
                        valid_filenames = set()
                        for ver in v_res.json():
                            for file_entry in ver.get('files', []):
                                fname = file_entry.get('filename')
                                if fname:
                                    valid_filenames.add(fname)

                        if valid_filenames:
                            for existing_file in os.listdir(save_dir):
                                if existing_file == filename:
                                    continue
                                existing_path = os.path.join(save_dir, existing_file)
                                if not os.path.isfile(existing_path):
                                    continue
                                if existing_file in valid_filenames:
                                    files_to_delete_after_download.append(existing_file)
                                QApplication.processEvents()
            except (requests.RequestException, OSError) as e:
                logging.error("Ошибка точной очистки: %s", e)

        files_to_delete_after_download = sorted(set(files_to_delete_after_download))

        dest = os.path.join(save_dir, filename)
        container = self.table.cellWidget(row, 4)
        pbar = container.findChild(QProgressBar)
        btn.setEnabled(False)

        downloader = DownloadThread(url, dest)
        downloader.progress.connect(pbar.setValue)

        def cleanup():
            if downloader in self.active_downloads:
                self.active_downloads.remove(downloader)
                logging.info("Поток для %s очищен из памяти.", filename)

        def on_done(path):
            btn.setText("Ок")
            btn.setProperty("batch_run", False)
            self.table.item(row, 0).setText(filename)
            self.table.item(row, 0).setData(Qt.ItemDataRole.UserRole, {
                "filename": filename,
                "source_folder": source_folder
            })
            self.status_lbl.setText(f"Скачано: {filename}")

            if os.path.exists(path):
                for old_file in files_to_delete_after_download:
                    old_path = os.path.join(save_dir, old_file)
                    if os.path.exists(old_path):
                        try:
                            os.remove(old_path)
                            logging.info("Удалена старая версия мода после загрузки: %s", old_file)
                        except OSError as e:
                            logging.error("Не удалось удалить %s: %s", old_file, e)
                if project_id:
                    removed_post = self._cleanup_project_duplicates_in_folder(save_dir, project_id, filename)
                    if removed_post:
                        logging.info("Post-cleanup удалил %s дублей для project_id=%s", removed_post, project_id)

            if os.path.exists(path) and filename not in self.updated_mods:
                self.updated_mods.append(filename)

            cleanup()
            self._mark_batch_download_done(True)
            if is_batch_run and self.active_batch_downloads > 0:
                self.active_batch_downloads -= 1
            self._pump_batch_downloads()

        def on_error(err_msg):
            QMessageBox.critical(self, "Ошибка", err_msg)
            btn.setEnabled(True)
            btn.setProperty("batch_run", False)
            cleanup()
            self._mark_batch_download_done(False)
            if is_batch_run and self.active_batch_downloads > 0:
                self.active_batch_downloads -= 1
            self._pump_batch_downloads()

        downloader.finished.connect(on_done)
        downloader.error.connect(on_error)

        self.active_downloads.append(downloader)
        downloader.start()

    def backup_updated_mods(self):
        if not self.updated_mods:
            QMessageBox.information(self, "Резервное копирование", "Нет обновленных модов для копирования.")
            return

        if not self.mods_folder:
            QMessageBox.warning(self, "Резервное копирование", "Сначала выберите папку с модами.")
            return

        backup_dir = QFileDialog.getExistingDirectory(self, "Выберите папку для резервных копий")
        if not backup_dir:
            return

        copied = 0
        errors = []

        for filename in sorted(self.updated_mods):
            src = os.path.join(self.mods_folder, filename)
            dst = os.path.join(backup_dir, filename)

            if os.path.exists(src):
                try:
                    shutil.copy2(src, dst)
                    copied += 1
                except OSError as e:
                    logging.error("Ошибка копирования %s: %s", filename, e)
                    errors.append(f"{filename} ({str(e)})")

        msg = f"Успешно скопировано файлов: {copied}"
        if errors:
            msg += f"\n\nНе удалось скопировать ({len(errors)}):"
            msg += "\n" + "\n".join(errors[:5])
            if len(errors) > 5:
                msg += "\n... и другие."

            QMessageBox.warning(self, "Результат копирования с ошибками", msg)
        else:
            QMessageBox.information(self, "Резервное копирование", msg)


if __name__ == "__main__":
    _install_global_exception_hook()
    try:
        app = QApplication(sys.argv)
        w = ModManagerApp()
        w.show()
        sys.exit(app.exec())
    except Exception:
        logging.exception("Критическая ошибка запуска приложения")
        raise
