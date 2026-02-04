import sys
import os
import requests
import json
import concurrent.futures
import re
import shutil
from functools import partial
from PyQt6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout,
    QLineEdit, QPushButton, QComboBox, QTableWidget,
    QTableWidgetItem, QFileDialog, QMessageBox,
    QLabel, QProgressBar, QHeaderView, QDialog, QAbstractItemView,
    QToolButton, QMenu
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QColor, QIcon, QAction

from utils import get_file_hash


def resource_path(relative_path):
    """ Функция для поиска иконки внутри собранного EXE """
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)


VERSION = "1.4"
MODRINTH_API = "https://api.modrinth.com/v2"
HEADERS = {"User-Agent": f"MyMinecraftManager/{VERSION}"}
WORKER_THREADS = 8
CONFIG_FILE = "mod_manager_config.json"

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


class DownloadThread(QThread):
    progress = pyqtSignal(int)
    finished = pyqtSignal(str)
    error = pyqtSignal(str)

    def __init__(self, url, save_path):
        super().__init__()
        self.url, self.save_path = url, save_path

    def run(self):
        try:
            with requests.get(self.url, stream=True, headers=HEADERS, timeout=20) as r:
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

    def __init__(self, query, loader, mc_ver):
        super().__init__()
        self.query, self.loader, self.mc_ver = query.strip(), loader, mc_ver

    def run(self):
        try:
            clean_loader = self.loader.strip().lower()
            clean_ver = self.mc_ver.strip()

            facets = [
                '["project_type:mod"]',
                f'["categories:{clean_loader}"]',
                f'["versions:{clean_ver}"]'
            ]

            params = {
                "query": f'"{self.query}"',
                "limit": 25,
                "index": "relevance",
                "facets": f"[{','.join(facets)}]"
            }

            r = requests.get(f"{MODRINTH_API}/search", params=params, headers=HEADERS, timeout=10)
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
                v_params = {
                    "loaders": f'["{clean_loader}"]',
                    "game_versions": f'["{clean_ver}"]'
                }

                try:
                    vr = requests.get(
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

                            return {
                                "title": hit["title"],
                                "author": hit["author"],
                                "version": selected_v["version_number"],
                                "project_id": hit["project_id"],
                                "url": selected_v["files"][0]["url"],
                                "filename": selected_v["files"][0]["filename"],
                                "status": "Доступен",
                                "needs_update": False
                            }
                except Exception as e:
                    print(f"Ошибка получения версии: {e}")
                return None

            with concurrent.futures.ThreadPoolExecutor(max_workers=WORKER_THREADS) as ex:
                futures = [ex.submit(fetch_ver, h) for h in hits]
                for f in concurrent.futures.as_completed(futures):
                    res = f.result()
                    if res:
                        results.append(res)

            self.results_ready.emit(results, True)

        except Exception as e:
            print(f"Search error: {e}")
            self.results_ready.emit([], False)


class FolderScannerWorker(QThread):
    mod_found = pyqtSignal(dict)
    finished = pyqtSignal()

    def __init__(self, folder, loader, mc_ver, check_updates=False):
        super().__init__()
        self.folder = folder
        self.loader = loader.lower()
        self.mc_ver = mc_ver
        self.check_updates = check_updates

    def run(self):
        if not os.path.exists(self.folder):
            self.finished.emit()
            return

        files = [f for f in os.listdir(self.folder) if f.endswith('.jar')]
        hash_to_file = {}
        for f in files:
            path = os.path.join(self.folder, f)
            h = get_file_hash(path)
            if h:
                hash_to_file[h] = f

        if not hash_to_file:
            self.finished.emit()
            return

        recognized = {}
        try:
            r = requests.post(
                f"{MODRINTH_API}/version_files",
                json={"hashes": list(hash_to_file.keys()), "algorithm": "sha1"},
                headers=HEADERS,
                timeout=15
            )
            if r.status_code == 200:
                recognized = r.json()
        except:
            pass

        project_names = {}
        u_ids = list(set(v['project_id'] for v in recognized.values()))
        if u_ids:
            try:
                rp = requests.get(
                    f"{MODRINTH_API}/projects",
                    params={"ids": json.dumps(u_ids)},
                    headers=HEADERS,
                    timeout=15
                )
                if rp.status_code == 200:
                    project_names = {p['id']: p['title'] for p in rp.json()}
            except:
                pass

        def process_one_mod(item):
            f_hash, filename = item
            result = {
                "title": filename,
                "display_name": filename,
                "author": "-",
                "version": "-",
                "status": "Неизвестно",
            }

            if f_hash in recognized:
                v_data = recognized[f_hash]
                p_id = v_data['project_id']

                result["title"] = project_names.get(p_id, filename)
                result["author"] = "Modrinth"
                result["version"] = v_data['version_number']
                result["status"] = "Загружен"

                if self.check_updates:
                    try:
                        v_p = {"loaders": f'["{self.loader}"]', "game_versions": f'["{self.mc_ver}"]'}
                        vr = requests.get(
                            f"{MODRINTH_API}/project/{p_id}/version",
                            params=v_p,
                            headers=HEADERS,
                            timeout=10
                        )
                        if vr.status_code == 200 and vr.json():
                            latest = vr.json()[0]
                            if latest['id'] != v_data['id']:
                                result[
                                    "status"] = f"Обновление! ({v_data['version_number']} -> {latest['version_number']})"
                                result["url"] = latest['files'][0]['url']
                                result["filename"] = latest['files'][0]['filename']
                                result["needs_update"] = True
                            else:
                                result["status"] = "Актуально"
                    except:
                        pass

            return result

        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKER_THREADS) as executor:
            futures = [executor.submit(process_one_mod, item) for item in hash_to_file.items()]

            for future in concurrent.futures.as_completed(futures):
                try:
                    res = future.result()
                    self.mod_found.emit(res)
                except Exception:
                    pass

        self.finished.emit()


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


class ModManagerApp(QWidget):
    def check_for_app_updates(self):
        repo_url = "https://api.github.com/repos/1FaY1/ModManager/releases/latest"
        try:
            response = requests.get(repo_url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                latest_version = data.get("tag_name", "").replace("v", "")

                if latest_version and is_version_newer(latest_version, VERSION):
                    reply = QMessageBox.question(
                        self, "Обновление доступно",
                        f"Доступна новая версия v{latest_version}!\n"
                        f"У вас установлена v{VERSION}.\n\n"
                        "Хотите перейти на страницу скачивания?",
                        QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
                    )
                    if reply == QMessageBox.StandardButton.Yes:
                        import webbrowser
                        webbrowser.open(data.get("html_url"))
        except Exception as e:
            print(f"Ошибка проверки обновлений программы: {e}")

    def __init__(self):
        super().__init__()
        self.setStyleSheet("background-color: #1e1e1e; color: #ffffff;")

        self.setWindowTitle(f"Mod Manager Pro v{VERSION}")

        icon_path = resource_path("icon.ico")
        self.setWindowIcon(QIcon(icon_path))

        self.resize(1100, 650)
        self.mods_folder, self.download_folder, self.active_downloads = "", "", []
        self.updated_mods = set()
        self.track_updated_mods = True

        self._init_ui()
        self.load_settings()
        self._load_api_data()

        from PyQt6.QtCore import QTimer
        QTimer.singleShot(1000, self.check_for_app_updates)

    def load_settings(self):
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r') as f:
                    config = json.load(f)
                    self.download_folder = config.get("download_folder", "")
                    if self.download_folder: self.status_lbl.setText(f"Загрузка в: {self.download_folder}")
            except:
                pass

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
        nav.addWidget(QLabel("Загрузчик:"))
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
        update_menu.addAction(self.backup_before_update_action)

        backup_action = QAction("Резервное копирование", self)
        backup_action.triggered.connect(self.backup_updated_mods)
        update_menu.addAction(backup_action)

        track_updates_action = QAction("Запоминать обновленные моды", self)
        track_updates_action.setCheckable(True)
        track_updates_action.setChecked(True)
        track_updates_action.toggled.connect(lambda enabled: setattr(self, "track_updated_mods", enabled))
        update_menu.addAction(track_updates_action)

        self.update_all_btn.setMenu(update_menu)

        bottom.addWidget(self.scan_btn)
        bottom.addWidget(self.update_all_btn)
        layout.addLayout(bottom)

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
        self.mods_folder = path
        self.scan_btn.setEnabled(True)
        self.table.setRowCount(0)
        self.scanner = FolderScannerWorker(path, self.loader_box.currentText(), self.version_box.currentText(),
                                           check_updates=False)
        self.scanner.mod_found.connect(self.add_mod_to_table)
        self.scanner.start()

    def scan_folder(self):
        if not self.mods_folder: return
        self.table.setRowCount(0)
        self.update_all_btn.hide()
        self.set_loading(True, "Проверка обновлений")
        self.scanner = FolderScannerWorker(self.mods_folder, self.loader_box.currentText(),
                                           self.version_box.currentText(), check_updates=True)
        self.scanner.mod_found.connect(self.add_mod_to_table)
        self.scanner.finished.connect(lambda: self.set_loading(False))
        self.scanner.start()

    def select_download_folder(self):
        f = QFileDialog.getExistingDirectory(self, "Куда скачивать моды?")
        if f:
            self.download_folder = f
            self.status_lbl.setText(f"Загрузка в: {f}")
            with open(CONFIG_FILE, 'w') as conf: json.dump({"download_folder": f}, conf)

    def _load_api_data(self):
        """Загружаем версии игры и загрузчики с обработкой ошибок."""
        self.status_lbl.setText("Подключение к Modrinth...")
        QApplication.processEvents()  # Чтобы интерфейс не завис на секунду

        try:
            v_res = requests.get(f"{MODRINTH_API}/tag/game_version", timeout=10)
            v_res.raise_for_status()  # Если сервер ответит ошибкой (404, 500), мы это поймаем

            if v_res.status_code == 200:
                versions = [v['version'] for v in v_res.json() if v.get('version_type') == 'release']
                self.version_box.clear()  # Очищаем перед добавлением
                self.version_box.addItems(versions)

            l_res = requests.get(f"{MODRINTH_API}/tag/loader", timeout=10)
            l_res.raise_for_status()

            if l_res.status_code == 200:
                loaders = sorted([l['name'].capitalize() for l in l_res.json()
                                  if "mod" in l.get("supported_project_types", [])])
                self.loader_box.clear()
                self.loader_box.addItems(loaders)

            self.status_lbl.setText("Данные API загружены")

        except requests.exceptions.RequestException as e:
            self.status_lbl.setText("Ошибка сети")
            QMessageBox.critical(self, "Ошибка подключения",
                                 f"Не удалось связаться с сервером Modrinth.\n"
                                 f"Проверьте интернет и перезапустите программу.\n\n"
                                 f"Детали: {str(e)}")
        except Exception as e:
            self.status_lbl.setText("Ошибка данных")
            QMessageBox.warning(self, "Ошибка", f"Сбой при обработке данных:\n{str(e)}")

    def start_search(self):
        q = self.search_input.text().strip()
        if not q: return
        self.table.setRowCount(0)
        self.update_all_btn.hide()
        self.set_loading(True, "Поиск модов")
        self.worker = ModSearchWorker(q, self.loader_box.currentText(), self.version_box.currentText())
        self.worker = ModSearchWorker(q, self.loader_box.currentText(), self.version_box.currentText())

        def on_done(res, ok):
            if ok:
                for r in res: self.add_mod_to_table(r)
            self.set_loading(False)

        self.worker.results_ready.connect(on_done)
        self.worker.start()

    def add_mod_to_table(self, res):
        row = self.table.rowCount()
        self.table.insertRow(row)

        items = [
            (0, res["title"]),
            (1, res["author"]),
            (2, res["version"]),
            (3, res["status"])
        ]

        for col, text in items:
            item = QTableWidgetItem(text)
            item.setToolTip(text)
            item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

            if col == 0:
                real_filename = res.get("display_name", res["title"])
                item.setData(Qt.ItemDataRole.UserRole, real_filename)

            if col == 3:
                if res.get("needs_update"):
                    item.setForeground(QColor("#e67e22"))
                    self.update_all_btn.show()
                elif "Актуально" in res["status"] or "Загружен" in res["status"]:
                    item.setForeground(QColor("#27ae60"))

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

        if res.get("url"):
            btn_container = QWidget()
            btn_layout = QHBoxLayout(btn_container)
            btn_text = "Обновить" if res.get("needs_update") else "Скачать"
            btn = QPushButton(btn_text)
            btn.setFixedWidth(100)
            btn.setProperty("project_id", res.get("project_id"))
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
        if self.backup_before_update_action.isChecked():
            if not self.mods_folder:
                QMessageBox.warning(self, "Резервное копирование", "Сначала выберите папку с модами.")
                return

            mods_to_backup = []
            for r in range(self.table.rowCount()):
                container = self.table.cellWidget(r, 5)
                if container:
                    btn = container.findChild(QPushButton)
                    if btn and btn.text() == "Обновить":
                        item = self.table.item(r, 0)
                        original_filename = item.data(Qt.ItemDataRole.UserRole) if item else None
                        if original_filename:
                            mods_to_backup.append(original_filename)

            if mods_to_backup:
                backup_dir = QFileDialog.getExistingDirectory(self, "Выберите папку для резервных копий")
                if not backup_dir:
                    return

                copied = 0
                missing = 0
                for filename in mods_to_backup:
                    src = os.path.join(self.mods_folder, filename)
                    if os.path.exists(src):
                        shutil.copy2(src, os.path.join(backup_dir, filename))
                        copied += 1
                    else:
                        missing += 1

                QMessageBox.information(
                    self,
                    "Резервное копирование",
                    f"Скопировано файлов: {copied}\nОтсутствовало файлов: {missing}"
                )

        for r in range(self.table.rowCount()):
            container = self.table.cellWidget(r, 5)
            if container:
                btn = container.findChild(QPushButton)
                if btn and btn.text() == "Обновить":
                    btn.click()

    def download(self, row, url, filename, needs_update):
        container_btn = self.table.cellWidget(row, 5)
        btn = container_btn.findChild(QPushButton)
        if not btn:
            return

        project_id = btn.property("project_id")
        is_update = btn.text() == "Обновить"
        save_dir = self.mods_folder if (is_update or not self.download_folder) else self.download_folder

        if not save_dir:
            QMessageBox.warning(self, "!", "Выберите папку!")
            return

        if project_id and is_update and needs_update:
            try:
                candidate_files = [
                    f for f in os.listdir(save_dir)
                    if f.endswith(".jar") and f != filename
                ]
                hash_to_file = {}
                for existing_file in candidate_files:
                    existing_path = os.path.join(save_dir, existing_file)
                    file_hash = get_file_hash(existing_path)
                    if file_hash:
                        hash_to_file[file_hash] = existing_file

                recognized_processed = False
                if hash_to_file:
                    r = requests.post(
                        f"{MODRINTH_API}/version_files",
                        json={"hashes": list(hash_to_file.keys()), "algorithm": "sha1"},
                        headers=HEADERS,
                        timeout=15
                    )
                    if r.status_code == 200:
                        recognized = r.json()
                        for file_hash, data in recognized.items():
                            if data.get("project_id") == project_id:
                                old_file = hash_to_file.get(file_hash)
                                if old_file and old_file != filename:
                                    old_path = os.path.join(save_dir, old_file)
                                    os.remove(old_path)
                                    print(f"Удалена старая версия мода по ID: {old_file}")
                        recognized_processed = True

                if not recognized_processed:
                    v_res = requests.get(
                        f"{MODRINTH_API}/project/{project_id}/version",
                        headers=HEADERS,
                        timeout=5
                    )
                    if v_res.status_code == 200:
                        valid_filenames = []
                        for ver in v_res.json():
                            for f in ver['files']:
                                valid_filenames.append(f['filename'])

                        for existing_file in os.listdir(save_dir):
                            if existing_file in valid_filenames and existing_file != filename:
                                old_path = os.path.join(save_dir, existing_file)
                                os.remove(old_path)
                                print(f"Удалена старая версия мода по ID: {existing_file}")
            except Exception as e:
                print(f"Ошибка точной очистки: {e}")

        dest = os.path.join(save_dir, filename)
        container = self.table.cellWidget(row, 4)
        pbar = container.findChild(QProgressBar)
        btn.setEnabled(False)

        downloader = DownloadThread(url, dest)
        downloader.progress.connect(pbar.setValue)

        def on_done(path):
            btn.setText("Ок")
            self.table.item(row, 0).setText(filename)
            self.table.item(row, 0).setData(Qt.ItemDataRole.UserRole, filename)
            self.status_lbl.setText(f"Скачано: {filename}")
            if is_update and needs_update and self.track_updated_mods:
                self.updated_mods.add(filename)

        downloader.finished.connect(on_done)
        downloader.error.connect(lambda e: (QMessageBox.critical(self, "Ошибка", e), btn.setEnabled(True)))
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
        for filename in sorted(self.updated_mods):
            src = os.path.join(self.mods_folder, filename)
            if os.path.exists(src):
                shutil.copy2(src, os.path.join(backup_dir, filename))
                copied += 1

                QMessageBox.information(
                    self,
                    "Резервное копирование",
                    f"Скопировано файлов: {copied}"
                )

    def _set_track_updated_mods(self, enabled):
        self.track_updated_mods = enabled


if __name__ == "__main__":
    app = QApplication(sys.argv)
    w = ModManagerApp()
    w.show()
    sys.exit(app.exec())
