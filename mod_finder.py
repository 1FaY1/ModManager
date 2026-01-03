import sys
import os
import requests
import hashlib
import json
import concurrent.futures
from functools import partial
from PyQt6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout,
    QLineEdit, QPushButton, QComboBox, QTableWidget,
    QTableWidgetItem, QFileDialog, QMessageBox,
    QLabel, QProgressBar, QHeaderView, QDialog
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QColor, QIcon # Добавили QIcon

def resource_path(relative_path):
    """ Функция для поиска иконки внутри собранного EXE """
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

# --- Настройки ---
VERSION = "1.1"
MODRINTH_API = "https://api.modrinth.com/v2"
HEADERS = {"User-Agent": f"MyMinecraftManager/{VERSION}"}
WORKER_THREADS = 8
CONFIG_FILE = "mod_manager_config.json"


def get_file_hash(path):
    sha1 = hashlib.sha1()
    try:
        with open(path, 'rb') as f:
            while chunk := f.read(8192):
                sha1.update(chunk)
        return sha1.hexdigest()
    except:
        return None


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
        self.query, self.loader, self.mc_ver = query.lower(), loader, mc_ver

    def run(self):
        try:
            facets = ['["project_type:mod"]', f'["categories:{self.loader.lower()}"]', f'["versions:{self.mc_ver}"]']
            params = {"query": self.query, "limit": 20, "facets": f"[{','.join(facets)}]"}
            r = requests.get(f"{MODRINTH_API}/search", params=params, headers=HEADERS, timeout=10)
            hits = r.json().get("hits", [])
            results = []

            def fetch_ver(hit):
                v_p = {"loaders": f'["{self.loader.lower()}"]', "game_versions": f'["{self.mc_ver}"]'}
                vr = requests.get(f"{MODRINTH_API}/project/{hit['project_id']}/version", params=v_p, headers=HEADERS,
                                  timeout=5)
                if vr.status_code == 200 and vr.json():
                    v = vr.json()[0]
                    return {"title": hit["title"], "author": hit["author"], "version": v["version_number"],
                            "url": v["files"][0]["url"], "filename": v["files"][0]["filename"], "status": "Доступен",
                            "needs_update": False}
                return None

            with concurrent.futures.ThreadPoolExecutor(max_workers=WORKER_THREADS) as ex:
                futures = [ex.submit(fetch_ver, h) for h in hits]
                for f in concurrent.futures.as_completed(futures):
                    res = f.result()
                    if res: results.append(res)
            self.results_ready.emit(results, True)
        except:
            self.results_ready.emit([], False)


class FolderScannerWorker(QThread):
    mod_found = pyqtSignal(dict)
    finished = pyqtSignal()

    def __init__(self, folder, loader, mc_ver, check_updates=False):
        super().__init__()
        self.folder, self.loader, self.mc_ver, self.check_updates = folder, loader.lower(), mc_ver, check_updates

    def run(self):
        if not os.path.exists(self.folder):
            self.finished.emit();
            return
        files = [f for f in os.listdir(self.folder) if f.endswith('.jar')]
        hash_to_file = {get_file_hash(os.path.join(self.folder, f)): f for f in files if
                        get_file_hash(os.path.join(self.folder, f))}

        recognized = {}
        if hash_to_file:
            try:
                r = requests.post(f"{MODRINTH_API}/version_files",
                                  json={"hashes": list(hash_to_file.keys()), "algorithm": "sha1"}, headers=HEADERS,
                                  timeout=10)
                if r.status_code == 200: recognized = r.json()
            except:
                pass

        project_names = {}
        u_ids = list(set(v['project_id'] for v in recognized.values()))
        if u_ids:
            try:
                rp = requests.get(f"{MODRINTH_API}/projects", params={"ids": json.dumps(u_ids)}, headers=HEADERS,
                                  timeout=5)
                if rp.status_code == 200: project_names = {p['id']: p['title'] for p in rp.json()}
            except:
                pass

        for f_hash, filename in hash_to_file.items():
            result = {"title": filename, "author": "-", "version": "-", "status": "Не опознан", "url": None,
                      "filename": filename, "needs_update": False}
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
                        vr = requests.get(f"{MODRINTH_API}/project/{p_id}/version", params=v_p, headers=HEADERS,
                                          timeout=5)
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
            self.mod_found.emit(result)
        self.finished.emit()


class FolderSelectDialog(QDialog):
    folder_selected = pyqtSignal(str)

    def __init__(self, title="Выбор папки", parent=None):
        super().__init__(parent)
        self.setWindowTitle(title);
        self.setFixedSize(350, 200);
        self.setAcceptDrops(True)
        layout = QVBoxLayout()
        # text= убирает предупреждение PyCharm
        self.lbl = QLabel(text="Перетащите папку сюда\nили нажмите кнопку")
        self.lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        btn = QPushButton("Открыть проводник")
        btn.clicked.connect(self.browse)
        layout.addWidget(self.lbl);
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
    def __init__(self):
        super().__init__()
        self.setWindowTitle(f"Mod Manager Pro v{VERSION}")

        # ВСТАВЬ ЭТИ СТРОКИ ЗДЕСЬ:
        icon_path = resource_path("icon.ico")
        self.setWindowIcon(QIcon(icon_path))

        self.resize(1100, 650)
        self.mods_folder, self.download_folder, self.active_downloads = "", "", []

        self._init_ui()
        self.load_settings()
        self._load_api_data()

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

        self.loader_box = QComboBox();
        self.version_box = QComboBox()
        nav.addWidget(QLabel("Загрузчик:"));
        nav.addWidget(self.loader_box)
        nav.addWidget(QLabel("Версия:"));
        nav.addWidget(self.version_box)

        scan_dir_btn = QPushButton("📂 Выбрать сборку")
        scan_dir_btn.clicked.connect(self.select_scan_folder)
        nav.addWidget(scan_dir_btn)
        layout.addLayout(nav)

        self.table = QTableWidget(0, 6)
        self.table.setHorizontalHeaderLabels(["Мод / Файл", "Источник", "Версия", "Статус", "Прогресс", "Действие"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        layout.addWidget(self.table)

        bottom = QHBoxLayout()
        self.menu_btn = QPushButton("⋮");
        self.menu_btn.setObjectName("MenuBtn")
        self.menu_btn.clicked.connect(self.select_download_folder)

        self.status_lbl = QLabel("Готов к работе")
        self.status_lbl.setStyleSheet("color: #7f8c8d; font-size: 11px;")

        bottom.addWidget(self.menu_btn);
        bottom.addWidget(self.status_lbl);
        bottom.addStretch()

        self.scan_btn = QPushButton("🔄 Проверить обновления")
        self.scan_btn.clicked.connect(self.scan_folder);
        self.scan_btn.setEnabled(False)

        self.update_all_btn = QPushButton("⬇️ Обновить всё")
        self.update_all_btn.setStyleSheet("background-color: #2ecc71; color: white;")
        self.update_all_btn.clicked.connect(self.update_all_mods);
        self.update_all_btn.hide()

        bottom.addWidget(self.scan_btn);
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
        d.folder_selected.connect(self._set_scan_path);
        d.exec()

    def _set_scan_path(self, path):
        self.mods_folder = path;
        self.scan_btn.setEnabled(True)
        self.table.setRowCount(0)
        self.scanner = FolderScannerWorker(path, self.loader_box.currentText(), self.version_box.currentText(),
                                           check_updates=False)
        self.scanner.mod_found.connect(self.add_mod_to_table);
        self.scanner.start()

    def scan_folder(self):
        if not self.mods_folder: return
        self.table.setRowCount(0);
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
            self.download_folder = f;
            self.status_lbl.setText(f"Загрузка в: {f}")
            with open(CONFIG_FILE, 'w') as conf: json.dump({"download_folder": f}, conf)

    def _load_api_data(self):
        try:
            v_res = requests.get(f"{MODRINTH_API}/tag/game_version", timeout=5)
            if v_res.status_code == 200:
                self.version_box.addItems([v['version'] for v in v_res.json() if v.get('version_type') == 'release'])
            l_res = requests.get(f"{MODRINTH_API}/tag/loader", timeout=5)
            if l_res.status_code == 200:
                self.loader_box.addItems(sorted(
                    [l['name'].capitalize() for l in l_res.json() if "mod" in l.get("supported_project_types", [])]))
        except:
            pass

    def start_search(self):
        q = self.search_input.text().strip()
        if not q: return
        self.table.setRowCount(0);
        self.update_all_btn.hide()
        self.set_loading(True, "Поиск модов")
        self.worker = ModSearchWorker(q, self.loader_box.currentText(), self.version_box.currentText())

        def on_done(res, ok):
            if ok:
                for r in res: self.add_mod_to_table(r)
            self.set_loading(False)

        self.worker.results_ready.connect(on_done);
        self.worker.start()

    def add_mod_to_table(self, res):
        row = self.table.rowCount();
        self.table.insertRow(row)
        self.table.setItem(row, 0, QTableWidgetItem(res["title"]))
        self.table.setItem(row, 1, QTableWidgetItem(res["author"]))
        self.table.setItem(row, 2, QTableWidgetItem(res["version"]))
        st_item = QTableWidgetItem(res["status"])
        if res.get("needs_update"):
            st_item.setForeground(QColor("#e67e22"));
            self.update_all_btn.show()
        elif "Актуально" in res["status"]:
            st_item.setForeground(QColor("#27ae60"))
        self.table.setItem(row, 3, st_item)
        pbar = QProgressBar();
        pbar.setFixedHeight(12);
        pbar.setTextVisible(False);
        self.table.setCellWidget(row, 4, pbar)
        if res.get("url"):
            btn = QPushButton("Обновить" if res.get("needs_update") else "Скачать")
            btn.clicked.connect(partial(self.download, row, res["url"], res["filename"]))
            self.table.setCellWidget(row, 5, btn)

    def update_all_mods(self):
        for r in range(self.table.rowCount()):
            btn = self.table.cellWidget(r, 5)
            if isinstance(btn, QPushButton) and btn.text() == "Обновить": btn.click()

    def download(self, row, url, filename):
        save_dir = self.download_folder if self.download_folder else self.mods_folder
        if not save_dir:
            QMessageBox.warning(self, "!", "Выберите папку для сохранения!");
            return
        dest = os.path.join(save_dir, filename);
        pbar = self.table.cellWidget(row, 4);
        btn = self.table.cellWidget(row, 5)
        btn.setEnabled(False);
        downloader = DownloadThread(url, dest)
        downloader.progress.connect(pbar.setValue);
        downloader.finished.connect(lambda: btn.setText("Ок"))
        downloader.start();
        self.active_downloads.append(downloader)


if __name__ == "__main__":
    app = QApplication(sys.argv);
    w = ModManagerApp();
    w.show();
    sys.exit(app.exec())