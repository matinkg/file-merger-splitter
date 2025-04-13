# -*- coding: utf-8 -*-
import sys
import os
import re
import pathlib
import traceback  # Added for detailed error logging
from PyQt6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
    QFileDialog, QListWidget, QListWidgetItem, QLabel, QTextEdit,
    QMessageBox, QProgressBar, QSizePolicy, QStyleFactory, QStyle,
    QDialog, QTreeView, QDialogButtonBox, QScrollArea, QTabWidget, QSpacerItem
)
from PyQt6.QtGui import QStandardItemModel, QStandardItem, QIcon, QPalette, QColor
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QObject, QDir

# --- Constants ---
START_DELIMITER_FORMAT = "--- START FILE: {filepath} ---"
END_DELIMITER_FORMAT = "--- END FILE: {filepath} ---"
# Regex to find the start delimiter and capture the filepath
START_DELIMITER_REGEX = re.compile(r"^--- START FILE: (.*) ---$")

# --- Worker Signals ---


class WorkerSignals(QObject):
    ''' Defines signals available from a running worker thread. '''
    progress = pyqtSignal(int)       # Percentage progress
    log = pyqtSignal(str)            # Log message
    finished = pyqtSignal(bool, str)  # Success (bool), final message (str)
    error = pyqtSignal(str)          # Error message

# --- Merger Worker ---


class MergerWorker(QObject):
    ''' Performs the file merging in a separate thread. '''
    signals = WorkerSignals()

    def __init__(self, items_to_merge, output_file):
        super().__init__()
        self.items_to_merge = items_to_merge
        self.output_file = output_file
        self.is_running = True

    def stop(self):
        self.is_running = False

    def run(self):
        self.signals.log.emit(f"Starting merge process -> {self.output_file}")
        files_to_process = []
        total_size = 0
        processed_size = 0
        processed_files_count = 0

        try:
            self.signals.log.emit("Scanning files and folders...")
            initial_item_count = len(self.items_to_merge)
            files_discovered_in_scan = []

            for item_idx, (item_type, item_path_str, base_path_str) in enumerate(self.items_to_merge):
                if not self.is_running:
                    break
                item_path = pathlib.Path(item_path_str)
                base_path = pathlib.Path(base_path_str)
                relative_base = base_path.parent if base_path.parent else base_path

                if item_type == "file":
                    if item_path.is_file():
                        try:
                            relative_path = item_path.relative_to(
                                relative_base)
                            fsize = item_path.stat().st_size
                            files_discovered_in_scan.append(
                                (item_path, relative_path, fsize))
                            total_size += fsize
                        except ValueError:
                            self.signals.log.emit(
                                f"Warning: Could not determine relative path for {item_path} against {relative_base}. Skipping.")
                        except OSError as e:
                            self.signals.log.emit(
                                f"Warning: Could not get size for {item_path}: {e}. Skipping size calculation.")
                            files_discovered_in_scan.append(
                                (item_path, relative_path, 0))
                        except Exception as e:
                            self.signals.log.emit(
                                f"Warning: Error processing file {item_path}: {e}")
                    else:
                        self.signals.log.emit(
                            f"Warning: File not found during scan: {item_path}")
                elif item_type == "folder":
                    if item_path.is_dir():
                        self.signals.log.emit(f"Scanning folder: {item_path}")
                        for root, _, filenames in os.walk(item_path):
                            if not self.is_running:
                                break
                            root_path = pathlib.Path(root)
                            for filename in filenames:
                                if not self.is_running:
                                    break
                                file_path = root_path / filename
                                try:
                                    relative_path = file_path.relative_to(
                                        relative_base)
                                    fsize = file_path.stat().st_size
                                    files_discovered_in_scan.append(
                                        (file_path, relative_path, fsize))
                                    total_size += fsize
                                except ValueError:
                                    self.signals.log.emit(
                                        f"Warning: Could not determine relative path for {file_path} against {relative_base}. Skipping.")
                                except OSError as e:
                                    self.signals.log.emit(
                                        f"Warning: Could not get size for {file_path}: {e}. Skipping size calc.")
                                    files_discovered_in_scan.append(
                                        (file_path, relative_path, 0))
                                except Exception as e:
                                    self.signals.log.emit(
                                        f"Warning: Could not process file {file_path}: {e}")
                        if not self.is_running:
                            break
                    else:
                        self.signals.log.emit(
                            f"Warning: Folder not found during scan: {item_path}")

            files_to_process = sorted(
                list(set(files_discovered_in_scan)), key=lambda x: x[1])

            if not self.is_running:
                self.signals.log.emit("Merge cancelled during scan.")
                self.signals.finished.emit(False, "Merge cancelled.")
                return

            if not files_to_process:
                self.signals.log.emit("No valid files found to merge.")
                self.signals.finished.emit(False, "No files merged.")
                return

            self.signals.log.emit(
                f"Found {len(files_to_process)} unique files to merge. Total size: {total_size} bytes.")
            self.signals.progress.emit(0)

            with open(self.output_file, "w", encoding="utf-8", errors='replace') as outfile:
                total_files_count = len(files_to_process)
                for i, (file_path, relative_path, fsize) in enumerate(files_to_process):
                    if not self.is_running:
                        break

                    relative_path_str = str(relative_path.as_posix())
                    self.signals.log.emit(
                        f"Merging ({i+1}/{total_files_count}): {relative_path_str}")

                    start_delimiter = START_DELIMITER_FORMAT.format(
                        filepath=relative_path_str)
                    end_delimiter = END_DELIMITER_FORMAT.format(
                        filepath=relative_path_str)

                    outfile.write(start_delimiter + "\n")
                    content_written = False
                    try:
                        try:
                            with open(file_path, "r", encoding="utf-8") as infile:
                                content = infile.read()
                        except UnicodeDecodeError:
                            self.signals.log.emit(
                                f"Warning: Non-UTF-8 file detected: {relative_path_str}. Reading with 'latin-1'.")
                            with open(file_path, "r", encoding="latin-1") as infile:
                                content = infile.read()
                        except FileNotFoundError:
                            self.signals.log.emit(
                                f"Error: File disappeared during merge: {file_path}. Skipping.")
                            content = f"Error: File not found during merge process."
                        except Exception as e:
                            self.signals.log.emit(
                                f"Error reading file {file_path}: {e}. Skipping content.")
                            content = f"Error reading file: {e}"

                        outfile.write(content)
                        content_written = True
                        if content and not content.endswith('\n'):
                            outfile.write("\n")

                    except Exception as e:
                        self.signals.log.emit(
                            f"Error processing file {file_path}: {e}")
                        if not content_written:
                            outfile.write(f"Error processing file: {e}\n")

                    outfile.write(end_delimiter + "\n\n")

                    processed_size += fsize
                    processed_files_count += 1
                    if total_size > 0:
                        progress_percent = int(
                            (processed_size / total_size) * 100)
                        self.signals.progress.emit(progress_percent)
                    elif total_files_count > 0:
                        self.signals.progress.emit(
                            int((processed_files_count / total_files_count) * 100))

            if not self.is_running:
                self.signals.log.emit("Merge cancelled during writing.")
                try:
                    if os.path.exists(self.output_file):
                        os.remove(self.output_file)
                        self.signals.log.emit(
                            f"Removed incomplete file: {self.output_file}")
                except OSError as e:
                    self.signals.log.emit(
                        f"Could not remove incomplete file: {e}")
                self.signals.finished.emit(False, "Merge cancelled.")
            else:
                self.signals.progress.emit(100)
                self.signals.log.emit("Merge process completed successfully.")
                self.signals.finished.emit(True, "Merge successful!")

        except Exception as e:
            self.signals.log.emit(
                f"An error occurred during merge: {e}\n{traceback.format_exc()}")
            self.signals.error.emit(f"Merge failed: {e}")
            self.signals.finished.emit(False, "Merge failed.")

# --- Splitter Worker ---


class SplitterWorker(QObject):
    ''' Performs the file splitting in a separate thread. '''
    signals = WorkerSignals()

    def __init__(self, merged_file, output_dir):
        super().__init__()
        self.merged_file = merged_file
        self.output_dir = pathlib.Path(output_dir)
        self.is_running = True

    def stop(self):
        self.is_running = False

    def run(self):
        self.signals.log.emit(
            f"Starting split process for: {self.merged_file}")
        self.signals.log.emit(f"Output directory: {self.output_dir}")
        self.signals.progress.emit(0)

        try:
            total_size = os.path.getsize(self.merged_file)
            processed_size = 0
            file_count = 0

            with open(self.merged_file, "r", encoding="utf-8", errors='replace') as infile:
                current_file_path_relative = None
                current_file_content = []
                in_file_block = False

                for line_num, line in enumerate(infile):
                    if not self.is_running:
                        break

                    processed_size += len(line.encode('utf-8',
                                          errors='replace'))
                    if total_size > 0:
                        self.signals.progress.emit(
                            int((processed_size / total_size) * 100))

                    line_stripped = line.strip()
                    start_match = START_DELIMITER_REGEX.match(line_stripped)

                    if start_match:
                        if in_file_block:
                            self.signals.log.emit(
                                f"Warning: Found new START delimiter for '{start_match.group(1)}' before END delimiter for '{current_file_path_relative}' near line {line_num+1}. Saving previous block.")
                            if self._write_file(current_file_path_relative, "".join(current_file_content)):
                                file_count += 1

                        current_file_path_relative = start_match.group(1)
                        safe_path = True
                        if os.path.isabs(current_file_path_relative):
                            safe_path = False

                        if not safe_path:
                            self.signals.log.emit(
                                f"Error: Invalid or potentially unsafe path found in delimiter: '{current_file_path_relative}'. Skipping block.")
                            current_file_path_relative = None
                            in_file_block = False
                            continue

                        current_file_content = []
                        in_file_block = True
                        self.signals.log.emit(
                            f"Found file block: {current_file_path_relative}")
                        continue

                    if in_file_block and current_file_path_relative:
                        expected_end_delimiter = END_DELIMITER_FORMAT.format(
                            filepath=current_file_path_relative)
                        if line_stripped == expected_end_delimiter:
                            if self._write_file(current_file_path_relative, "".join(current_file_content)):
                                file_count += 1
                            in_file_block = False
                            current_file_path_relative = None
                            current_file_content = []
                        else:
                            current_file_content.append(line)

                if not self.is_running:
                    self.signals.log.emit("Split cancelled.")
                    self.signals.finished.emit(False, "Split cancelled.")
                    return

                if in_file_block and current_file_path_relative:
                    self.signals.log.emit(
                        f"Warning: Merged file ended before finding END delimiter for '{current_file_path_relative}'. Saving partial content.")
                    if self._write_file(current_file_path_relative, "".join(current_file_content)):
                        file_count += 1

            self.signals.progress.emit(100)
            self.signals.log.emit(
                f"Split process completed. {file_count} files potentially created (check log for errors).")
            self.signals.finished.emit(
                True, f"Split successful! {file_count} files processed.")

        except FileNotFoundError:
            self.signals.log.emit(
                f"Error: Merged file not found: {self.merged_file}")
            self.signals.error.emit(
                f"Merged file not found: {self.merged_file}")
            self.signals.finished.emit(
                False, "Split failed: Input file not found.")
        except Exception as e:
            self.signals.log.emit(
                f"An critical error occurred during split: {e}\n{traceback.format_exc()}")
            self.signals.error.emit(f"Split failed: {e}")
            self.signals.finished.emit(False, "Split failed.")
        finally:
            self.signals.progress.emit(100)

    def _write_file(self, relative_path_str, content):
        """Helper to write content to the appropriate file. Returns True on success, False on failure."""
        if not relative_path_str:
            self.signals.log.emit(
                "Error: Attempted to write file with no path. Skipping.")
            return False

        try:
            target_path = self.output_dir.joinpath(relative_path_str).resolve()
            output_dir_resolved = self.output_dir.resolve()

            if not str(target_path).startswith(str(output_dir_resolved)):
                self.signals.log.emit(
                    f"Error: Security risk! Path '{relative_path_str}' attempted traversal outside output directory '{output_dir_resolved}' -> '{target_path}'. Skipping.")
                return False
            if output_dir_resolved != target_path and output_dir_resolved not in target_path.parents:
                self.signals.log.emit(
                    f"Error: Security risk! Path '{relative_path_str}' resolved outside output directory '{output_dir_resolved}' -> '{target_path}'. Skipping.")
                return False

            self.signals.log.emit(f"Creating file: {target_path}")
            target_path.parent.mkdir(parents=True, exist_ok=True)
            with open(target_path, "w", encoding="utf-8") as outfile:
                outfile.write(content)
            return True

        except OSError as e:
            self.signals.log.emit(
                f"Error writing file {relative_path_str} (OS Error): {e}")
            return False
        except Exception as e:
            self.signals.log.emit(
                f"Error writing file {relative_path_str} (General Error): {e}")
            return False

# --- Folder Selection Dialog ---


class FolderSelectionDialog(QDialog):
    """A dialog to select specific files and subfolders within a chosen folder."""

    def __init__(self, folder_path_str, parent=None):
        super().__init__(parent)
        self.folder_path = pathlib.Path(folder_path_str)
        self.selected_items = []

        self.setWindowTitle(f"Select items in: {self.folder_path.name}")
        self.setMinimumSize(450, 400)
        self.setSizeGripEnabled(True)

        layout = QVBoxLayout(self)
        self.tree_view = QTreeView()
        self.tree_view.setHeaderHidden(True)
        self.model = QStandardItemModel()
        self.tree_view.setModel(self.model)

        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setWidget(self.tree_view)

        layout.addWidget(
            QLabel(f"Select items to include from:\n<b>{self.folder_path}</b>"))
        layout.addWidget(scroll_area, 1)

        self.populate_tree()

        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def populate_tree(self):
        """Scans the folder and populates the tree view."""
        self.model.clear()
        root_node = self.model.invisibleRootItem()
        try:
            # Use icons appropriate for dark theme (often requires custom icons or letting system handle it)
            # For now, rely on the style or provide placeholder icons
            folder_icon = self.style().standardIcon(QStyle.StandardPixmap.SP_DirIcon)
            file_icon = self.style().standardIcon(QStyle.StandardPixmap.SP_FileIcon)
        except Exception:
            folder_icon, file_icon = QIcon(), QIcon()  # Fallback

        try:
            items_in_dir = sorted(list(self.folder_path.iterdir()), key=lambda p: (
                not p.is_dir(), p.name.lower()))
            if not items_in_dir:
                no_items_label = QStandardItem("(Folder is empty)")
                no_items_label.setFlags(
                    Qt.ItemFlag.ItemIsEnabled)  # Not checkable
                # Explicitly set text color for disabled/info items if needed by theme
                # no_items_label.setForeground(QColor("#aaaaaa"))
                root_node.appendRow(no_items_label)
                return

            for item_path in items_in_dir:
                item = QStandardItem(item_path.name)
                # Set default check state before flags if it matters for signals (here it's likely fine)
                item.setCheckState(Qt.CheckState.Checked)
                item.setFlags(Qt.ItemFlag.ItemIsUserCheckable |
                              Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable)

                item_type = None
                if item_path.is_dir():
                    item_type = "folder"
                    item.setIcon(folder_icon)
                elif item_path.is_file():
                    item_type = "file"
                    item.setIcon(file_icon)
                else:
                    # Make unsupported items visually distinct and non-interactive
                    # Not selectable or checkable
                    item.setFlags(Qt.ItemFlag.ItemIsEnabled)
                    item.setText(f"{item_path.name} (Unsupported Type)")
                    item.setData(None, Qt.ItemDataRole.UserRole)
                    # item.setForeground(QColor("#aaaaaa")) # Dim color for unsupported

                if item_type:
                    item.setData((item_type, str(item_path)),
                                 Qt.ItemDataRole.UserRole)

                root_node.appendRow(item)
        except OSError as e:
            QMessageBox.warning(self, "Error Reading Folder",
                                f"Could not read folder contents:\n{e}")
        except Exception as e:
            QMessageBox.critical(self, "Error Populating Tree",
                                 f"An unexpected error occurred:\n{e}")

    def accept(self):
        """Process selections when OK is clicked."""
        self.selected_items = []
        root = self.model.invisibleRootItem()
        base_path_str = str(self.folder_path)

        for row in range(root.rowCount()):
            item = root.child(row, 0)
            # Check if item is checkable AND checked
            if item and item.flags() & Qt.ItemFlag.ItemIsUserCheckable and item.checkState() == Qt.CheckState.Checked:
                item_data = item.data(Qt.ItemDataRole.UserRole)
                if item_data:
                    item_type, item_path_str = item_data
                    self.selected_items.append(
                        (item_type, item_path_str, base_path_str))
        super().accept()

    def get_selected_items(self):
        """Return the list of selected (type, path, base_path) tuples."""
        return self.selected_items

# --- Main Application Window ---


class MergerSplitterApp(QWidget):
    DARK_STYLESHEET = """
        /* General Widget Styles */
        QWidget {
            font-size: 10pt;
            color: #e0e0e0; /* Light text for dark background */
            background-color: #2b2b2b; /* Dark background */
            border-color: #555555; /* Darker border */
        }

        /* Main Window Background */
        MergerSplitterAppWindow {
             background-color: #2b2b2b;
        }

        /* Labels */
        QLabel {
            background-color: transparent; /* Ensure labels don't block background */
            padding: 2px;
            color: #dcdcdc; /* Slightly dimmer text for non-critical labels */
        }
        QLabel[text*="<b>"] {
            font-weight: bold;
            color: #e0e0e0; /* Brighter text for titles */
        }
        /* Labels showing file paths */
        QLabel#OutputMergeLabel, QLabel#InputSplitLabel, QLabel#OutputSplitLabel {
            color: #cccccc;
            padding-left: 5px;
        }


        /* Buttons */
        QPushButton {
            padding: 6px 12px;
            border: 1px solid #555555;
            border-radius: 4px;
            background-color: #3c3c3c; /* Button background */
            color: #e0e0e0;
            min-width: 80px;
        }
        QPushButton:hover {
            background-color: #4f4f4f; /* Slightly lighter hover */
            border-color: #666666;
        }
        QPushButton:pressed {
            background-color: #569cd6; /* Accent color press */
            color: #ffffff;
            border-color: #569cd6;
        }
        QPushButton:disabled {
            background-color: #404040; /* Darker disabled background */
            color: #777777; /* Dim disabled text */
            border-color: #484848;
        }
        /* Specific Action Buttons */
        QPushButton#MergeButton, QPushButton#SplitButton {
             padding: 8px 18px;
             font-size: 11pt;
             font-weight: bold;
             background-color: #4a5d75; /* Slightly different background for action */
        }
        QPushButton#MergeButton:hover, QPushButton#SplitButton:hover {
             background-color: #5a6d85;
        }
        QPushButton#MergeButton:pressed, QPushButton#SplitButton:pressed {
             background-color: #569cd6;
        }

        QPushButton#MergeCancelButton, QPushButton#SplitCancelButton {
             min-width: 60px;
             padding: 6px 10px;
             background-color: #6e3c3c; /* Reddish background for cancel */
        }
        QPushButton#MergeCancelButton:hover, QPushButton#SplitCancelButton:hover {
             background-color: #8e4c4c;
        }
         QPushButton#MergeCancelButton:pressed, QPushButton#SplitCancelButton:pressed {
             background-color: #a05656;
        }


        /* List Widget */
        QListWidget {
            border: 1px solid #555555;
            border-radius: 4px;
            background-color: #333333; /* Slightly different dark bg */
            color: #dcdcdc;
            margin: 2px;
            alternate-background-color: #3a3a3a; /* Subtle alternating color */
        }
        QListWidget::item:selected {
            background-color: #569cd6; /* Accent selection color */
            color: #ffffff; /* White text on selection */
        }
        QListWidget::item:hover {
            background-color: #4f4f4f; /* Hover effect */
        }


        /* Text Edit (Log Area) */
        QTextEdit {
            border: 1px solid #555555;
            border-radius: 4px;
            background-color: #303030; /* Darker background for log */
            font-family: Consolas, Courier New, monospace;
            color: #cccccc; /* Log text color */
            selection-background-color: #569cd6; /* Selection color */
            selection-color: #ffffff;
        }

        /* Progress Bar */
        QProgressBar {
            border: 1px solid #555555;
            border-radius: 5px;
            text-align: center;
            color: #ffffff; /* Text on top of the bar */
            background-color: #444444; /* Background of the bar track */
            font-weight: bold;
        }
        QProgressBar::chunk {
            background-color: #569cd6; /* Accent color for progress chunk */
            border-radius: 4px;
            margin: 1px; /* Optional margin for chunk */
        }

        /* Tab Widget */
        QTabWidget::pane {
            border: 1px solid #555555;
            border-radius: 4px;
            background-color: #333333; /* Pane background */
            margin-top: -1px;
            padding: 10px;
        }
        QTabBar::tab {
            background: #3c3c3c; /* Non-selected tab background */
            border: 1px solid #555555;
            border-bottom: none; /* Remove bottom border */
            border-top-left-radius: 4px;
            border-top-right-radius: 4px;
            min-width: 8ex;
            padding: 6px 12px;
            margin-right: 2px;
            color: #bbbbbb; /* Dimmer text for non-selected tabs */
        }
        QTabBar::tab:hover {
            background: #4f4f4f; /* Hover for non-selected */
            color: #e0e0e0;
        }
        QTabBar::tab:selected {
            background: #333333; /* Selected tab matches pane */
            border: 1px solid #555555;
            border-bottom: 1px solid #333333; /* Blend bottom border with pane */
            margin-bottom: -1px; /* Overlap pane slightly */
            font-weight: bold;
            color: #e0e0e0; /* Brighter text for selected tab */
        }
        QTabBar::tab:!selected {
            margin-top: 2px; /* Push non-selected tabs down slightly */
        }

        /* ToolTips */
        QToolTip {
            border: 1px solid #666666;
            padding: 5px;
            background-color: #424242; /* Dark tooltip background */
            color: #e0e0e0; /* Light text */
            opacity: 240;
        }

        /* Tree View (in Dialog) */
        QTreeView {
            background-color: #333333;
            color: #dcdcdc;
            border: 1px solid #555555;
            border-radius: 4px;
            alternate-background-color: #3a3a3a;
        }
        QTreeView::item:selected {
            background-color: #569cd6; /* Accent selection */
            color: #ffffff;
        }
         QTreeView::item:hover:!selected {
            background-color: #4f4f4f;
        }
        /* Style the check boxes within the tree */
         QTreeView::indicator {
            width: 13px;
            height: 13px;
         }
         QTreeView::indicator:unchecked {
            /* Consider using image for custom dark theme checkbox */
            /* image: url(:/dark/checkbox_unchecked.png); */
         }
         QTreeView::indicator:checked {
            /* image: url(:/dark/checkbox_checked.png); */
         }


        /* Dialogs */
        QDialog {
            background-color: #2b2b2b; /* Match main window background */
        }
        QDialogButtonBox QPushButton { /* Buttons inside dialog boxes */
            min-width: 70px;
        }

        /* Scroll Bars */
        QScrollBar:vertical {
            border: 1px solid #444444;
            background: #303030;
            width: 12px;
            margin: 0px 0px 0px 0px;
        }
        QScrollBar::handle:vertical {
            background: #555555;
            min-height: 20px;
            border-radius: 4px;
        }
         QScrollBar::handle:vertical:hover {
            background: #666666;
        }
        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
            height: 0px; /* Hide arrows */
            background: none;
        }
        QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {
            background: none;
        }

        QScrollBar:horizontal {
            border: 1px solid #444444;
            background: #303030;
            height: 12px;
            margin: 0px 0px 0px 0px;
        }
        QScrollBar::handle:horizontal {
            background: #555555;
            min-width: 20px;
             border-radius: 4px;
        }
         QScrollBar::handle:horizontal:hover {
            background: #666666;
        }
        QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {
             width: 0px; /* Hide arrows */
             background: none;
        }
        QScrollBar::add-page:horizontal, QScrollBar::sub-page:horizontal {
            background: none;
        }

    """

    def __init__(self):
        super().__init__()
        self.items_to_merge = []
        self.output_merge_file = ""
        self.input_split_file = ""
        self.output_split_dir = ""
        self.worker_thread = None
        self.worker = None

        self.initUI()
        self.apply_dark_style()  # Changed method name

    def initUI(self):
        self.setWindowTitle('File Merger & Splitter')
        self.setGeometry(150, 150, 750, 650)

        main_layout = QVBoxLayout(self)
        self.tab_widget = QTabWidget()
        main_layout.addWidget(self.tab_widget, 1)

        self.merge_tab = QWidget()
        self.split_tab = QWidget()
        self.tab_widget.addTab(self.merge_tab, "Merge")
        self.tab_widget.addTab(self.split_tab, "Split")

        # --- Populate Merge Tab ---
        merge_layout = QVBoxLayout(self.merge_tab)

        select_items_layout = QHBoxLayout()
        self.add_files_button = QPushButton("Add Files")
        self.add_folder_button = QPushButton("Add Folder...")
        self.remove_item_button = QPushButton("Remove Selected")
        self.clear_list_button = QPushButton("Clear List")
        select_items_layout.addWidget(self.add_files_button)
        select_items_layout.addWidget(self.add_folder_button)
        select_items_layout.addWidget(self.remove_item_button)
        select_items_layout.addWidget(self.clear_list_button)
        select_items_layout.addStretch()
        merge_layout.addLayout(select_items_layout)

        self.item_list_widget = QListWidget()
        # self.item_list_widget.setAlternatingRowColors(True) # Style handles this
        self.item_list_widget.setSelectionMode(
            QListWidget.SelectionMode.ExtendedSelection)
        merge_layout.addWidget(QLabel("Items to Merge:"))
        merge_layout.addWidget(self.item_list_widget, 1)

        output_merge_layout = QHBoxLayout()
        self.select_output_merge_button = QPushButton(
            "Select Output Merged File (.txt)")
        self.output_merge_label = QLabel("Output: [Not Selected]")
        self.output_merge_label.setObjectName(
            "OutputMergeLabel")  # For styling
        self.output_merge_label.setWordWrap(True)
        output_merge_layout.addWidget(self.select_output_merge_button)
        output_merge_layout.addWidget(self.output_merge_label, 1)
        merge_layout.addLayout(output_merge_layout)

        merge_actions_layout = QHBoxLayout()
        merge_actions_layout.addStretch()
        self.merge_button = QPushButton("Merge")
        self.merge_button.setObjectName("MergeButton")
        self.merge_cancel_button = QPushButton("Cancel")
        self.merge_cancel_button.setObjectName("MergeCancelButton")
        self.merge_cancel_button.setEnabled(False)
        merge_actions_layout.addWidget(self.merge_button)
        merge_actions_layout.addWidget(self.merge_cancel_button)
        merge_actions_layout.addStretch()
        merge_layout.addLayout(merge_actions_layout)

        # --- Populate Split Tab ---
        split_layout = QVBoxLayout(self.split_tab)

        input_split_layout = QHBoxLayout()
        self.select_input_split_button = QPushButton(
            "Select Merged File (.txt)")
        self.input_split_label = QLabel("Input: [Not Selected]")
        self.input_split_label.setObjectName("InputSplitLabel")  # For styling
        self.input_split_label.setWordWrap(True)
        input_split_layout.addWidget(self.select_input_split_button)
        input_split_layout.addWidget(self.input_split_label, 1)
        split_layout.addLayout(input_split_layout)

        output_split_layout = QHBoxLayout()
        self.select_output_split_button = QPushButton("Select Output Folder")
        self.output_split_label = QLabel("Output Dir: [Not Selected]")
        self.output_split_label.setObjectName(
            "OutputSplitLabel")  # For styling
        self.output_split_label.setWordWrap(True)
        output_split_layout.addWidget(self.select_output_split_button)
        output_split_layout.addWidget(self.output_split_label, 1)
        split_layout.addLayout(output_split_layout)

        split_actions_layout = QHBoxLayout()
        split_actions_layout.addStretch()
        self.split_button = QPushButton("Split")
        self.split_button.setObjectName("SplitButton")
        self.split_cancel_button = QPushButton("Cancel")
        self.split_cancel_button.setObjectName("SplitCancelButton")
        self.split_cancel_button.setEnabled(False)
        split_actions_layout.addWidget(self.split_button)
        split_actions_layout.addWidget(self.split_cancel_button)
        split_actions_layout.addStretch()
        split_layout.addLayout(split_actions_layout)

        split_layout.addStretch(1)

        # --- Shared Controls (Log, Progress Bar) Below Tabs ---
        shared_controls_layout = QVBoxLayout()
        shared_controls_layout.addWidget(QLabel("<b>Log / Status</b>"))
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setLineWrapMode(QTextEdit.LineWrapMode.NoWrap)
        self.log_text.setFixedHeight(150)
        shared_controls_layout.addWidget(self.log_text)

        self.progress_bar = QProgressBar()
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setValue(0)
        self.progress_bar.setFixedHeight(20)
        shared_controls_layout.addWidget(self.progress_bar)
        main_layout.addLayout(shared_controls_layout)

        # --- Connect Signals ---
        self.add_files_button.clicked.connect(self.add_files)
        self.add_folder_button.clicked.connect(self.add_folder)
        self.remove_item_button.clicked.connect(self.remove_selected_items)
        self.clear_list_button.clicked.connect(self.clear_item_list)
        self.select_output_merge_button.clicked.connect(
            self.select_output_merge_file)
        self.merge_button.clicked.connect(self.start_merge)
        self.merge_cancel_button.clicked.connect(self.cancel_operation)

        self.select_input_split_button.clicked.connect(
            self.select_input_split_file)
        self.select_output_split_button.clicked.connect(
            self.select_output_split_dir)
        self.split_button.clicked.connect(self.start_split)
        self.split_cancel_button.clicked.connect(self.cancel_operation)

    def apply_dark_style(self):
        """Applies the dark mode stylesheet and Fusion style."""
        # Set Fusion style first, as it provides a good base
        try:
            QApplication.setStyle(QStyleFactory.create('Fusion'))
        except Exception as e:
            # Use print for early logs
            print(f"Warning: Could not apply Fusion style: {e}")

        # Apply the custom dark stylesheet
        self.setStyleSheet(self.DARK_STYLESHEET)

        # Load Icons (Consider using icons designed for dark themes if available)
        try:
            # Standard icons might look okay, but custom dark-theme icons are better
            merge_icon = self.style().standardIcon(QStyle.StandardPixmap.SP_MediaPlay)
            split_icon = self.style().standardIcon(QStyle.StandardPixmap.SP_ArrowRight)
            cancel_icon = self.style().standardIcon(
                QStyle.StandardPixmap.SP_DialogCancelButton)
            file_icon = self.style().standardIcon(QStyle.StandardPixmap.SP_FileIcon)
            folder_icon = self.style().standardIcon(QStyle.StandardPixmap.SP_DirOpenIcon)
            remove_icon = self.style().standardIcon(
                QStyle.StandardPixmap.SP_DialogDiscardButton)
            clear_icon = self.style().standardIcon(QStyle.StandardPixmap.SP_TrashIcon)

            self.add_files_button.setIcon(file_icon)
            self.add_folder_button.setIcon(folder_icon)
            self.remove_item_button.setIcon(remove_icon)
            self.clear_list_button.setIcon(clear_icon)
            self.merge_button.setIcon(merge_icon)
            self.split_button.setIcon(split_icon)
            self.merge_cancel_button.setIcon(cancel_icon)
            self.split_cancel_button.setIcon(cancel_icon)
        except Exception as e:
            # Log might not be ready yet if called very early
            print(f"Warning: Could not load standard icons: {e}")

        # Log that the style was applied *after* the UI is likely initialized
        self.log("Applied dark theme stylesheet.")

    def log(self, message):
        if hasattr(self, 'log_text') and self.log_text:
            self.log_text.append(message)
            self.log_text.ensureCursorVisible()
        else:
            print(f"LOG (pre-init): {message}")

    def update_progress(self, value):
        self.progress_bar.setValue(value)

    def operation_finished(self, success, message):
        self.log(f"Finished: {message}")
        self.progress_bar.setValue(100)
        if success:
            QMessageBox.information(self, "Operation Complete", message)
        else:
            if not hasattr(self, '_error_shown') or not self._error_shown:
                QMessageBox.warning(self, "Operation Finished", message)
        self._reset_error_flag()
        self._set_ui_enabled(True)
        if self.worker_thread:
            self.worker_thread.quit()
            if not self.worker_thread.wait(1000):
                self.log(
                    "Warning: Worker thread didn't finish quitting gracefully.")
            self.worker_thread = None
        self.worker = None

    def operation_error(self, error_message):
        self.log(f"ERROR: {error_message}")
        QMessageBox.critical(self, "Error", error_message)
        self._error_shown = True

    def _reset_error_flag(self):
        if hasattr(self, '_error_shown'):
            del self._error_shown

    def _set_ui_enabled(self, enabled):
        """Enable/disable UI elements during processing."""
        self.merge_tab.setEnabled(enabled)
        self.split_tab.setEnabled(enabled)
        self.tab_widget.tabBar().setEnabled(enabled)

        # Cancel buttons are enabled ONLY when an operation is running (i.e., UI is disabled)
        self.merge_cancel_button.setEnabled(not enabled)
        self.split_cancel_button.setEnabled(not enabled)

    def add_files(self):
        start_dir = str(pathlib.Path(
            self.items_to_merge[0][1]).parent) if self.items_to_merge else ""
        files, _ = QFileDialog.getOpenFileNames(
            self, "Select Files to Merge", start_dir)
        if files:
            added_count = 0
            for file_path_str in files:
                file_path = pathlib.Path(file_path_str)
                base_path = file_path.parent
                item_data = ("file", str(file_path), str(base_path))

                if item_data not in self.items_to_merge:
                    self.items_to_merge.append(item_data)
                    item_text = f"[File] {file_path.name}  (in: {base_path.name})"
                    list_item = QListWidgetItem(item_text)
                    list_item.setToolTip(str(file_path))
                    list_item.setData(Qt.ItemDataRole.UserRole, item_data)
                    self.item_list_widget.addItem(list_item)
                    added_count += 1
            if added_count > 0:
                self.log(f"Added {added_count} file(s).")
            else:
                self.log("Selected file(s) already in the list.")

    def add_folder(self):
        start_dir = str(pathlib.Path(
            self.items_to_merge[0][1]).parent) if self.items_to_merge else ""
        folder_path_str = QFileDialog.getExistingDirectory(
            self, "Select Folder to Scan", start_dir)

        if folder_path_str:
            dialog = FolderSelectionDialog(folder_path_str, self)
            if dialog.exec() == QDialog.DialogCode.Accepted:
                selected_items = dialog.get_selected_items()
                if not selected_items:
                    self.log(
                        f"No items selected from folder: {folder_path_str}")
                    return

                added_count = 0
                folder_base_name = pathlib.Path(folder_path_str).name

                for item_type, item_path_str, base_path_str in selected_items:
                    item_path = pathlib.Path(item_path_str)
                    item_data = (item_type, item_path_str, base_path_str)

                    if item_data not in self.items_to_merge:
                        self.items_to_merge.append(item_data)
                        display_name = item_path.name
                        type_label = "[File]" if item_type == "file" else "[SubFolder]"
                        item_text = f"{type_label} {display_name} (from: {folder_base_name})"

                        list_item = QListWidgetItem(item_text)
                        list_item.setToolTip(
                            f"{item_type.capitalize()}: {item_path_str}\nBase: {base_path_str}")
                        list_item.setData(Qt.ItemDataRole.UserRole, item_data)
                        self.item_list_widget.addItem(list_item)
                        added_count += 1

                if added_count > 0:
                    self.log(
                        f"Added {added_count} selected item(s) from: {folder_path_str}")
                else:
                    self.log("Selected item(s) from folder already in the list.")
            else:
                self.log(f"Folder selection cancelled for: {folder_path_str}")

    def remove_selected_items(self):
        selected_items = self.item_list_widget.selectedItems()
        if not selected_items:
            self.log("No item(s) selected to remove.")
            return

        removed_count = 0
        for item in reversed(selected_items):
            item_data = item.data(Qt.ItemDataRole.UserRole)
            try:
                if item_data in self.items_to_merge:
                    self.items_to_merge.remove(item_data)
                    row = self.item_list_widget.row(item)
                    self.item_list_widget.takeItem(row)
                    removed_count += 1
                else:
                    self.log(
                        f"Warning: Item data mismatch, removing from view: {item.text()}")
                    row = self.item_list_widget.row(item)
                    self.item_list_widget.takeItem(row)
            except ValueError:
                self.log(
                    f"Warning: Value error during removal for: {item.text()}")
            except Exception as e:
                self.log(f"Error removing item {item.text()}: {e}")

        if removed_count > 0:
            self.log(f"Removed {removed_count} item(s).")

    def clear_item_list(self):
        self.item_list_widget.clear()
        self.items_to_merge.clear()
        self.log("Cleared item list.")

    def select_output_merge_file(self):
        start_dir = os.path.dirname(
            self.output_merge_file) if self.output_merge_file else ""
        file_path, file_filter = QFileDialog.getSaveFileName(
            self, "Save Merged File As", start_dir, "Text Files (*.txt);;All Files (*)")
        if file_path:
            if "." not in os.path.basename(file_path) and file_filter == "Text Files (*.txt)":
                file_path += ".txt"
            self.output_merge_file = file_path
            self.output_merge_label.setText(
                f"Output: {os.path.basename(file_path)}")
            self.output_merge_label.setToolTip(file_path)
            self.log(f"Selected merge output file: {file_path}")

    def select_input_split_file(self):
        start_dir = os.path.dirname(
            self.input_split_file) if self.input_split_file else ""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select Merged File", start_dir, "Text Files (*.txt);;All Files (*)")
        if file_path:
            self.input_split_file = file_path
            self.input_split_label.setText(
                f"Input: {os.path.basename(file_path)}")
            self.input_split_label.setToolTip(file_path)
            self.log(f"Selected split input file: {file_path}")

    def select_output_split_dir(self):
        start_dir = self.output_split_dir if self.output_split_dir else ""
        dir_path = QFileDialog.getExistingDirectory(
            self, "Select Output Directory for Split Files", start_dir)
        if dir_path:
            self.output_split_dir = dir_path
            self.output_split_label.setText(
                f"Output Dir: {os.path.basename(dir_path)}")
            self.output_split_label.setToolTip(dir_path)
            self.log(f"Selected split output directory: {dir_path}")

    def _create_output_dir_if_needed(self, dir_path, operation_name):
        """Checks if a directory exists and prompts to create it if not."""
        if dir_path and not os.path.isdir(dir_path):
            # Use QMessageBox for the dialog popup
            reply = QMessageBox.question(self, f"Create Directory for {operation_name}?",
                                         f"The directory does not exist:\n{dir_path}\n\nCreate it?",
                                         QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                         QMessageBox.StandardButton.Yes)
            if reply == QMessageBox.StandardButton.Yes:
                try:
                    os.makedirs(dir_path, exist_ok=True)
                    self.log(f"Created output directory: {dir_path}")
                    return True
                except OSError as e:
                    QMessageBox.critical(
                        self, f"{operation_name} Error", f"Could not create directory:\n{e}")
                    return False
            else:
                self.log(
                    f"{operation_name} cancelled by user (directory not created).")
                return False
        return True

    def start_merge(self):
        if not self.items_to_merge:
            QMessageBox.warning(self, "Merge Error",
                                "No files or folders selected to merge.")
            return
        if not self.output_merge_file:
            QMessageBox.warning(self, "Merge Error",
                                "Please select an output file first.")
            return
        if not self._create_output_dir_if_needed(os.path.dirname(self.output_merge_file), "Merge"):
            return

        if self.worker_thread and self.worker_thread.isRunning():
            QMessageBox.warning(
                self, "Busy", "Another operation is already in progress.")
            return

        self.log_text.clear()
        self.progress_bar.setValue(0)
        self._set_ui_enabled(False)
        self._reset_error_flag()

        self.worker_thread = QThread(self)
        self.worker = MergerWorker(
            list(self.items_to_merge), self.output_merge_file)
        self.worker.moveToThread(self.worker_thread)

        self.worker.signals.progress.connect(self.update_progress)
        self.worker.signals.log.connect(self.log)
        self.worker.signals.error.connect(self.operation_error)
        self.worker.signals.finished.connect(self.operation_finished)
        self.worker_thread.finished.connect(self.worker.deleteLater)
        self.worker_thread.finished.connect(self.worker_thread.deleteLater)

        self.worker_thread.started.connect(self.worker.run)
        self.worker_thread.start()

    def start_split(self):
        if not self.input_split_file:
            QMessageBox.warning(self, "Split Error",
                                "No merged file selected to split.")
            return
        if not os.path.exists(self.input_split_file):
            QMessageBox.critical(
                self, "Split Error", f"Input file does not exist:\n{self.input_split_file}")
            return
        if not self.output_split_dir:
            QMessageBox.warning(self, "Split Error",
                                "Please select an output directory first.")
            return
        if not self._create_output_dir_if_needed(self.output_split_dir, "Split"):
            return

        if self.worker_thread and self.worker_thread.isRunning():
            QMessageBox.warning(
                self, "Busy", "Another operation is already in progress.")
            return

        self.log_text.clear()
        self.progress_bar.setValue(0)
        self._set_ui_enabled(False)
        self._reset_error_flag()

        self.worker_thread = QThread(self)
        self.worker = SplitterWorker(
            self.input_split_file, self.output_split_dir)
        self.worker.moveToThread(self.worker_thread)

        self.worker.signals.progress.connect(self.update_progress)
        self.worker.signals.log.connect(self.log)
        self.worker.signals.error.connect(self.operation_error)
        self.worker.signals.finished.connect(self.operation_finished)
        self.worker_thread.finished.connect(self.worker.deleteLater)
        self.worker_thread.finished.connect(self.worker_thread.deleteLater)

        self.worker_thread.started.connect(self.worker.run)
        self.worker_thread.start()

    def cancel_operation(self):
        if self.worker and self.worker.is_running:
            self.log("Attempting to cancel operation...")
            self.worker.stop()
            self.merge_cancel_button.setEnabled(False)
            self.split_cancel_button.setEnabled(False)
        else:
            self.log("No operation is currently running to cancel.")

    def closeEvent(self, event):
        if self.worker and self.worker_thread and self.worker_thread.isRunning():
            self.log(
                "Closing application - Attempting to stop running operation...")
            self.cancel_operation()  # Trigger stop signal and disable buttons
            if not self.worker_thread.wait(2000):
                self.log(
                    "Warning: Worker thread did not terminate gracefully after stop signal during close.")
            else:
                self.log("Worker thread stopped during close.")
        event.accept()


# --- Main Execution ---
if __name__ == '__main__':
    # High DPI settings
    os.environ["QT_ENABLE_HIGHDPI_SCALING"] = "1"
    os.environ["QT_SCALE_FACTOR_ROUNDING_POLICY"] = "PassThrough"
    # For some Linux systems, may need AA_EnableHighDpiScaling too
    if sys.platform.startswith('linux'):
        QApplication.setAttribute(
            Qt.ApplicationAttribute.AA_EnableHighDpiScaling, True)

    app = QApplication(sys.argv)
    app.setApplicationName("File Merger & Splitter")
    app.setOrganizationName("YourCompanyName")  # Optional

    ex = MergerSplitterApp()
    ex.setObjectName("MergerSplitterAppWindow")  # For styling root window
    ex.show()
    sys.exit(app.exec())
