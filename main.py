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
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QObject, QDir, QModelIndex

# --- Constants ---
START_DELIMITER_FORMAT = "--- START FILE: {filepath} ---"
END_DELIMITER_FORMAT = "--- END FILE: {filepath} ---"
# Regex to find the start delimiter and capture the filepath
START_DELIMITER_REGEX = re.compile(r"^--- START FILE: (.*) ---$")

# --- Data Roles for Tree Items ---
# Use UserRole + N for custom data if needed, otherwise just UserRole
PATH_DATA_ROLE = Qt.ItemDataRole.UserRole + 1
TYPE_DATA_ROLE = Qt.ItemDataRole.UserRole + 2
BASE_PATH_DATA_ROLE = Qt.ItemDataRole.UserRole + 3


# --- Worker Signals ---
class WorkerSignals(QObject):
    ''' Defines signals available from a running worker thread. '''
    progress = pyqtSignal(int)       # Percentage progress
    log = pyqtSignal(str)            # Log message
    finished = pyqtSignal(bool, str)  # Success (bool), final message (str)
    error = pyqtSignal(str)          # Error message

# --- Merger Worker --- (No changes needed in MergerWorker logic itself)


class MergerWorker(QObject):
    ''' Performs the file merging in a separate thread. '''
    signals = WorkerSignals()

    def __init__(self, items_to_merge, output_file):
        super().__init__()
        # items_to_merge is expected to be a list of tuples:
        # ('file', '/path/to/file.txt', '/path/to')
        # ('folder', '/path/to/folder', '/path/to')
        self.items_to_merge = items_to_merge
        self.output_file = output_file
        self.is_running = True
        # Debug log
        self.log(f"Worker received {len(items_to_merge)} items to process.")

    def stop(self):
        self.is_running = False

    def log(self, msg):
        # Helper to emit log signals easily
        self.signals.log.emit(msg)

    def run(self):
        self.log(f"Starting merge process -> {self.output_file}")
        files_to_process = []
        total_size = 0
        processed_size = 0
        processed_files_count = 0
        encountered_paths = set()  # To avoid duplicates if selection overlaps

        try:
            self.log("Scanning files and folders based on input...")
            initial_item_count = len(self.items_to_merge)
            files_discovered_in_scan = []

            for item_idx, (item_type, item_path_str, base_path_str) in enumerate(self.items_to_merge):
                if not self.is_running:
                    break
                item_path = pathlib.Path(item_path_str)
                # The base_path_str passed from the selection determines the root for relative paths
                base_path = pathlib.Path(base_path_str)
                self.log(
                    f"Processing item: {item_type} - {item_path} (Base: {base_path})")

                if item_type == "file":
                    if item_path.is_file():
                        try:
                            # Calculate relative path based on the provided base_path
                            relative_path = item_path.relative_to(base_path)
                            fsize = item_path.stat().st_size
                            if item_path not in encountered_paths:
                                files_discovered_in_scan.append(
                                    (item_path, relative_path, fsize))
                                total_size += fsize
                                encountered_paths.add(item_path)
                            else:
                                self.log(
                                    f"Skipping duplicate file: {item_path}")
                        except ValueError:
                            self.log(
                                f"Warning: Could not determine relative path for {item_path} against base {base_path}. Using absolute path.")
                            # Fallback or skip? Using absolute might not be desired. Let's use filename as fallback.
                            relative_path = pathlib.Path(item_path.name)
                            if item_path not in encountered_paths:
                                fsize = item_path.stat().st_size  # Still try to get size
                                files_discovered_in_scan.append(
                                    (item_path, relative_path, fsize))
                                total_size += fsize
                                encountered_paths.add(item_path)
                            else:
                                self.log(
                                    f"Skipping duplicate file: {item_path}")
                        except OSError as e:
                            self.log(
                                f"Warning: Could not get size for {item_path}: {e}. Skipping size calculation.")
                            relative_path = item_path.relative_to(
                                base_path) if base_path in item_path.parents else pathlib.Path(item_path.name)
                            if item_path not in encountered_paths:
                                files_discovered_in_scan.append(
                                    (item_path, relative_path, 0))
                                encountered_paths.add(item_path)
                            else:
                                self.log(
                                    f"Skipping duplicate file: {item_path}")
                        except Exception as e:
                            self.log(
                                f"Warning: Error processing file entry {item_path}: {e}")
                    else:
                        self.log(
                            f"Warning: File not found during scan: {item_path}")

                elif item_type == "folder":
                    if item_path.is_dir():
                        self.log(
                            f"Scanning folder: {item_path} (Base for relative paths: {base_path})")
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
                                        base_path)
                                    fsize = file_path.stat().st_size
                                    if file_path not in encountered_paths:
                                        files_discovered_in_scan.append(
                                            (file_path, relative_path, fsize))
                                        total_size += fsize
                                        encountered_paths.add(file_path)
                                    else:
                                        self.log(
                                            f"Skipping duplicate file during folder scan: {file_path}")
                                except ValueError:
                                    self.log(
                                        f"Warning: Could not determine relative path for {file_path} against base {base_path}. Using path relative to scanned folder.")
                                    # Fallback to path relative to the item_path (the folder being walked)
                                    relative_path = file_path.relative_to(
                                        item_path)
                                    if file_path not in encountered_paths:
                                        fsize = file_path.stat().st_size  # Still try get size
                                        files_discovered_in_scan.append(
                                            (file_path, relative_path, fsize))
                                        total_size += fsize
                                        encountered_paths.add(file_path)
                                    else:
                                        self.log(
                                            f"Skipping duplicate file during folder scan: {file_path}")
                                except OSError as e:
                                    self.log(
                                        f"Warning: Could not get size for {file_path}: {e}. Skipping size calc.")
                                    relative_path = file_path.relative_to(
                                        base_path) if base_path in file_path.parents else file_path.relative_to(item_path)
                                    if file_path not in encountered_paths:
                                        files_discovered_in_scan.append(
                                            (file_path, relative_path, 0))
                                        encountered_paths.add(file_path)
                                    else:
                                        self.log(
                                            f"Skipping duplicate file during folder scan: {file_path}")

                                except Exception as e:
                                    self.log(
                                        f"Warning: Could not process file {file_path} in folder scan: {e}")
                        if not self.is_running:
                            break
                    else:
                        self.log(
                            f"Warning: Folder not found during scan: {item_path}")

            # Sort the unique files based on their relative paths
            files_to_process = sorted(
                files_discovered_in_scan, key=lambda x: x[1].as_posix())

            if not self.is_running:
                self.log("Merge cancelled during scan.")
                self.signals.finished.emit(False, "Merge cancelled.")
                return

            if not files_to_process:
                self.log("No valid, unique files found to merge.")
                self.signals.finished.emit(False, "No files merged.")
                return

            self.log(
                f"Found {len(files_to_process)} unique files to merge. Total size: {total_size} bytes.")
            self.signals.progress.emit(0)

            with open(self.output_file, "w", encoding="utf-8", errors='replace') as outfile:
                total_files_count = len(files_to_process)
                for i, (file_path, relative_path, fsize) in enumerate(files_to_process):
                    if not self.is_running:
                        break

                    # Use POSIX paths in delimiters for consistency
                    relative_path_str = relative_path.as_posix()
                    self.log(
                        f"Merging ({i+1}/{total_files_count}): {relative_path_str}")

                    start_delimiter = START_DELIMITER_FORMAT.format(
                        filepath=relative_path_str)
                    end_delimiter = END_DELIMITER_FORMAT.format(
                        filepath=relative_path_str)

                    outfile.write(start_delimiter + "\n")
                    content_written = False
                    try:
                        try:
                            # Try reading as UTF-8 first
                            with open(file_path, "r", encoding="utf-8") as infile:
                                content = infile.read()
                        except UnicodeDecodeError:
                            self.log(
                                f"Warning: Non-UTF-8 file detected: {relative_path_str}. Reading with 'latin-1'.")
                            try:
                                with open(file_path, "r", encoding="latin-1") as infile:
                                    content = infile.read()
                            except Exception as e_latin:
                                self.log(
                                    f"Error reading file {file_path} even with latin-1: {e_latin}. Skipping content.")
                                content = f"Error reading file (latin-1): {e_latin}"
                        except FileNotFoundError:
                            self.log(
                                f"Error: File disappeared during merge: {file_path}. Skipping.")
                            content = f"Error: File not found during merge process."
                        except Exception as e:
                            self.log(
                                f"Error reading file {file_path}: {e}. Skipping content.")
                            content = f"Error reading file: {e}"

                        outfile.write(content)
                        content_written = True
                        # Ensure a newline after content, before the end delimiter
                        if content and not content.endswith('\n'):
                            outfile.write("\n")

                    except Exception as e:
                        self.log(f"Error processing file {file_path}: {e}")
                        # Ensure delimiters are written even if content fails
                        if not content_written:
                            outfile.write(f"Error processing file: {e}\n")

                    # Add extra newline for readability
                    outfile.write(end_delimiter + "\n\n")

                    processed_size += fsize
                    processed_files_count += 1
                    # Update progress based on size if available, otherwise by file count
                    if total_size > 0:
                        progress_percent = int(
                            (processed_size / total_size) * 100)
                        self.signals.progress.emit(progress_percent)
                    elif total_files_count > 0:
                        # Fallback progress based on file count
                        self.signals.progress.emit(
                            int((processed_files_count / total_files_count) * 100))

            # --- Finalization ---
            if not self.is_running:
                self.log("Merge cancelled during writing.")
                try:
                    # Attempt to clean up the possibly incomplete output file
                    if os.path.exists(self.output_file):
                        os.remove(self.output_file)
                        self.log(
                            f"Removed incomplete file: {self.output_file}")
                except OSError as e:
                    self.log(f"Could not remove incomplete file: {e}")
                self.signals.finished.emit(False, "Merge cancelled.")
            else:
                self.signals.progress.emit(100)
                self.log("Merge process completed successfully.")
                self.signals.finished.emit(
                    True, f"Merge successful! {len(files_to_process)} files merged.")

        except Exception as e:
            self.log(
                f"An error occurred during merge: {e}\n{traceback.format_exc()}")
            self.signals.error.emit(f"Merge failed: {e}")
            self.signals.finished.emit(False, "Merge failed.")


# --- Splitter Worker --- (No changes needed)
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

    def log(self, msg):
        self.signals.log.emit(msg)

    def run(self):
        self.log(f"Starting split process for: {self.merged_file}")
        self.log(f"Output directory: {self.output_dir}")
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
                            self.log(
                                f"Warning: Found new START delimiter for '{start_match.group(1)}' before END delimiter for '{current_file_path_relative}' near line {line_num+1}. Saving previous block.")
                            if self._write_file(current_file_path_relative, "".join(current_file_content)):
                                file_count += 1
                        # Capture the relative path from the delimiter
                        current_file_path_relative = start_match.group(1)
                        # Basic path safety check (avoids absolute paths and excessive ..)
                        # More robust checking might be needed depending on security requirements
                        safe_path = True
                        if os.path.isabs(current_file_path_relative) or "../" in current_file_path_relative.replace("\\", "/"):
                            # Basic check against absolute paths or parent traversal attempts
                            # Note: This check might be too simplistic for complex cases.
                            safe_path = False

                        if not safe_path:
                            self.log(
                                f"Error: Invalid or potentially unsafe path found in delimiter: '{current_file_path_relative}'. Skipping block.")
                            current_file_path_relative = None
                            in_file_block = False
                            continue  # Skip to next line

                        current_file_content = []
                        in_file_block = True
                        self.log(
                            f"Found file block: {current_file_path_relative}")
                        continue  # Move to next line, don't include delimiter in content

                    # Check for the end delimiter *only if* we are inside a block
                    if in_file_block and current_file_path_relative:
                        # Construct the expected end delimiter for the current block
                        expected_end_delimiter = END_DELIMITER_FORMAT.format(
                            filepath=current_file_path_relative)
                        if line_stripped == expected_end_delimiter:
                            # Found the end, write the file
                            if self._write_file(current_file_path_relative, "".join(current_file_content)):
                                file_count += 1
                            # Reset state for the next block
                            in_file_block = False
                            current_file_path_relative = None
                            current_file_content = []
                        else:
                            # Not the end delimiter, append the line to the current file's content
                            current_file_content.append(line)

                # --- Loop finished ---
                if not self.is_running:
                    self.log("Split cancelled.")
                    self.signals.finished.emit(False, "Split cancelled.")
                    return

                # Check if the file ended while still inside a block (missing end delimiter)
                if in_file_block and current_file_path_relative:
                    self.log(
                        f"Warning: Merged file ended before finding END delimiter for '{current_file_path_relative}'. Saving partial content.")
                    if self._write_file(current_file_path_relative, "".join(current_file_content)):
                        file_count += 1

            # --- Post-processing ---
            self.signals.progress.emit(100)
            self.log(
                f"Split process completed. {file_count} files potentially created (check log for errors).")
            self.signals.finished.emit(
                True, f"Split successful! {file_count} files processed.")

        except FileNotFoundError:
            self.log(f"Error: Merged file not found: {self.merged_file}")
            self.signals.error.emit(
                f"Merged file not found: {self.merged_file}")
            self.signals.finished.emit(
                False, "Split failed: Input file not found.")
        except Exception as e:
            self.log(
                f"An critical error occurred during split: {e}\n{traceback.format_exc()}")
            self.signals.error.emit(f"Split failed: {e}")
            self.signals.finished.emit(False, "Split failed.")
        finally:
            # Ensure progress hits 100 even on error exit from try block
            self.signals.progress.emit(100)

    def _write_file(self, relative_path_str, content):
        """Helper to write content to the appropriate file within the output directory.
           Includes safety checks. Returns True on success, False on failure."""
        if not relative_path_str:
            self.log(
                "Error: Attempted to write file with no relative path. Skipping.")
            return False

        try:
            # Create the full target path by joining the output directory and the relative path
            # Normalize the path to resolve any '.' or '..' if possible (though '..' should have been caught earlier)
            target_path = self.output_dir.joinpath(relative_path_str).resolve()
            output_dir_resolved = self.output_dir.resolve()

            # --- Security Check ---
            # Ensure the resolved target path is still within the intended output directory
            if output_dir_resolved != target_path and output_dir_resolved not in target_path.parents:
                # This check prevents path traversal attacks (e.g., relative_path = "../../../etc/passwd")
                self.log(
                    f"Error: Security risk! Path '{relative_path_str}' resolved to '{target_path}', which is outside the designated output directory '{output_dir_resolved}'. Skipping.")
                return False

            self.log(f"Creating file: {target_path}")
            # Create parent directories if they don't exist
            target_path.parent.mkdir(parents=True, exist_ok=True)

            # Write the content to the file
            with open(target_path, "w", encoding="utf-8") as outfile:
                outfile.write(content)
            return True

        except OSError as e:
            self.log(f"Error writing file {relative_path_str} (OS Error): {e}")
            return False
        except Exception as e:
            # Catch any other unexpected errors during file writing
            self.log(
                f"Error writing file {relative_path_str} (General Error): {e}")
            return False


# --- Folder Selection Dialog (Corrected for Tristate) ---
class FolderSelectionDialog(QDialog):
    """A dialog to select specific files and subfolders within a chosen folder using a tree view."""

    def __init__(self, folder_path_str, parent=None):
        super().__init__(parent)
        self.folder_path = pathlib.Path(folder_path_str)
        self._selected_items_for_worker = [] # Store result for get_selected_items

        self.setWindowTitle(f"Select items in: {self.folder_path.name}")
        self.setMinimumSize(450, 450) # Increased min height for tree
        self.setSizeGripEnabled(True)

        layout = QVBoxLayout(self)
        self.tree_view = QTreeView()
        self.tree_view.setHeaderHidden(True)
        self.model = QStandardItemModel()
        self.tree_view.setModel(self.model)

        layout.addWidget(QLabel(f"Select items to include from:\n<b>{self.folder_path}</b>"))
        layout.addWidget(self.tree_view, 1) # Give tree view expansion space

        # Icons
        self.folder_icon = self.style().standardIcon(QStyle.StandardPixmap.SP_DirIcon)
        self.file_icon = self.style().standardIcon(QStyle.StandardPixmap.SP_FileIcon)
        self.error_icon = self.style().standardIcon(QStyle.StandardPixmap.SP_MessageBoxWarning)

        self.populate_tree()
        self.tree_view.expandToDepth(0) # Expand top level initially

        # Connect itemChanged signal for check state synchronization
        self.model.itemChanged.connect(self.on_item_changed)

        button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def _populate_recursive(self, parent_node, current_path):
        """Recursively populates the model with files and folders."""
        try:
            items_in_dir = sorted(
                list(current_path.iterdir()),
                key=lambda p: (not p.is_dir(), p.name.lower())
            )

            for item_path in items_in_dir:
                item = QStandardItem(item_path.name)
                item.setCheckable(True)
                item.setCheckState(Qt.CheckState.Checked) # Default to checked
                item.setEditable(False)

                if item_path.is_dir():
                    item.setIcon(self.folder_icon)
                    item.setData("folder", TYPE_DATA_ROLE)
                    item.setData(str(item_path), PATH_DATA_ROLE)
                    # --- FIX: Use ItemIsUserTristate flag for folders ---
                    item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserTristate)
                    # --- REMOVED: item.setTristate(True) ---
                    parent_node.appendRow(item)
                    self._populate_recursive(item, item_path)
                elif item_path.is_file():
                    item.setIcon(self.file_icon)
                    item.setData("file", TYPE_DATA_ROLE)
                    item.setData(str(item_path), PATH_DATA_ROLE)
                    # --- FIX: Explicitly ensure files are NOT tristate ---
                    item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsUserTristate)
                    parent_node.appendRow(item)

        except OSError as e:
            error_item = QStandardItem(f"Error reading: {e.strerror}")
            error_item.setIcon(self.error_icon)
            error_item.setFlags(Qt.ItemFlag.ItemIsEnabled)
            error_item.setToolTip(str(current_path))
            parent_node.appendRow(error_item)
            print(f"OS Error reading {current_path}: {e}")
        except Exception as e:
            error_item = QStandardItem(f"Unexpected error processing {current_path.name}")
            error_item.setIcon(self.error_icon)
            error_item.setFlags(Qt.ItemFlag.ItemIsEnabled)
            error_item.setToolTip(f"{current_path}\n{e}")
            parent_node.appendRow(error_item)
            print(f"Unexpected Error processing {current_path}: {e}\n{traceback.format_exc()}")


    def populate_tree(self):
        """Scans the folder recursively and populates the tree view."""
        self.model.blockSignals(True)
        self.model.clear()
        root_node = self.model.invisibleRootItem()
        self._populate_recursive(root_node, self.folder_path)
        self.model.blockSignals(False)

    def on_item_changed(self, item: QStandardItem):
        """Handles changes in item check state for recursive checking."""
        if not item or not item.isCheckable():
            return

        check_state = item.checkState()
        item_type = item.data(TYPE_DATA_ROLE)

        self.model.blockSignals(True)

        # Update children if the changed item is a folder and state is not partial
        if item_type == "folder" and check_state != Qt.CheckState.PartiallyChecked:
            self._set_child_check_state(item, check_state)

        # Update parent's check state
        self._update_parent_check_state(item)

        self.model.blockSignals(False)

    def _set_child_check_state(self, parent_item: QStandardItem, state: Qt.CheckState):
        """Recursively sets the check state of all checkable children."""
        if state == Qt.CheckState.PartiallyChecked:
             return

        for row in range(parent_item.rowCount()):
            child = parent_item.child(row, 0)
            if child and child.isCheckable():
                if child.checkState() != state:
                     child.setCheckState(state)


    def _update_parent_check_state(self, item: QStandardItem):
        """Updates the parent's check state based on its children's states."""
        parent = item.parent()
        # Stop if no parent, parent is root, or parent isn't user-tristate
        # --- FIX: Check ItemIsUserTristate flag ---
        if not parent or parent == self.model.invisibleRootItem() or not (parent.flags() & Qt.ItemFlag.ItemIsUserTristate):
            return

        checked_children = 0
        partially_checked_children = 0
        total_checkable_children = 0

        for row in range(parent.rowCount()):
            child = parent.child(row, 0)
            if child and child.isCheckable():
                total_checkable_children += 1
                state = child.checkState()
                if state == Qt.CheckState.Checked:
                    checked_children += 1
                elif state == Qt.CheckState.PartiallyChecked:
                    partially_checked_children += 1

        new_parent_state = Qt.CheckState.Unchecked
        if total_checkable_children == 0: # Handle case where parent has no checkable children
             pass # Stays unchecked
        elif checked_children == total_checkable_children:
            new_parent_state = Qt.CheckState.Checked
        elif partially_checked_children > 0 or (checked_children > 0 and checked_children < total_checkable_children):
            new_parent_state = Qt.CheckState.PartiallyChecked

        if parent.checkState() != new_parent_state:
            parent.setCheckState(new_parent_state)


    def accept(self):
        """Process selections when OK is clicked. Traverses the tree."""
        self._selected_items_for_worker = []
        root = self.model.invisibleRootItem()
        base_path_str = str(self.folder_path.parent)
        self._collect_selected_roots(root, base_path_str)
        super().accept()

    def _collect_selected_roots(self, parent_item: QStandardItem, base_path_str: str):
        """
        Recursively collects items for the worker.
        Adds fully checked items directly.
        Recurses into partially checked folders.
        """
        for row in range(parent_item.rowCount()):
            item = parent_item.child(row, 0)
            if not item or not item.isCheckable():
                continue

            state = item.checkState()
            item_type = item.data(TYPE_DATA_ROLE)
            item_path_str = item.data(PATH_DATA_ROLE)

            if not item_type or not item_path_str:
                continue

            if state == Qt.CheckState.Checked:
                self._selected_items_for_worker.append((item_type, item_path_str, base_path_str))
            elif state == Qt.CheckState.PartiallyChecked:
                if item_type == "folder" and item.hasChildren():
                    self._collect_selected_roots(item, base_path_str)

    def get_selected_items(self):
        """Return the list of selected (type, path, base_path) tuples for the worker."""
        return self._selected_items_for_worker

# --- Main Application Window (Modified for Tree View) ---
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


        /* List Widget (Kept for reference, now using TreeView) */
        /* QListWidget { ... } */

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

        /* Tree View (Main window and Dialog) */
        QTreeView {
            background-color: #333333;
            color: #dcdcdc;
            border: 1px solid #555555;
            border-radius: 4px;
            alternate-background-color: #3a3a3a;
            /* Ensure branch lines are visible */
            /* show-decoration-selected: 1; */ /* Might help selection visibility */
            outline: none; /* Remove focus outline if desired */
        }
        QTreeView::item {
            padding: 3px; /* Spacing around items */
            border-radius: 2px; /* Slightly rounded item background */
        }
        QTreeView::item:selected {
            background-color: #569cd6; /* Accent selection */
            color: #ffffff;
        }
         QTreeView::item:hover:!selected {
            background-color: #4f4f4f;
        }
        /* Style the branch lines */
        QTreeView::branch {
             background-color: transparent; /* Use background of tree */
             /* Use images for custom branch indicators if needed */
             /* image: none; */
        }
        QTreeView::branch:has-siblings:!adjoins-item {
            border-image: url(none) 0; /* Example: Hide default lines if using images */
        }
        /* Style the check boxes within the tree */
         QTreeView::indicator {
            width: 13px;
            height: 13px;
            /* You might need specific images for a good dark theme checkbox */
            /* border: 1px solid #666; background-color: #444; */
         }
         QTreeView::indicator:unchecked {
            /* image: url(:/dark/checkbox_unchecked.png); */
         }
         QTreeView::indicator:checked {
            /* image: url(:/dark/checkbox_checked.png); */
            background-color: #569cd6; /* Simple checked indicator color */
         }
         QTreeView::indicator:indeterminate {
            /* image: url(:/dark/checkbox_tristate.png); */
            background-color: #77aaff; /* Simple tristate color */
         }


        /* Dialogs */
        QDialog {
            background-color: #2b2b2b; /* Match main window background */
        }
        QDialog QTreeView { /* Ensure Dialog TreeView gets the style too */
             background-color: #333333;
             color: #dcdcdc;
             border: 1px solid #555555;
             alternate-background-color: #3a3a3a;
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
        # Internal list storing tuples (type, path_str, base_path_str) for the MergerWorker
        self._items_to_merge_internal = []
        self.output_merge_file = ""
        self.input_split_file = ""
        self.output_split_dir = ""
        self.worker_thread = None
        self.worker = None

        # Icons (loaded after style applied)
        self.folder_icon = QIcon()
        self.file_icon = QIcon()

        self.initUI()
        self.apply_dark_style()

    def initUI(self):
        self.setWindowTitle('File Merger & Splitter')
        self.setGeometry(150, 150, 800, 700)  # Slightly larger default size

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

        # --- Tree View for Items to Merge ---
        self.item_list_view = QTreeView()
        self.item_list_view.setHeaderHidden(True)
        self.item_model = QStandardItemModel()
        self.item_list_view.setModel(self.item_model)
        self.item_list_view.setSelectionMode(
            QTreeView.SelectionMode.ExtendedSelection)
        # self.item_list_view.setAlternatingRowColors(True) # Stylesheet handles this
        self.item_list_view.setEditTriggers(
            QTreeView.EditTrigger.NoEditTriggers)  # Read-only view

        merge_layout.addWidget(QLabel("Items to Merge:"))
        # Give tree view expansion space
        merge_layout.addWidget(self.item_list_view, 1)

        output_merge_layout = QHBoxLayout()
        self.select_output_merge_button = QPushButton(
            "Select Output Merged File (.txt)")
        self.output_merge_label = QLabel("Output: [Not Selected]")
        self.output_merge_label.setObjectName("OutputMergeLabel")
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
        # (Split tab layout remains the same)
        input_split_layout = QHBoxLayout()
        self.select_input_split_button = QPushButton(
            "Select Merged File (.txt)")
        self.input_split_label = QLabel("Input: [Not Selected]")
        self.input_split_label.setObjectName("InputSplitLabel")
        self.input_split_label.setWordWrap(True)
        input_split_layout.addWidget(self.select_input_split_button)
        input_split_layout.addWidget(self.input_split_label, 1)
        split_layout.addLayout(input_split_layout)

        output_split_layout = QHBoxLayout()
        self.select_output_split_button = QPushButton("Select Output Folder")
        self.output_split_label = QLabel("Output Dir: [Not Selected]")
        self.output_split_label.setObjectName("OutputSplitLabel")
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
        split_layout.addStretch(1)  # Pushes split controls up

        # --- Shared Controls (Log, Progress Bar) Below Tabs ---
        shared_controls_layout = QVBoxLayout()
        shared_controls_layout.addWidget(QLabel("<b>Log / Status</b>"))
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setLineWrapMode(QTextEdit.LineWrapMode.NoWrap)
        self.log_text.setFixedHeight(180)  # Slightly taller log
        shared_controls_layout.addWidget(self.log_text)

        self.progress_bar = QProgressBar()
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setValue(0)
        self.progress_bar.setFixedHeight(22)  # Slightly taller progress bar
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
        try:
            QApplication.setStyle(QStyleFactory.create('Fusion'))
        except Exception as e:
            # Use print for early logs
            print(f"Warning: Could not apply Fusion style: {e}")

        # Apply the custom dark stylesheet
        self.setStyleSheet(self.DARK_STYLESHEET)

        # Load Icons AFTER style is set
        try:
            self.folder_icon = self.style().standardIcon(QStyle.StandardPixmap.SP_DirIcon)
            self.file_icon = self.style().standardIcon(QStyle.StandardPixmap.SP_FileIcon)
            merge_icon = self.style().standardIcon(QStyle.StandardPixmap.SP_MediaPlay)
            split_icon = self.style().standardIcon(
                QStyle.StandardPixmap.SP_ArrowRight)  # Or SP_MediaSeekForward?
            cancel_icon = self.style().standardIcon(
                QStyle.StandardPixmap.SP_DialogCancelButton)
            remove_icon = self.style().standardIcon(
                QStyle.StandardPixmap.SP_DialogDiscardButton)  # Or SP_TrashIcon
            clear_icon = self.style().standardIcon(
                QStyle.StandardPixmap.SP_TrashIcon)  # Or SP_DialogResetButton

            self.add_files_button.setIcon(self.file_icon)
            self.add_folder_button.setIcon(self.folder_icon)
            self.remove_item_button.setIcon(remove_icon)
            self.clear_list_button.setIcon(clear_icon)
            self.merge_button.setIcon(merge_icon)
            self.split_button.setIcon(split_icon)
            self.merge_cancel_button.setIcon(cancel_icon)
            self.split_cancel_button.setIcon(cancel_icon)
        except Exception as e:
            # Log might not be ready yet
            print(f"Warning: Could not load standard icons: {e}")

        # Log after UI is likely initialized
        self.log("Applied dark theme stylesheet.")

    def log(self, message):
        if hasattr(self, 'log_text') and self.log_text:
            self.log_text.append(message)
            self.log_text.ensureCursorVisible()  # Scroll to the bottom
        else:
            # Fallback if log widget isn't ready
            print(f"LOG (pre-init): {message}")

    def update_progress(self, value):
        self.progress_bar.setValue(value)

    def operation_finished(self, success, message):
        self.log(f"Finished: {message}")
        self.progress_bar.setValue(100)  # Ensure progress is 100 on finish
        if success:
            QMessageBox.information(self, "Operation Complete", message)
        else:
            # Check if an error message was already shown via operation_error
            if not hasattr(self, '_error_shown') or not self._error_shown:
                QMessageBox.warning(self, "Operation Finished", message)
        self._reset_error_flag()  # Clear the error flag
        self._set_ui_enabled(True)  # Re-enable UI
        # Clean up worker and thread
        if self.worker_thread:
            self.worker_thread.quit()
            if not self.worker_thread.wait(1000):  # Wait up to 1 sec
                self.log(
                    "Warning: Worker thread didn't finish quitting gracefully.")
            self.worker_thread = None
        self.worker = None  # Worker should have been deleted by thread finish signal

    def operation_error(self, error_message):
        self.log(f"ERROR: {error_message}")
        QMessageBox.critical(self, "Error", error_message)
        self._error_shown = True  # Flag that an error message was displayed

    def _reset_error_flag(self):
        # Reset the flag ensuring popups don't duplicate on finish
        if hasattr(self, '_error_shown'):
            del self._error_shown

    def _set_ui_enabled(self, enabled):
        """Enable/disable UI elements during processing."""
        # Disable/Enable Tabs
        # self.merge_tab.setEnabled(enabled) # Disabling tab content is usually enough
        # self.split_tab.setEnabled(enabled)
        # Disabling the whole tab widget is simpler
        self.tab_widget.setEnabled(enabled)

        # Selectively enable/disable controls within the merge tab
        self.add_files_button.setEnabled(enabled)
        self.add_folder_button.setEnabled(enabled)
        self.remove_item_button.setEnabled(enabled)
        self.clear_list_button.setEnabled(enabled)
        self.item_list_view.setEnabled(enabled)
        self.select_output_merge_button.setEnabled(enabled)
        self.merge_button.setEnabled(enabled)
        # Cancel enabled ONLY when running
        self.merge_cancel_button.setEnabled(not enabled)

        # Selectively enable/disable controls within the split tab
        self.select_input_split_button.setEnabled(enabled)
        self.select_output_split_button.setEnabled(enabled)
        self.split_button.setEnabled(enabled)
        # Cancel enabled ONLY when running
        self.split_cancel_button.setEnabled(not enabled)

    def add_files(self):
        # Determine starting directory for the dialog
        start_dir = ""
        if self._items_to_merge_internal:
            # Try to get the directory of the last added item
            try:
                last_item_path = pathlib.Path(
                    self._items_to_merge_internal[-1][1])
                start_dir = str(last_item_path.parent)
            except:
                pass  # Ignore errors getting start dir

        files, _ = QFileDialog.getOpenFileNames(
            self, "Select Files to Merge", start_dir)
        if files:
            added_count = 0
            root_node = self.item_model.invisibleRootItem()
            for file_path_str in files:
                file_path = pathlib.Path(file_path_str)
                # Base path is the file's containing directory
                base_path = file_path.parent
                item_data_tuple = ("file", str(file_path), str(base_path))

                # Check if this exact file path is already in the internal list
                if not any(item[1] == str(file_path) for item in self._items_to_merge_internal):
                    self._items_to_merge_internal.append(item_data_tuple)

                    # Add to the Tree View
                    item = QStandardItem(self.file_icon, file_path.name)
                    item.setToolTip(str(file_path))
                    # Store data in the item for potential future use (like removal)
                    item.setData(item_data_tuple[0], TYPE_DATA_ROLE)
                    item.setData(item_data_tuple[1], PATH_DATA_ROLE)
                    item.setData(item_data_tuple[2], BASE_PATH_DATA_ROLE)
                    item.setEditable(False)
                    root_node.appendRow(item)
                    added_count += 1
                else:
                    self.log(f"Skipping already added file: {file_path.name}")

            if added_count > 0:
                self.log(f"Added {added_count} file(s).")
                self.item_list_view.expandAll()  # Optional: expand after adding
            else:
                self.log("Selected file(s) were already in the list.")

    def add_folder(self):
        # Determine starting directory
        start_dir = ""
        if self._items_to_merge_internal:
            try:  # Try parent of last added item's base path
                last_item_base = pathlib.Path(
                    self._items_to_merge_internal[-1][2])
                start_dir = str(last_item_base)
            except:
                pass

        folder_path_str = QFileDialog.getExistingDirectory(
            self, "Select Folder to Scan", start_dir)

        if folder_path_str:
            folder_path = pathlib.Path(folder_path_str)
            dialog = FolderSelectionDialog(folder_path_str, self)
            if dialog.exec() == QDialog.DialogCode.Accepted:
                # Get the list of ('type', 'path', 'base_path') tuples from the dialog
                selected_items_for_worker = dialog.get_selected_items()

                if not selected_items_for_worker:
                    self.log(
                        f"No items selected from folder: {folder_path_str}")
                    return

                added_count_worker = 0
                items_already_present = []

                # --- Add to the internal list for the worker ---
                for item_tuple in selected_items_for_worker:
                    item_type, item_path, base_path = item_tuple
                    # Check if path already exists in worker list
                    if not any(i[1] == item_path for i in self._items_to_merge_internal):
                        self._items_to_merge_internal.append(item_tuple)
                        added_count_worker += 1
                    else:
                        items_already_present.append(
                            pathlib.Path(item_path).name)

                # --- Add representation to the Tree View ---
                root_node = self.item_model.invisibleRootItem()

                # Check if a top-level item for this folder already exists
                existing_folder_item = None
                for row in range(root_node.rowCount()):
                    item = root_node.child(row, 0)
                    if item and item.data(PATH_DATA_ROLE) == folder_path_str:
                        existing_folder_item = item
                        break

                if not existing_folder_item:
                    # Create a top-level item for the root folder added
                    folder_item = QStandardItem(
                        self.folder_icon, folder_path.name)
                    folder_item.setToolTip(f"Folder Source: {folder_path_str}")
                    # Special type for root display
                    folder_item.setData("folder-root", TYPE_DATA_ROLE)
                    folder_item.setData(folder_path_str, PATH_DATA_ROLE)
                    # Base path isn't directly relevant for display root, store folder path itself
                    folder_item.setData(folder_path_str, BASE_PATH_DATA_ROLE)
                    folder_item.setEditable(False)
                    root_node.appendRow(folder_item)
                    display_parent_item = folder_item  # Children will be added under this
                else:
                    self.log(
                        f"Updating existing entry for folder: {folder_path.name}")
                    # Clear existing children of this node before adding new selection? Or merge?
                    # Simplest: Assume adding folder again replaces previous selection from it.
                    # We already updated the worker list, now update the display tree children.
                    existing_folder_item.removeRows(
                        0, existing_folder_item.rowCount())
                    display_parent_item = existing_folder_item

                # Add children to the tree view based on selected_items_for_worker hierarchy
                # This is a bit complex as we need to reconstruct the tree visually
                # Store added paths to avoid duplicates in the visual tree under this root
                added_visual_paths = set()
                item_map = {}  # Map path_str to QStandardItem for parenting

                # Sort selected items to potentially help with parent creation order
                sorted_selection = sorted(
                    selected_items_for_worker, key=lambda x: x[1])

                for item_type, item_path_str, base_path_str in sorted_selection:
                    item_path = pathlib.Path(item_path_str)
                    # Path relative to the added folder root
                    relative_path = item_path.relative_to(folder_path)

                    current_parent_in_view = display_parent_item
                    # Create intermediate parent folders in the VIEW if they don't exist
                    # Iterate through parent parts
                    for i, part in enumerate(relative_path.parts[:-1]):
                        part_path_str = str(folder_path.joinpath(
                            *relative_path.parts[:i+1]))
                        child_item = item_map.get(part_path_str)
                        if not child_item:
                            # Find existing child or create new
                            found = False
                            for r in range(current_parent_in_view.rowCount()):
                                check_item = current_parent_in_view.child(r, 0)
                                if check_item and check_item.data(PATH_DATA_ROLE) == part_path_str:
                                    child_item = check_item
                                    found = True
                                    break
                            if not found:
                                child_item = QStandardItem(
                                    self.folder_icon, part)
                                child_item.setData("folder", TYPE_DATA_ROLE)
                                child_item.setData(
                                    part_path_str, PATH_DATA_ROLE)
                                child_item.setEditable(False)
                                child_item.setToolTip(
                                    f"Subfolder: {part_path_str}")
                                current_parent_in_view.appendRow(child_item)
                                item_map[part_path_str] = child_item

                        current_parent_in_view = child_item  # Move down the tree

                    # Now add the actual file or folder item
                    if item_path_str not in added_visual_paths:
                        final_item = QStandardItem(relative_path.name)
                        final_item.setData(item_type, TYPE_DATA_ROLE)
                        final_item.setData(item_path_str, PATH_DATA_ROLE)
                        # Store base for worker consistency
                        final_item.setData(base_path_str, BASE_PATH_DATA_ROLE)
                        final_item.setEditable(False)
                        final_item.setToolTip(
                            f"{item_type.capitalize()}: {item_path_str}\nBase: {base_path_str}")
                        if item_type == "folder":
                            final_item.setIcon(self.folder_icon)
                        else:
                            final_item.setIcon(self.file_icon)
                        current_parent_in_view.appendRow(final_item)
                        added_visual_paths.add(item_path_str)
                        # Add leaf to map too
                        item_map[item_path_str] = final_item

                self.item_list_view.expandAll()  # Expand to show the new structure

                # Log summary
                if added_count_worker > 0:
                    log_msg = f"Added {added_count_worker} item(s) from: {folder_path_str}"
                    if items_already_present:
                        log_msg += f" ({len(items_already_present)} skipped as duplicates: {', '.join(items_already_present[:3])}{'...' if len(items_already_present) > 3 else ''})"
                    self.log(log_msg)
                else:
                    self.log(
                        f"All selected items from {folder_path_str} were already in the list.")

            else:  # Dialog was cancelled
                self.log(f"Folder selection cancelled for: {folder_path_str}")

    def remove_selected_items(self):
        selected_indexes = self.item_list_view.selectedIndexes()
        if not selected_indexes:
            self.log("No item(s) selected to remove.")
            return

        # Get the top-level items corresponding to the selection
        # Need to handle cases where a child item might be selected. We remove the root item it belongs to.
        # Or, just remove the exact selected items? Let's remove the selected rows directly.
        # QModelIndex objects are persistent even if rows shift, but get unique rows first.
        rows_to_remove = sorted(list(set(index.row() for index in selected_indexes if index.parent(
        ) == self.item_model.invisibleRootItem())), reverse=True)
        # Also consider removing selected children? For now, only top-level removal. User selects root to remove.

        if not rows_to_remove:
            # Maybe a child was selected? Find its top-level parent.
            top_level_indexes_to_remove = set()
            for index in selected_indexes:
                current = index
                while current.parent() != self.item_model.invisibleRootItem() and current.parent().isValid():
                    current = current.parent()
                if current.parent() == self.item_model.invisibleRootItem():
                    top_level_indexes_to_remove.add(
                        current)  # Add the QModelIndex

            if not top_level_indexes_to_remove:
                self.log(
                    "Selection is not a top-level item or its child. Cannot remove.")
                return
            rows_to_remove = sorted(
                list(set(index.row() for index in top_level_indexes_to_remove)), reverse=True)

        removed_count_display = 0
        removed_count_worker = 0

        items_to_remove_from_worker = []

        self.item_model.blockSignals(True)
        try:
            for row in rows_to_remove:
                item = self.item_model.item(row)  # Get item from top-level row
                if not item:
                    continue

                item_path = item.data(PATH_DATA_ROLE)
                item_type = item.data(TYPE_DATA_ROLE)

                # Collect all worker items associated with this display item and its children
                queue = [item]
                while queue:
                    current_item = queue.pop(0)
                    c_type = current_item.data(TYPE_DATA_ROLE)
                    c_path = current_item.data(PATH_DATA_ROLE)
                    c_base = current_item.data(BASE_PATH_DATA_ROLE)

                    # Add if it represents a worker item (not 'folder-root')
                    if c_path and c_type != "folder-root":
                        items_to_remove_from_worker.append(
                            (c_type, c_path, c_base))

                    # Add children to queue for traversal
                    for r in range(current_item.rowCount()):
                        child = current_item.child(r, 0)
                        if child:
                            queue.append(child)

                # Remove the top-level row from the view model
                self.item_model.removeRow(
                    row, self.item_model.invisibleRootItem())
                removed_count_display += 1

            # Now remove collected items from the internal worker list
            temp_worker_list = list(
                self._items_to_merge_internal)  # Copy to modify
            items_removed_set = set()  # Track paths removed to count unique removals
            for worker_tuple in items_to_remove_from_worker:
                # Find and remove matching tuples (match primarily on path)
                removed_this_pass = False
                # Iterate backwards for safe removal
                for i in range(len(temp_worker_list) - 1, -1, -1):
                    if temp_worker_list[i][1] == worker_tuple[1]:  # Match path
                        del temp_worker_list[i]
                        if worker_tuple[1] not in items_removed_set:
                            removed_count_worker += 1
                            items_removed_set.add(worker_tuple[1])
                        removed_this_pass = True
                        # break # Remove only first match? Or all? Assume path is unique key.

                # if not removed_this_pass:
                #     self.log(f"Warning: Did not find worker item to remove for path: {worker_tuple[1]}")

            self._items_to_merge_internal = temp_worker_list  # Update internal list

        finally:
            self.item_model.blockSignals(False)

        if removed_count_display > 0:
            self.log(
                f"Removed {removed_count_display} top-level item(s) from view and {removed_count_worker} associated item(s) from merge list.")
        else:
            self.log("Could not identify top-level items to remove from selection.")

    def clear_item_list(self):
        self.item_model.clear()  # Clears the tree view
        self._items_to_merge_internal.clear()  # Clears the list for the worker
        self.log("Cleared item list and merge data.")

    def select_output_merge_file(self):
        start_dir = os.path.dirname(
            self.output_merge_file) if self.output_merge_file else ""
        # Suggest .txt extension
        suggested_filename = os.path.join(start_dir, "merged_output.txt")
        file_path, file_filter = QFileDialog.getSaveFileName(
            self, "Save Merged File As", suggested_filename, "Text Files (*.txt);;All Files (*)")
        if file_path:
            # Ensure .txt extension if filter is Text Files and no extension provided
            if file_filter == "Text Files (*.txt)" and not pathlib.Path(file_path).suffix:
                file_path += ".txt"
            self.output_merge_file = file_path
            # Display only filename in label for brevity
            self.output_merge_label.setText(
                f"Output: {os.path.basename(file_path)}")
            self.output_merge_label.setToolTip(
                file_path)  # Full path in tooltip
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
            # Use pathlib for potentially better path display
            self.output_split_label.setText(
                f"Output Dir: {pathlib.Path(dir_path).name}")
            self.output_split_label.setToolTip(dir_path)
            self.log(f"Selected split output directory: {dir_path}")

    def _create_output_dir_if_needed(self, dir_path_str, operation_name):
        """Checks if a directory exists and prompts to create it if not."""
        if not dir_path_str:
            return False  # Path is empty
        dir_path = pathlib.Path(dir_path_str)
        if not dir_path.exists():
            reply = QMessageBox.question(self, f"Create Directory for {operation_name}?",
                                         f"The directory does not exist:\n{dir_path}\n\nCreate it?",
                                         QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                         QMessageBox.StandardButton.Yes)
            if reply == QMessageBox.StandardButton.Yes:
                try:
                    dir_path.mkdir(parents=True, exist_ok=True)
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
        elif not dir_path.is_dir():
            QMessageBox.critical(
                self, f"{operation_name} Error", f"The selected output path exists but is not a directory:\n{dir_path}")
            return False
        return True  # Directory exists and is a directory

    def start_merge(self):
        if not self._items_to_merge_internal:  # Check the internal list
            QMessageBox.warning(self, "Merge Error",
                                "No files or folders selected to merge.")
            return
        if not self.output_merge_file:
            QMessageBox.warning(self, "Merge Error",
                                "Please select an output file first.")
            return
        # Check/create the output file's *directory*
        if not self._create_output_dir_if_needed(os.path.dirname(self.output_merge_file), "Merge"):
            return

        if self.worker_thread and self.worker_thread.isRunning():
            QMessageBox.warning(
                self, "Busy", "Another operation is already in progress.")
            return

        self.log_text.clear()
        self.progress_bar.setValue(0)
        self._set_ui_enabled(False)
        self._reset_error_flag()  # Reset error display flag

        # Pass a copy of the internal list to the worker
        worker_data = list(self._items_to_merge_internal)
        # Log count
        self.log(f"Starting merge with {len(worker_data)} items...")

        self.worker_thread = QThread(self)
        # Pass the worker_data to the worker constructor
        self.worker = MergerWorker(worker_data, self.output_merge_file)
        self.worker.moveToThread(self.worker_thread)

        # Connect signals from worker to slots in GUI thread
        self.worker.signals.progress.connect(self.update_progress)
        self.worker.signals.log.connect(self.log)
        self.worker.signals.error.connect(self.operation_error)
        self.worker.signals.finished.connect(self.operation_finished)
        # Clean up worker and thread when thread finishes
        self.worker_thread.finished.connect(
            self.worker.deleteLater)  # Schedule worker deletion
        self.worker_thread.finished.connect(
            self.worker_thread.deleteLater)  # Schedule thread deletion

        # Connect started signal to worker's run method
        self.worker_thread.started.connect(self.worker.run)
        self.worker_thread.start()  # Start the thread event loop

    def start_split(self):
        if not self.input_split_file:
            QMessageBox.warning(self, "Split Error",
                                "No merged file selected to split.")
            return
        if not os.path.exists(self.input_split_file):  # Use os.path.exists for files
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

        # Connect signals
        self.worker.signals.progress.connect(self.update_progress)
        self.worker.signals.log.connect(self.log)
        self.worker.signals.error.connect(self.operation_error)
        self.worker.signals.finished.connect(self.operation_finished)
        # Clean up
        self.worker_thread.finished.connect(self.worker.deleteLater)
        self.worker_thread.finished.connect(self.worker_thread.deleteLater)

        self.worker_thread.started.connect(self.worker.run)
        self.worker_thread.start()

    def cancel_operation(self):
        if self.worker and self.worker_thread and self.worker_thread.isRunning():
            self.log("Attempting to cancel operation...")
            try:
                self.worker.stop()  # Signal the worker to stop
            except Exception as e:
                # Log potential error
                self.log(f"Error signalling worker to stop: {e}")
            # Disable cancel buttons immediately to prevent multiple clicks
            self.merge_cancel_button.setEnabled(False)
            self.split_cancel_button.setEnabled(False)
            # Worker should eventually emit finished(False, "Cancelled")
        else:
            self.log("No operation is currently running to cancel.")

    def closeEvent(self, event):
        """Ensure worker thread is stopped cleanly on application close."""
        if self.worker and self.worker_thread and self.worker_thread.isRunning():
            self.log(
                "Closing application - Attempting to stop running operation...")
            self.cancel_operation()  # Trigger stop signal and disable buttons

            # Give the thread a moment to finish after being signalled
            if not self.worker_thread.wait(2000):  # Wait up to 2 seconds
                self.log(
                    "Warning: Worker thread did not terminate gracefully after stop signal during close.")
            else:
                self.log("Worker thread stopped during close.")
        event.accept()  # Accept the close event


# --- Main Execution ---
if __name__ == '__main__':
    # High DPI settings (Place before QApplication initialization)
    # These environment variables can help on some systems
    os.environ["QT_ENABLE_HIGHDPI_SCALING"] = "1"
    # Controls how non-integer scale factors are handled
    # Or Round, Floor, Ceil
    os.environ["QT_SCALE_FACTOR_ROUNDING_POLICY"] = "PassThrough"

    # For some Linux systems or older Qt versions, this might be needed instead/additionally
    # QApplication.setAttribute(Qt.ApplicationAttribute.AA_EnableHighDpiScaling, True)
    # QApplication.setAttribute(Qt.ApplicationAttribute.AA_UseHighDpiPixmaps, True)

    app = QApplication(sys.argv)

    # Set Application Info (Optional but good practice)
    app.setApplicationName("File Merger & Splitter")
    app.setOrganizationName("YourCompanyName")  # Replace if desired

    # Apply High DPI attributes *after* QApplication instance exists if needed
    # if hasattr(Qt.ApplicationAttribute, 'AA_EnableHighDpiScaling'):
    #     app.setAttribute(Qt.ApplicationAttribute.AA_EnableHighDpiScaling, True)
    # if hasattr(Qt.ApplicationAttribute, 'AA_UseHighDpiPixmaps'):
    #     app.setAttribute(Qt.ApplicationAttribute.AA_UseHighDpiPixmaps, True)

    ex = MergerSplitterApp()
    ex.setObjectName("MergerSplitterAppWindow")  # For root window styling
    ex.show()
    sys.exit(app.exec())
