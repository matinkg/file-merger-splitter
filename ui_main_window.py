import sys
import os
import pathlib
import traceback
from PyQt6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
    QFileDialog, QListWidget, QListWidgetItem, QLabel, QTextEdit,
    QMessageBox, QProgressBar, QSizePolicy, QStyleFactory, QStyle,
    QDialog, QTreeView, QDialogButtonBox, QScrollArea, QTabWidget, QSpacerItem,
    QComboBox, QCheckBox  # <--- Imported QCheckBox
)
from PyQt6.QtGui import QStandardItemModel, QStandardItem, QIcon, QPalette, QColor
from PyQt6.QtCore import (
    Qt, QThread, pyqtSignal, QObject, QDir, QModelIndex
)

# Local imports
from config import MERGE_FORMATS, PATH_DATA_ROLE, TYPE_DATA_ROLE, BASE_PATH_DATA_ROLE
# WorkerSignals is used internally by workers
# Ensure this uses updated workers.py
from workers import MergerWorker, SplitterWorker
from dialogs import FolderSelectionDialog


# --- Main Application Window ---
class MergerSplitterApp(QWidget):
    def __init__(self):
        super().__init__()
        # List[Tuple(type, path, base_path)]
        self._items_to_merge_internal = []
        self.output_merge_file = ""
        self.input_split_file = ""
        self.output_split_dir = ""
        self.worker_thread = None
        self.worker = None
        self._error_shown = False  # Flag to prevent multiple critical error popups

        # Icons (will be loaded in apply_dark_style)
        self.folder_icon = QIcon()
        self.file_icon = QIcon()

        self.initUI()
        self.apply_dark_style()
        self._populate_format_combos()  # Populate dropdowns after UI init

    def initUI(self):
        self.setObjectName("MergerSplitterAppWindow")  # For styling hook
        self.setWindowTitle('File Merger & Splitter')
        self.setGeometry(150, 150, 850, 800)  # Initial size and position
        self.setAcceptDrops(True)  # Enable drag-drop for the whole window

        main_layout = QVBoxLayout(self)
        self.tab_widget = QTabWidget()
        main_layout.addWidget(self.tab_widget, 1)  # Tabs take most space

        # --- Create Tabs ---
        self.merge_tab = QWidget()
        self.split_tab = QWidget()
        self.tab_widget.addTab(self.merge_tab, " Merge Files/Folders ")
        self.tab_widget.addTab(self.split_tab, " Split Merged File ")

        # --- Populate Merge Tab ---
        merge_layout = QVBoxLayout(self.merge_tab)

        # Top Buttons (Add/Remove)
        select_items_layout = QHBoxLayout()
        self.add_files_button = QPushButton("Add Files...")
        self.add_folder_button = QPushButton("Add Folder...")
        self.remove_item_button = QPushButton("Remove Selected")
        self.clear_list_button = QPushButton("Clear List")
        select_items_layout.addWidget(self.add_files_button)
        select_items_layout.addWidget(self.add_folder_button)
        select_items_layout.addSpacing(20)
        select_items_layout.addWidget(self.remove_item_button)
        select_items_layout.addWidget(self.clear_list_button)
        select_items_layout.addStretch()
        merge_layout.addLayout(select_items_layout)

        # Tree View for Items to Merge
        merge_layout.addWidget(QLabel("<b>Items to Merge:</b>"))
        self.item_list_view = QTreeView()
        self.item_list_view.setHeaderHidden(True)
        self.item_model = QStandardItemModel()
        self.item_list_view.setModel(self.item_model)
        self.item_list_view.setSelectionMode(
            QTreeView.SelectionMode.ExtendedSelection)  # Allow multi-select
        self.item_list_view.setEditTriggers(
            QTreeView.EditTrigger.NoEditTriggers)  # Read-only view
        self.item_list_view.setAlternatingRowColors(
            True)  # Improves readability
        self.item_list_view.setSortingEnabled(False)  # Keep user order
        # Tree view takes vertical space
        merge_layout.addWidget(self.item_list_view, 1)

        # Merge Options Layout (Format and Tree Checkbox)
        merge_options_layout = QHBoxLayout()

        # Format Selection (Merge)
        merge_options_layout.addWidget(QLabel("Merge Format:"))
        self.merge_format_combo = QComboBox()
        self.merge_format_combo.setToolTip(
            "Select the delimiter format for the merged output file.")
        merge_options_layout.addWidget(self.merge_format_combo)

        # --- Add Hierarchy Tree Checkbox ---
        self.include_tree_checkbox = QCheckBox("Include File Hierarchy Tree")
        self.include_tree_checkbox.setToolTip(
            "Prepend a list of included files (relative paths) to the merged file.")
        self.include_tree_checkbox.setChecked(True)  # Default to checked
        merge_options_layout.addWidget(self.include_tree_checkbox)
        # --- End Checkbox Addition ---

        merge_options_layout.addStretch(1)  # Push controls to the left
        # Add the combined options layout
        merge_layout.addLayout(merge_options_layout)

        # Output File Selection (Merge)
        output_merge_layout = QHBoxLayout()
        self.select_output_merge_button = QPushButton(
            "Select Output Merged File...")
        self.select_output_merge_button.setSizePolicy(
            QSizePolicy.Policy.Maximum, QSizePolicy.Policy.Fixed)
        self.output_merge_label = QLabel("[Output file not selected]")
        self.output_merge_label.setObjectName(
            "OutputMergeLabel")  # For styling
        self.output_merge_label.setWordWrap(False)
        self.output_merge_label.setAlignment(
            Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)
        output_merge_layout.addWidget(self.select_output_merge_button)
        # Label takes remaining space
        output_merge_layout.addWidget(self.output_merge_label, 1)
        merge_layout.addLayout(output_merge_layout)

        # Merge Action Buttons
        merge_actions_layout = QHBoxLayout()
        merge_actions_layout.addStretch()
        self.merge_button = QPushButton(" Merge ")
        self.merge_button.setObjectName("MergeButton")
        self.merge_cancel_button = QPushButton("Cancel")
        self.merge_cancel_button.setObjectName("MergeCancelButton")
        self.merge_cancel_button.setEnabled(False)  # Initially disabled
        merge_actions_layout.addWidget(self.merge_button)
        merge_actions_layout.addWidget(self.merge_cancel_button)
        merge_actions_layout.addStretch()
        merge_layout.addLayout(merge_actions_layout)

        # --- Populate Split Tab ---
        split_layout = QVBoxLayout(self.split_tab)

        # Input File Selection (Split)
        input_split_layout = QHBoxLayout()
        self.select_input_split_button = QPushButton(
            "Select Merged File to Split...")
        self.select_input_split_button.setSizePolicy(
            QSizePolicy.Policy.Maximum, QSizePolicy.Policy.Fixed)
        self.input_split_label = QLabel("[Input file not selected]")
        self.input_split_label.setObjectName("InputSplitLabel")
        self.input_split_label.setWordWrap(False)
        self.input_split_label.setAlignment(
            Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)
        input_split_layout.addWidget(self.select_input_split_button)
        input_split_layout.addWidget(self.input_split_label, 1)
        split_layout.addLayout(input_split_layout)

        # Output Directory Selection (Split)
        output_split_layout = QHBoxLayout()
        self.select_output_split_button = QPushButton(
            "Select Output Folder...")
        self.select_output_split_button.setSizePolicy(
            QSizePolicy.Policy.Maximum, QSizePolicy.Policy.Fixed)
        self.output_split_label = QLabel("[Output directory not selected]")
        self.output_split_label.setObjectName("OutputSplitLabel")
        self.output_split_label.setWordWrap(False)
        self.output_split_label.setAlignment(
            Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)
        output_split_layout.addWidget(self.select_output_split_button)
        output_split_layout.addWidget(self.output_split_label, 1)
        split_layout.addLayout(output_split_layout)

        # Format Selection (Split)
        format_split_layout = QHBoxLayout()
        format_split_layout.addWidget(QLabel("Split Format:"))
        self.split_format_combo = QComboBox()
        self.split_format_combo.setToolTip(
            "Select the delimiter format expected in the merged file.")
        format_split_layout.addWidget(self.split_format_combo)
        format_split_layout.addStretch(1)
        split_layout.addLayout(format_split_layout)

        # Spacer to push buttons down
        split_layout.addSpacerItem(QSpacerItem(
            20, 20, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Expanding))

        # Split Action Buttons
        split_actions_layout = QHBoxLayout()
        split_actions_layout.addStretch()
        self.split_button = QPushButton(" Split ")
        self.split_button.setObjectName("SplitButton")
        self.split_cancel_button = QPushButton("Cancel")
        self.split_cancel_button.setObjectName("SplitCancelButton")
        self.split_cancel_button.setEnabled(False)
        split_actions_layout.addWidget(self.split_button)
        split_actions_layout.addWidget(self.split_cancel_button)
        split_actions_layout.addStretch()
        split_layout.addLayout(split_actions_layout)

        # --- Shared Controls (Log, Progress Bar) Below Tabs ---
        shared_controls_layout = QVBoxLayout()
        shared_controls_layout.addWidget(QLabel("<b>Log / Status</b>"))
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setLineWrapMode(
            QTextEdit.LineWrapMode.WidgetWidth)  # Wrap lines
        self.log_text.setFixedHeight(200)  # Fixed height for log area
        shared_controls_layout.addWidget(self.log_text)

        self.progress_bar = QProgressBar()
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setValue(0)
        self.progress_bar.setFixedHeight(24)
        self.progress_bar.setAlignment(Qt.AlignmentFlag.AlignCenter)
        shared_controls_layout.addWidget(self.progress_bar)
        # Add shared controls below tabs
        main_layout.addLayout(shared_controls_layout)

        # --- Connect Signals ---
        self.add_files_button.clicked.connect(self.show_add_files_dialog)
        self.add_folder_button.clicked.connect(self.show_add_folder_dialog)
        self.remove_item_button.clicked.connect(self.remove_selected_items)
        self.clear_list_button.clicked.connect(self.clear_item_list)
        self.select_output_merge_button.clicked.connect(
            self.select_output_merge_file)
        self.merge_button.clicked.connect(self.start_merge)
        self.merge_cancel_button.clicked.connect(self.cancel_operation)
        self.merge_format_combo.currentIndexChanged.connect(
            self._update_merge_button_state)
        self.item_list_view.selectionModel().selectionChanged.connect(
            self._update_merge_button_state)  # Update remove button state
        # Also connect checkbox to update button state (optional, but good practice if it affects mergeability)
        # self.include_tree_checkbox.stateChanged.connect(self._update_merge_button_state) # Uncomment if checkbox state affects if merge *can* start

        self.select_input_split_button.clicked.connect(
            self.select_input_split_file)
        self.select_output_split_button.clicked.connect(
            self.select_output_split_dir)
        self.split_button.clicked.connect(self.start_split)
        self.split_cancel_button.clicked.connect(self.cancel_operation)
        self.split_format_combo.currentIndexChanged.connect(
            self._update_split_button_state)

        # Initial state checks
        self._update_merge_button_state()
        self._update_split_button_state()

    def apply_dark_style(self):
        """Applies the dark mode stylesheet and Fusion style."""
        try:
            # Fusion style often works well with custom stylesheets
            QApplication.setStyle(QStyleFactory.create('Fusion'))
        except Exception as e:
            print(f"Warning: Could not apply Fusion style: {e}")

        # Apply standard icons after setting the style
        try:
            style = self.style()  # Get the currently applied style
            self.folder_icon = style.standardIcon(
                QStyle.StandardPixmap.SP_DirIcon)
            self.file_icon = style.standardIcon(
                QStyle.StandardPixmap.SP_FileIcon)

            # Get icons for buttons
            merge_icon = style.standardIcon(
                QStyle.StandardPixmap.SP_DialogSaveButton)  # Save icon for merge
            split_icon = style.standardIcon(
                QStyle.StandardPixmap.SP_ArrowRight)  # Arrow for split
            cancel_icon = style.standardIcon(
                QStyle.StandardPixmap.SP_DialogCancelButton)
            remove_icon = style.standardIcon(
                QStyle.StandardPixmap.SP_TrashIcon)  # Trash icon for remove
            clear_icon = style.standardIcon(
                QStyle.StandardPixmap.SP_DialogResetButton)  # Reset/clear icon

            # Set icons on buttons
            self.add_files_button.setIcon(self.file_icon)
            self.add_folder_button.setIcon(self.folder_icon)
            self.remove_item_button.setIcon(remove_icon)
            self.clear_list_button.setIcon(clear_icon)

            self.merge_button.setIcon(merge_icon)
            self.split_button.setIcon(split_icon)
            self.merge_cancel_button.setIcon(cancel_icon)
            self.split_cancel_button.setIcon(cancel_icon)

            self.select_output_merge_button.setIcon(
                style.standardIcon(QStyle.StandardPixmap.SP_FileIcon))
            self.select_input_split_button.setIcon(
                style.standardIcon(QStyle.StandardPixmap.SP_FileIcon))
            self.select_output_split_button.setIcon(
                style.standardIcon(QStyle.StandardPixmap.SP_DirIcon))

        except Exception as e:
            print(f"Warning: Could not load some standard icons: {e}")
            # Application will still run, just without some icons

    def _populate_format_combos(self):
        """Populates the format combo boxes from config."""
        self.merge_format_combo.clear()
        self.split_format_combo.clear()

        format_names = list(MERGE_FORMATS.keys())
        if not format_names:
            self.log("Error: No merge formats defined in config.py.")
            self.merge_format_combo.addItem("Error: No Formats")
            self.split_format_combo.addItem("Error: No Formats")
            return

        self.merge_format_combo.addItems(format_names)
        self.split_format_combo.addItems(format_names)

        # Optionally set a default selection
        if "Default" in format_names:
            self.merge_format_combo.setCurrentText("Default")
            self.split_format_combo.setCurrentText("Default")
        elif format_names:  # Select first if Default not found
            self.merge_format_combo.setCurrentIndex(0)
            self.split_format_combo.setCurrentIndex(0)

        # self.log(f"Populated format selectors with: {', '.join(format_names)}") # A bit verbose for startup

    def log(self, message):
        """Appends a message to the log text area."""
        # Ensure log_text exists before appending (might be called during init)
        if hasattr(self, 'log_text') and self.log_text:
            self.log_text.append(message)
            # Scroll to the bottom to show the latest message
            self.log_text.verticalScrollBar().setValue(
                self.log_text.verticalScrollBar().maximum())
            QApplication.processEvents()  # Update UI immediately - use sparingly
        else:
            print(f"LOG (pre-init): {message}")  # Fallback to console

    def update_progress(self, value):
        """Updates the progress bar value."""
        safe_value = max(0, min(value, 100))  # Clamp value between 0 and 100
        self.progress_bar.setValue(safe_value)
        self.progress_bar.setFormat(f"%p% ({safe_value}%)")

    def operation_finished(self, success, message):
        """Handles the completion of a worker operation."""
        self.log(f"Operation Finished: Success={success}, Message='{message}'")
        self.progress_bar.setValue(100)  # Ensure it shows 100%
        self.progress_bar.setFormat("Finished")

        if success:
            QMessageBox.information(self, "Operation Complete", message)
        else:
            # Only show warning popup if a critical error popup wasn't already shown
            if not self._error_shown:
                if "cancel" in message.lower():
                    self.log("Operation was cancelled by user.")
                    # No popup for cancellation needed, log is sufficient
                else:
                    # Show warning for non-critical failures reported via 'finished' signal
                    QMessageBox.warning(self, "Operation Finished", message)

        self._reset_error_flag()  # Reset flag after handling finished signal
        self._set_ui_enabled(True)  # Re-enable UI

        # Clean up worker and thread
        if self.worker:
            try:  # Disconnect signals safely
                if hasattr(self.worker, 'signals'):
                    signals = self.worker.signals  # Cache signals object

                    # Check if signal exists before trying to disconnect
                    if hasattr(signals, 'progress') and signals.progress is not None:
                        try:
                            signals.progress.disconnect(self.update_progress)
                        except TypeError:
                            pass  # Ignore if not connected or already disconnected

                    if hasattr(signals, 'log') and signals.log is not None:
                        try:
                            signals.log.disconnect(self.log)
                        except TypeError:
                            pass

                    if hasattr(signals, 'error') and signals.error is not None:
                        try:
                            signals.error.disconnect(self.operation_error)
                        except TypeError:
                            pass

                    if hasattr(signals, 'finished') and signals.finished is not None:
                        try:
                            signals.finished.disconnect(
                                self.operation_finished)
                        except TypeError:
                            pass
            except Exception as e:
                self.log(f"Warning: Error disconnecting worker signals: {e}")

            # Schedule worker object deletion via event loop
            self.worker.deleteLater()

        if self.worker_thread:
            if self.worker_thread.isRunning():
                self.worker_thread.quit()
                # Wait a bit for graceful exit, then terminate if necessary
                if not self.worker_thread.wait(1500):  # 1.5 second timeout
                    self.log(
                        "Warning: Worker thread didn't quit gracefully. Terminating.")
                    self.worker_thread.terminate()
                    self.worker_thread.wait(500)  # Short wait after terminate
            # Schedule thread object deletion via event loop
            self.worker_thread.deleteLater()

        self.worker_thread = None
        self.worker = None
        self.log("Worker resources released.")
        # Update button states after cleanup and UI re-enable
        self._update_merge_button_state()
        self._update_split_button_state()

    def operation_error(self, error_message):
        """Slot specifically for critical errors reported by the worker's 'error' signal."""
        self.log(f"CRITICAL ERROR received: {error_message}")
        # Avoid double-showing if finished also reports an error
        if not self._error_shown:
            QMessageBox.critical(
                self, "Critical Operation Error", error_message)
            self._error_shown = True  # Flag that a critical error message was displayed
            # Optionally force UI reset here too if error is unrecoverable
            # self._set_ui_enabled(True)

    def _reset_error_flag(self):
        """Reset the flag that tracks if a critical error message was shown."""
        self._error_shown = False

    def _set_ui_enabled(self, enabled):
        """Enable/disable UI elements during processing."""
        is_running = not enabled
        current_index = self.tab_widget.currentIndex()

        # Disable the *other* tab while an operation is running
        self.tab_widget.setTabEnabled(1 - current_index, enabled)

        # Determine active tab
        is_merge_tab_active = (self.tab_widget.widget(
            current_index) == self.merge_tab)
        is_split_tab_active = (self.tab_widget.widget(
            current_index) == self.split_tab)

        # --- Merge Tab Controls ---
        self.add_files_button.setEnabled(enabled)
        self.add_folder_button.setEnabled(enabled)
        self.remove_item_button.setEnabled(
            enabled and self._can_remove_merge_items())  # Also check selection
        self.clear_list_button.setEnabled(
            enabled and self._can_clear_merge_items())  # Also check if list has items
        self.item_list_view.setEnabled(enabled)
        self.merge_format_combo.setEnabled(enabled)
        self.select_output_merge_button.setEnabled(enabled)
        self.include_tree_checkbox.setEnabled(
            enabled)  # <<< Enable/disable checkbox
        # Enable merge button only if conditions met AND UI is enabled
        self.merge_button.setEnabled(enabled and self._can_start_merge())
        # Enable cancel button only if running AND on the merge tab
        self.merge_cancel_button.setEnabled(is_running and is_merge_tab_active)

        # --- Split Tab Controls ---
        self.select_input_split_button.setEnabled(enabled)
        self.select_output_split_button.setEnabled(enabled)
        self.split_format_combo.setEnabled(enabled)
        # Enable split button only if conditions met AND UI is enabled
        self.split_button.setEnabled(enabled and self._can_start_split())
        # Enable cancel button only if running AND on the split tab
        self.split_cancel_button.setEnabled(is_running and is_split_tab_active)

    def _can_start_merge(self):
        """Check if conditions are met to start merging."""
        selected_format_name = self.merge_format_combo.currentText()
        format_ok = selected_format_name and selected_format_name in MERGE_FORMATS
        # Check if internal list is not empty OR if the view model has items (more robust check)
        has_items = bool(self._items_to_merge_internal) or (
            self.item_model.rowCount() > 0)
        return bool(has_items and self.output_merge_file and format_ok)

    def _can_remove_merge_items(self):
        """Check if items are present and selected in the merge list view."""
        has_items = len(self._items_to_merge_internal) > 0 or (
            self.item_model.rowCount() > 0)
        has_selection = len(self.item_list_view.selectedIndexes()) > 0
        return has_items and has_selection

    def _can_clear_merge_items(self):
        """Check if items are present in the merge list."""
        return len(self._items_to_merge_internal) > 0 or (self.item_model.rowCount() > 0)

    def _update_merge_button_state(self):
        """Enable/disable the Merge/Remove/Clear buttons based on state."""
        # Check if UI is currently enabled (not running an operation)
        ui_enabled = not (
            self.worker_thread and self.worker_thread.isRunning())

        can_merge = self._can_start_merge()
        can_remove = self._can_remove_merge_items()
        can_clear = self._can_clear_merge_items()

        self.merge_button.setEnabled(ui_enabled and can_merge)
        self.remove_item_button.setEnabled(ui_enabled and can_remove)
        self.clear_list_button.setEnabled(ui_enabled and can_clear)

    def _can_start_split(self):
        """Check if conditions are met to start splitting."""
        input_exists = os.path.isfile(
            self.input_split_file)  # Check if file exists
        selected_format_name = self.split_format_combo.currentText()
        format_ok = selected_format_name and selected_format_name in MERGE_FORMATS
        output_dir_set = bool(self.output_split_dir)
        return bool(self.input_split_file and input_exists and output_dir_set and format_ok)

    def _update_split_button_state(self):
        """Enable/disable the Split button based on state."""
        # Check if UI is currently enabled (not running an operation)
        ui_enabled = not (
            self.worker_thread and self.worker_thread.isRunning())
        can_split = self._can_start_split()
        self.split_button.setEnabled(ui_enabled and can_split)

    def dragEnterEvent(self, event):
        """Accept drops if they contain URLs and we are on the merge tab."""
        if self.tab_widget.currentWidget() == self.merge_tab and event.mimeData().hasUrls():
            event.acceptProposedAction()
        else:
            event.ignore()

    def dropEvent(self, event):
        """Handle dropped files and folders."""
        if not (self.tab_widget.currentWidget() == self.merge_tab and event.mimeData().hasUrls()):
            event.ignore()
            return

        urls = event.mimeData().urls()
        files_to_add = []
        folders_to_add = []

        self.log(f"Dropped {len(urls)} item(s). Processing...")

        for url in urls:
            if not url.isLocalFile():
                self.log(f"Skipping non-local URL: {url.toString()}")
                continue

            path_str = url.toLocalFile()
            path = pathlib.Path(path_str)

            if path.is_file():
                files_to_add.append(path_str)
            elif path.is_dir():
                folders_to_add.append(path_str)
            else:
                self.log(f"Skipping unknown or inaccessible item: {path_str}")

        if files_to_add:
            self.add_files(files_to_add)

        for folder_path in folders_to_add:
            self.add_folder(folder_path)

        event.acceptProposedAction()

    def show_add_files_dialog(self):
        """Opens a file dialog to select files and then processes them."""
        start_dir = ""
        # Try to start dialog in the directory of the last added item
        if self._items_to_merge_internal:
            try:
                # Use base path of last item for consistency
                last_item_base_str = self._items_to_merge_internal[-1][2]
                last_item_base = pathlib.Path(last_item_base_str)
                if last_item_base.is_dir():
                    start_dir = str(last_item_base)
                elif last_item_base.parent.is_dir():  # Fallback to parent if base wasn't a dir itself
                    start_dir = str(last_item_base.parent)
            except Exception:  # Ignore errors determining start dir
                pass
        # If still no start_dir, use current working directory
        if not start_dir:
            start_dir = QDir.currentPath()

        files, _ = QFileDialog.getOpenFileNames(
            self, "Select Files to Merge", start_dir, "All Files (*.*)")
        if files:
            self.add_files(files)

    def add_files(self, files):
        """Adds a list of file paths to the merge list and tree view."""
        if not files:
            return

        added_count = 0
        root_node = self.item_model.invisibleRootItem()
        # Keep track of paths added to the view in this operation to avoid duplicates in view
        added_view_paths_this_op = set()
        # Get set of existing worker paths for quick lookup
        existing_worker_paths = {item[1]
                                 for item in self._items_to_merge_internal}

        for file_path_str in files:
            try:
                file_path = pathlib.Path(file_path_str).resolve()
                if not file_path.is_file():
                    self.log(
                        f"Warning: Selected item is not a file or does not exist: {file_path_str}")
                    continue

                file_path_str_resolved = str(file_path)
                # Check if already in the internal worker list
                if file_path_str_resolved not in existing_worker_paths:
                    base_path = file_path.parent.resolve()
                    item_data_tuple = (
                        "file", file_path_str_resolved, str(base_path))
                    self._items_to_merge_internal.append(item_data_tuple)
                    existing_worker_paths.add(
                        file_path_str_resolved)  # Update set

                    # Add to view only if not already present at top level
                    # Check if a top-level item with this exact path already exists
                    already_in_view_toplevel = False
                    for row in range(root_node.rowCount()):
                        toplevel_item = root_node.child(row, 0)
                        if toplevel_item and toplevel_item.data(PATH_DATA_ROLE) == file_path_str_resolved:
                            already_in_view_toplevel = True
                            break

                    if not already_in_view_toplevel and file_path_str_resolved not in added_view_paths_this_op:
                        item = QStandardItem(self.file_icon, file_path.name)
                        item.setToolTip(
                            f"File: {file_path}\nBase: {base_path}")
                        item.setData(item_data_tuple[0], TYPE_DATA_ROLE)
                        item.setData(item_data_tuple[1], PATH_DATA_ROLE)
                        item.setData(item_data_tuple[2], BASE_PATH_DATA_ROLE)
                        item.setEditable(False)
                        root_node.appendRow(item)
                        added_view_paths_this_op.add(file_path_str_resolved)

                    added_count += 1
                # else: # Be less verbose, don't log every skipped file
                #     self.log(f"Skipping already added file: {file_path.name}")

            except OSError as e:
                self.log(f"Error resolving path '{file_path_str}': {e}")
            except Exception as e:
                self.log(
                    f"Unexpected error adding file '{file_path_str}': {e}")

        if added_count > 0:
            self.log(f"Added {added_count} unique file(s) to the merge list.")
            self._update_merge_button_state()
        elif files:
            self.log("Selected file(s) were already in the list.")

    def show_add_folder_dialog(self):
        """Opens a folder dialog to select a folder and then processes it."""
        start_dir = ""
        if self._items_to_merge_internal:
            try:
                last_item_base = pathlib.Path(
                    self._items_to_merge_internal[-1][2])
                if last_item_base.is_dir():
                    start_dir = str(last_item_base)
                elif last_item_base.parent.is_dir():
                    start_dir = str(last_item_base.parent)
            except Exception:
                pass
        # If still no start_dir, use current working directory
        if not start_dir:
            start_dir = QDir.currentPath()

        folder_path_str = QFileDialog.getExistingDirectory(
            self, "Select Folder to Scan", start_dir)
        if folder_path_str:
            self.add_folder(folder_path_str)

    def add_folder(self, folder_path_str):
        """Opens the FolderSelectionDialog for a given path and adds selected items."""
        if not folder_path_str:
            return

        try:
            folder_path = pathlib.Path(folder_path_str).resolve()
            if not folder_path.is_dir():
                QMessageBox.warning(self, "Invalid Folder",
                                    f"Not a valid directory:\n{folder_path_str}")
                return

            # --- Use FolderSelectionDialog ---
            dialog = FolderSelectionDialog(str(folder_path), self)
            if dialog.exec() == QDialog.DialogCode.Accepted:
                selected_items_for_worker = dialog.get_selected_items()

                if not selected_items_for_worker:
                    self.log(
                        f"No items selected from folder: {folder_path.name}")
                    return

                added_count_worker = 0
                added_count_view = 0
                items_already_present_paths = {
                    item[1] for item in self._items_to_merge_internal}
                new_items_for_worker = []

                # Add selected items to internal list if not already present
                for item_tuple in selected_items_for_worker:
                    item_abs_path = item_tuple[1]
                    if item_abs_path not in items_already_present_paths:
                        new_items_for_worker.append(item_tuple)
                        items_already_present_paths.add(
                            item_abs_path)  # Update set
                        added_count_worker += 1

                self._items_to_merge_internal.extend(new_items_for_worker)

                # Add/Update a single entry in the TreeView representing the folder source
                root_node = self.item_model.invisibleRootItem()
                folder_root_path_str = str(folder_path)
                existing_folder_root_item = None

                # Find if a representation for this folder already exists
                for row in range(root_node.rowCount()):
                    item = root_node.child(row, 0)
                    if item and item.data(TYPE_DATA_ROLE) == "folder-root" and item.data(
                            PATH_DATA_ROLE) == folder_root_path_str:
                        existing_folder_root_item = item
                        break

                num_selected = len(selected_items_for_worker)
                display_text = f"{folder_path.name} ({num_selected} selected)"
                tooltip_text = f"Folder Source: {folder_root_path_str}\nSelected {num_selected} item(s) within."
                # Get base path from the dialog results (should be consistent)
                base_path_from_dialog = selected_items_for_worker[
                    0][2] if selected_items_for_worker else ""

                if not existing_folder_root_item:
                    folder_item = QStandardItem(self.folder_icon, display_text)
                    folder_item.setToolTip(tooltip_text)
                    # Special type for view item
                    folder_item.setData("folder-root", TYPE_DATA_ROLE)
                    folder_item.setData(folder_root_path_str, PATH_DATA_ROLE)
                    folder_item.setData(
                        base_path_from_dialog, BASE_PATH_DATA_ROLE)
                    folder_item.setEditable(False)
                    root_node.appendRow(folder_item)
                    added_count_view += 1
                else:
                    # Update existing item text and tooltip
                    existing_folder_root_item.setText(display_text)
                    existing_folder_root_item.setToolTip(
                        tooltip_text + " (updated)")
                    # Ensure base path is updated if it somehow changed (unlikely here)
                    existing_folder_root_item.setData(
                        base_path_from_dialog, BASE_PATH_DATA_ROLE)
                    # self.log(f"Note: Selection from folder '{folder_path.name}' updated.")

                # Log results
                if added_count_worker > 0:
                    self.log(
                        f"Added {added_count_worker} new item(s) from folder '{folder_path.name}' to merge list.")
                elif selected_items_for_worker:  # Items selected, but none were new
                    self.log(
                        f"Selection from folder '{folder_path.name}' updated. No *new* items added (already present or re-selected).")

                # if added_count_view > 0: # Less important log message
                #     self.log(f"Added representation for folder '{folder_path.name}' to the view.")

                self._update_merge_button_state()

            else:  # Dialog was cancelled
                self.log(f"Folder selection cancelled for: {folder_path.name}")

        except OSError as e:
            err_msg = f"Error resolving folder path '{folder_path_str}': {e}"
            self.log(err_msg)
            QMessageBox.critical(
                self, "Error", f"Could not access or resolve folder:\n{folder_path_str}\n\nError: {e}")
        except Exception as e:
            err_msg = f"Unexpected error adding folder '{folder_path_str}': {e}\n{traceback.format_exc()}"
            self.log(err_msg)
            QMessageBox.critical(
                self, "Error", f"An unexpected error occurred adding folder:\n{e}")

    def remove_selected_items(self):
        """Removes selected items from the tree view AND the internal worker list."""
        selected_indexes = self.item_list_view.selectedIndexes()
        if not selected_indexes:
            # Nothing selected, nothing to do.
            return

        unique_top_level_rows_to_remove = set()
        paths_to_remove_from_worker = set()
        # Track rows processed for worker path collection
        items_processed_for_worker_paths = set()

        # --- Step 1 & 2: Identify unique top-level rows and map to worker paths ---
        for index in selected_indexes:
            # We only care about column 0 selections for identifying the item/row
            if not index.isValid() or index.column() != 0:
                continue

            # For the current flat view, the selected index IS the top-level index.
            top_level_row = index.row()
            if top_level_row < 0:  # Should not happen for valid selections from the view
                continue

            # Add the row index to the set for view removal
            unique_top_level_rows_to_remove.add(top_level_row)

            # Check if we already processed this row for worker path collection
            if top_level_row in items_processed_for_worker_paths:
                continue
            items_processed_for_worker_paths.add(top_level_row)

            # Get the item for this top-level row from the model
            item = self.item_model.item(top_level_row, 0)
            if not item:
                self.log(
                    f"Warning: Could not retrieve item for top-level row {top_level_row}. Skipping worker path collection for this row.")
                continue

            # Retrieve data stored in the item
            item_path = item.data(PATH_DATA_ROLE)
            item_type = item.data(TYPE_DATA_ROLE)
            item_base = item.data(BASE_PATH_DATA_ROLE)

            if not item_path:
                self.log(
                    f"Warning: Item '{item.text()}' at row {top_level_row} has no path data.")
                continue

            # Collect worker paths based on the type of the view item
            if item_type == "file":
                paths_to_remove_from_worker.add(item_path)
            elif item_type == "folder-root":
                folder_root_path_str = item_path
                folder_root_base_path = item_base
                if not folder_root_base_path:
                    self.log(
                        f"Warning: Missing base path for folder item '{item.text()}'. Cannot reliably remove worker items.")
                    continue

                # Find worker items matching this folder selection based on base path and containment
                items_to_mark = set()
                for worker_tuple in self._items_to_merge_internal:
                    _w_type, w_path, w_base = worker_tuple
                    is_match = False
                    # Match based on the base path associated with the folder-root item in the view
                    if w_base == folder_root_base_path:
                        # Use robust path matching logic
                        try:
                            p_worker = pathlib.Path(w_path).resolve()
                            p_root = pathlib.Path(
                                folder_root_path_str).resolve()
                            # Check if the worker path is the root itself or is contained within it
                            if p_worker == p_root or p_root in p_worker.parents:
                                is_match = True
                        except Exception as path_err:
                            self.log(
                                f"Warning: Path comparison error during removal check for folder '{folder_root_path_str}': {path_err}")
                            # Fallback string check (less reliable for edge cases)
                            try:
                                # Ensure path is valid before string check
                                if pathlib.Path(folder_root_path_str).is_dir() and w_path.startswith(
                                        folder_root_path_str):
                                    is_match = True
                            except OSError:  # Handle invalid path strings gracefully
                                pass
                    if is_match:
                        items_to_mark.add(w_path)
                paths_to_remove_from_worker.update(items_to_mark)
            # else: Ignore other potential item types if any exist

        # --- Step 3: Clear Selection in the View ---
        # Crucial step: Clear the view's selection *before* modifying the model rows
        # This helps prevent issues where the view's selection state interferes with removal.
        if unique_top_level_rows_to_remove:  # Only clear if we identified rows to remove
            self.item_list_view.clearSelection()
            # Optional: Force event processing if needed, but usually not necessary here.
            # QApplication.processEvents()

        # --- Step 4: Remove Worker Data ---
        initial_worker_count = len(self._items_to_merge_internal)
        removed_count_worker = 0
        if paths_to_remove_from_worker:
            # Create the new list, filtering out items whose path is in the removal set
            self._items_to_merge_internal = [
                item_tuple for item_tuple in self._items_to_merge_internal
                if item_tuple[1] not in paths_to_remove_from_worker
            ]
            removed_count_worker = initial_worker_count - \
                len(self._items_to_merge_internal)

        # --- Step 5: Remove View Rows ---
        removed_count_display = 0
        if unique_top_level_rows_to_remove:
            # Block signals for potentially faster batch removal, though less critical for small removals
            self.item_model.blockSignals(True)
            try:
                # Parent index for top-level items is the invalid index (represents the root)
                root_parent_index = QModelIndex()
                # Remove rows in descending order to avoid index shifting issues during removal
                sorted_rows = sorted(
                    list(unique_top_level_rows_to_remove), reverse=True)

                for row in sorted_rows:
                    # Check bounds *before* attempting removal using the model's current state
                    current_root_row_count = self.item_model.rowCount(
                        root_parent_index)
                    if 0 <= row < current_root_row_count:
                        # Use the model's removeRow method directly for top-level rows
                        removed_ok = self.item_model.removeRow(
                            row, root_parent_index)
                        if removed_ok:
                            removed_count_display += 1
                        else:
                            # Log if the model explicitly failed the removal
                            self.log(
                                f"Warning: model.removeRow({row}, root_parent) returned False.")
                    else:
                        # Log if the index is out of bounds, which might indicate an unexpected model state
                        # Avoid logging if list was already empty (count == 0)
                        if current_root_row_count > 0:
                            self.log(
                                f"Warning: Row index {row} was out of bounds for root parent during removal (current row count: {current_root_row_count}).")

            except Exception as e_view_remove:
                # Log any unexpected exceptions during the view removal process
                self.log(
                    f"ERROR: Exception during view removal: {e_view_remove}\n{traceback.format_exc()}")
            finally:
                # IMPORTANT: Always re-enable signals, even if errors occurred
                self.item_model.blockSignals(False)

        # --- Final Logging and State Update ---
        if removed_count_display > 0 or removed_count_worker > 0:
            self.log(
                f"Removed {removed_count_display} item(s) from view and {removed_count_worker} corresponding item(s) from merge list.")
        # Optional: Add a log if items were selected but nothing was ultimately removed
        # elif selected_indexes:
        #     self.log("Selected items processed, but no corresponding items were removed.")

        # Update button states (e.g., disable 'Remove' if list becomes empty)
        self._update_merge_button_state()

    def clear_item_list(self):
        """Clears both the view model and the internal worker list."""
        if not self._items_to_merge_internal and self.item_model.rowCount() == 0:
            self.log("List is already empty.")
            return

        reply = QMessageBox.question(self, "Confirm Clear",
                                     "Remove ALL items from the merge list?",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                     QMessageBox.StandardButton.No)  # Default to No

        if reply == QMessageBox.StandardButton.Yes:
            self.item_model.clear()  # Clear the tree view
            self._items_to_merge_internal.clear()  # Clear the internal data list
            self.log("Cleared merge item list.")
            self._update_merge_button_state()  # Update button states
        else:
            self.log("Clear operation cancelled.")

    def select_output_merge_file(self):
        """Selects the output .txt file for merging."""
        start_dir = os.path.dirname(
            self.output_merge_file) if self.output_merge_file else QDir.currentPath()
        # Suggest a default filename
        suggested_filename = os.path.join(start_dir, "merged_output.txt")

        file_path, file_filter = QFileDialog.getSaveFileName(
            self, "Save Merged File As", suggested_filename,
            # Allow all files but default to txt
            "Text Files (*.txt);;All Files (*)")

        if file_path:
            p = pathlib.Path(file_path)
            # Add .txt extension if user selected the filter and didn't type one
            if file_filter == "Text Files (*.txt)" and not p.suffix:
                file_path += ".txt"
                p = pathlib.Path(file_path)

            self.output_merge_file = str(p.resolve())  # Store resolved path
            display_path = self._truncate_path_display(self.output_merge_file)
            self.output_merge_label.setText(display_path)
            self.output_merge_label.setToolTip(
                self.output_merge_file)  # Full path in tooltip
            self.log(f"Selected merge output file: {self.output_merge_file}")
            self._update_merge_button_state()

    def select_input_split_file(self):
        """Selects the input .txt file for splitting."""
        start_dir = os.path.dirname(
            self.input_split_file) if self.input_split_file else QDir.currentPath()
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select Merged File to Split", start_dir,
            "Text Files (*.txt);;All Files (*)")

        if file_path:
            p = pathlib.Path(file_path)
            self.input_split_file = str(p.resolve())
            display_path = self._truncate_path_display(self.input_split_file)
            self.input_split_label.setText(display_path)
            self.input_split_label.setToolTip(self.input_split_file)
            self.log(f"Selected split input file: {self.input_split_file}")
            self._update_split_button_state()

    def select_output_split_dir(self):
        """Selects the output directory for split files."""
        start_dir = self.output_split_dir if self.output_split_dir else QDir.currentPath()
        dir_path = QFileDialog.getExistingDirectory(
            self, "Select Output Directory for Split Files", start_dir)

        if dir_path:
            p = pathlib.Path(dir_path)
            self.output_split_dir = str(p.resolve())
            display_path = self._truncate_path_display(self.output_split_dir)
            self.output_split_label.setText(display_path)
            self.output_split_label.setToolTip(self.output_split_dir)
            self.log(
                f"Selected split output directory: {self.output_split_dir}")
            self._update_split_button_state()

    def _truncate_path_display(self, path_str, max_len=60):
        """Truncates a path string for display in labels, adding ellipsis."""
        if len(path_str) <= max_len:
            return path_str
        else:
            # Try to show ".../grandparent/parent/filename"
            try:
                parts = pathlib.Path(path_str).parts
                if len(parts) > 2:
                    # Show last two parts
                    truncated = f"...{os.sep}{parts[-2]}{os.sep}{parts[-1]}"
                    # If still too long, show only last part
                    if len(truncated) > max_len:
                        truncated = f"...{os.sep}{parts[-1]}"
                    # If *still* too long, truncate the filename itself
                    if len(truncated) > max_len:
                        # Ensure ellipsis fits
                        filename_part_len = max_len - 4  # Length for filename part
                        if filename_part_len < 1:
                            filename_part_len = 1
                        truncated = "..." + parts[-1][-filename_part_len:]
                    return truncated
                elif len(parts) == 2:  # e.g., C:\file.txt -> C:\...\file.txt (or /root/file)
                    # Show root and filename part
                    truncated = f"{parts[0]}{os.sep}...{os.sep}{parts[-1]}"
                    if len(truncated) > max_len:
                        filename_part_len = max_len - \
                            len(parts[0]) - len(os.sep) * \
                            2 - 3  # Length for filename
                        if filename_part_len < 1:
                            filename_part_len = 1
                        truncated = f"{parts[0]}{os.sep}...{os.sep}" + \
                            parts[-1][-filename_part_len:]
                    return truncated

                else:  # Just a filename, unlikely but possible
                    return "..." + path_str[-(max_len - 3):]
            except Exception:  # Pathlib errors, fallback
                return "..." + path_str[-(max_len - 3):]

    def _create_output_dir_if_needed(self, dir_path_str, operation_name):
        """Checks if a directory exists and is writable, prompts to create if not.
           Returns True if directory is ready, False otherwise."""
        if not dir_path_str:
            self.log(
                f"Error: Output directory path is empty for {operation_name}.")
            QMessageBox.critical(
                self, f"{operation_name} Error", "Output directory path is not set.")
            return False
        try:
            dir_path = pathlib.Path(dir_path_str)

            if not dir_path.exists():
                reply = QMessageBox.question(
                    self, f"Create Directory?",
                    f"Output directory does not exist:\n{dir_path}\n\nCreate it?",
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                    QMessageBox.StandardButton.Yes)
                if reply == QMessageBox.StandardButton.Yes:
                    try:
                        dir_path.mkdir(parents=True, exist_ok=True)
                        self.log(f"Created output directory: {dir_path}")
                        # Check write permissions after creation
                        if not os.access(str(dir_path), os.W_OK):
                            raise OSError(
                                "Directory created but is not writable.")
                        return True  # Created and writable
                    except OSError as e:
                        QMessageBox.critical(
                            self, f"{operation_name} Error",
                            f"Could not create or write to directory:\n{dir_path}\n\nError: {e}")
                        self.log(f"Error: Failed create/write directory: {e}")
                        return False  # Failed creation or writing
                else:
                    self.log(
                        f"{operation_name} cancelled (output directory not created).")
                    return False  # User chose not to create

            elif not dir_path.is_dir():
                QMessageBox.critical(
                    self, f"{operation_name} Error",
                    f"Output path exists but is not a directory:\n{dir_path}")
                self.log(f"Error: Output path is not a directory: {dir_path}")
                return False  # Path exists but isn't a directory

            elif not os.access(str(dir_path), os.W_OK):
                QMessageBox.critical(
                    self, f"{operation_name} Error",
                    f"Output directory is not writable:\n{dir_path}")
                self.log(f"Error: Output directory not writable: {dir_path}")
                return False  # Directory exists but isn't writable

            else:
                # Directory exists, is a directory, and is writable
                return True

        except Exception as e:  # Catch potential Path errors for invalid strings
            QMessageBox.critical(
                self, f"{operation_name} Error",
                f"Invalid output directory path specified:\n{dir_path_str}\n\nError: {e}")
            self.log(f"Error: Invalid output path '{dir_path_str}': {e}")
            return False

    def start_merge(self):
        """Starts the merge operation in a background thread using the selected format."""
        if not self._can_start_merge():
            # Provide specific feedback
            msg = "Cannot start merge. Please ensure:"
            if not self._items_to_merge_internal and self.item_model.rowCount() == 0:
                msg += "\n- Items have been added to the list."
            if not self.output_merge_file:
                msg += "\n- An output file has been selected."
            if not self.merge_format_combo.currentText() or self.merge_format_combo.currentText() not in MERGE_FORMATS:
                msg += "\n- A valid merge format is selected."
            QMessageBox.warning(self, "Merge Error", msg)
            self.log(
                f"Merge aborted: Conditions not met. Reason: {msg.replace(':', ' -').replace('n- ', ' ')}")
            return

        # Check/Create output directory *before* starting worker
        try:
            output_dir = str(pathlib.Path(self.output_merge_file).parent)
        except Exception as e:
            QMessageBox.critical(
                self, "Merge Error", f"Invalid output file path:\n{self.output_merge_file}\n\nError: {e}")
            self.log(
                f"Merge aborted: Invalid output file path '{self.output_merge_file}': {e}")
            return

        if not self._create_output_dir_if_needed(output_dir, "Merge"):
            self.log("Merge aborted: Output directory check/creation failed.")
            return

        # Prevent starting if already running
        if self.worker_thread and self.worker_thread.isRunning():
            QMessageBox.warning(
                self, "Busy", "Another operation is already in progress.")
            self.log("Merge aborted: Another worker is active.")
            return

        # --- Get Selected Format Details ---
        selected_format_name = self.merge_format_combo.currentText()
        selected_format_details = MERGE_FORMATS.get(selected_format_name)
        if not selected_format_details:
            QMessageBox.critical(self, "Internal Error",
                                 f"Selected merge format '{selected_format_name}' not found in configuration.")
            self.log(
                f"CRITICAL Error: Cannot find details for merge format '{selected_format_name}'.")
            return

        # --- Get Tree Inclusion State ---
        include_tree = self.include_tree_checkbox.isChecked()  # <<< Get checkbox state
        # --- End Tree State ---

        # --- Prepare and Start Worker ---
        self.log_text.clear()
        self.progress_bar.setValue(0)
        self.progress_bar.setFormat("Starting Merge...")
        self._set_ui_enabled(False)
        self._reset_error_flag()

        # Pass a copy of the list and the tree flag to the worker
        # Important: Pass a copy in case the user modifies the list while worker runs
        worker_data = list(self._items_to_merge_internal)
        self.log(
            f"Starting merge with {len(worker_data)} items/sources using format '{selected_format_name}'.")
        if include_tree:
            self.log("Including file hierarchy tree at the start.")
        self.log(f"Output file: {self.output_merge_file}")

        self.worker_thread = QThread(self)
        # --- Pass include_tree flag to worker ---
        self.worker = MergerWorker(worker_data, self.output_merge_file, selected_format_details,
                                   include_tree)  # <<< Pass flag here
        # --- End passing flag ---
        self.worker.moveToThread(self.worker_thread)

        # Connect worker signals to UI slots
        self.worker.signals.progress.connect(self.update_progress)
        self.worker.signals.log.connect(self.log)
        self.worker.signals.error.connect(self.operation_error)
        self.worker.signals.finished.connect(self.operation_finished)

        # Connect thread signals for cleanup
        # Schedule worker deletion when thread finishes
        self.worker_thread.finished.connect(self.worker.deleteLater)
        # Schedule thread deletion when thread finishes
        self.worker_thread.finished.connect(self.worker_thread.deleteLater)

        # Start the worker's run method when the thread starts
        self.worker_thread.started.connect(self.worker.run)
        self.worker_thread.start()
        self.log("Merge worker thread started.")

    def start_split(self):
        """Starts the split operation in a background thread using the selected format."""
        if not self._can_start_split():
            msg = "Cannot start split. Please ensure:"
            if not self.input_split_file or not os.path.isfile(self.input_split_file):
                msg += "\n- A valid input file has been selected."
            if not self.output_split_dir:
                msg += "\n- An output directory has been selected."
            if not self.split_format_combo.currentText() or self.split_format_combo.currentText() not in MERGE_FORMATS:
                msg += "\n- A valid split format is selected."
            QMessageBox.warning(self, "Split Error", msg)
            self.log(
                f"Split aborted: Conditions not met. Reason: {msg.replace(':', ' -').replace('n- ', ' ')}")
            return

        # Check/Create output directory *before* starting worker
        if not self._create_output_dir_if_needed(self.output_split_dir, "Split"):
            self.log("Split aborted: Output directory check/creation failed.")
            return

        # Prevent starting if already running
        if self.worker_thread and self.worker_thread.isRunning():
            QMessageBox.warning(
                self, "Busy", "Another operation is already in progress.")
            self.log("Split aborted: Another worker is active.")
            return

        # --- Get Selected Format Details ---
        selected_format_name = self.split_format_combo.currentText()
        selected_format_details = MERGE_FORMATS.get(selected_format_name)
        if not selected_format_details:
            QMessageBox.critical(self, "Internal Error",
                                 f"Selected split format '{selected_format_name}' not found in configuration.")
            self.log(
                f"CRITICAL Error: Cannot find details for split format '{selected_format_name}'.")
            return

        # --- Prepare and Start Worker ---
        self.log_text.clear()
        self.progress_bar.setValue(0)
        self.progress_bar.setFormat("Starting Split...")
        self._set_ui_enabled(False)
        self._reset_error_flag()
        self.log(
            f"Starting split: '{os.path.basename(self.input_split_file)}' -> '{self.output_split_dir}'")
        self.log(f"Using format: '{selected_format_name}'")

        self.worker_thread = QThread(self)
        self.worker = SplitterWorker(
            self.input_split_file, self.output_split_dir, selected_format_details)
        self.worker.moveToThread(self.worker_thread)

        # Connect signals
        self.worker.signals.progress.connect(self.update_progress)
        self.worker.signals.log.connect(self.log)
        self.worker.signals.error.connect(self.operation_error)
        self.worker.signals.finished.connect(self.operation_finished)
        self.worker_thread.finished.connect(self.worker.deleteLater)
        self.worker_thread.finished.connect(self.worker_thread.deleteLater)
        self.worker_thread.started.connect(self.worker.run)

        self.worker_thread.start()
        self.log("Split worker thread started.")

    def cancel_operation(self):
        """Signals the running worker thread to stop."""
        if self.worker and self.worker_thread and self.worker_thread.isRunning():
            self.log("Attempting to cancel running operation...")
            try:
                # Call the worker's stop method if it exists
                if hasattr(self.worker, 'stop'):
                    self.worker.stop()
                else:
                    self.log(
                        "Warning: Worker object does not have a 'stop' method.")
                    # Fallback: Request interruption if possible (less reliable)
                    # self.worker_thread.requestInterruption()
            except Exception as e:
                # Log error but don't crash UI
                self.log(f"Error trying to signal worker to stop: {e}")

            # Disable cancel buttons immediately to prevent multiple clicks
            self.merge_cancel_button.setEnabled(False)
            self.split_cancel_button.setEnabled(False)
            self.progress_bar.setFormat("Cancelling...")
            # The operation_finished slot will handle actual UI re-enabling and cleanup
        else:
            self.log("No operation is currently running to cancel.")

    def closeEvent(self, event):
        """Ensure worker thread is stopped cleanly on application close."""
        if self.worker_thread and self.worker_thread.isRunning():
            self.log("Close Event: Attempting to stop active operation...")
            self.cancel_operation()  # Signal worker to stop

            # Give the thread some time to finish based on the worker's stop flag
            if not self.worker_thread.wait(2500):  # Wait 2.5 seconds
                self.log(
                    "Warning: Worker thread did not terminate gracefully after cancel signal. Forcing termination.")
                self.worker_thread.terminate()  # Force stop if needed
                self.worker_thread.wait(500)  # Brief wait after terminate
            else:
                self.log("Worker thread stopped successfully during close event.")
        event.accept()  # Proceed with closing the window
