import pathlib
import fnmatch  # Import for gitignore pattern matching
import os       # Import for checking file existence

from PyQt6.QtWidgets import (
    # Added QHBoxLayout, QPushButton
    QDialog, QTreeView, QVBoxLayout, QHBoxLayout, QPushButton,
    QLabel, QDialogButtonBox, QStyle, QMessageBox  # Added QMessageBox
)
from PyQt6.QtGui import QStandardItemModel, QStandardItem, QIcon
from PyQt6.QtCore import Qt

# Import constants from config.py
from config import PATH_DATA_ROLE, TYPE_DATA_ROLE

# --- Folder Selection Dialog ---


class FolderSelectionDialog(QDialog):
    """A dialog to select specific files and subfolders within a chosen folder using a tree view with tristate checkboxes."""

    def __init__(self, folder_path_str, parent=None):
        super().__init__(parent)
        self.folder_path = pathlib.Path(
            folder_path_str).resolve()  # Resolve path immediately
        self._selected_items_for_worker = []
        self.gitignore_patterns = []  # Store parsed gitignore patterns
        self.gitignore_path = self.folder_path / ".gitignore"

        self.setWindowTitle(f"Select items in: {self.folder_path.name}")
        self.setMinimumSize(550, 600)  # Increased minimum size slightly
        self.setSizeGripEnabled(True)  # Allow resizing

        layout = QVBoxLayout(self)

        self.tree_view = QTreeView()
        self.tree_view.setHeaderHidden(True)
        self.model = QStandardItemModel()
        self.tree_view.setModel(self.model)

        layout.addWidget(
            QLabel(f"Select items to include from:\n<b>{self.folder_path}</b>"))
        layout.addWidget(self.tree_view, 1)  # Give tree view stretch factor

        # --- Filter Buttons ---
        filter_layout = QHBoxLayout()
        filter_layout.addWidget(QLabel("Quick Filters:"))

        self.gitignore_button = QPushButton("Apply .gitignore")
        self.gitignore_button.setToolTip(
            f"Uncheck items matching patterns in\n{self.gitignore_path}")
        self.gitignore_button.clicked.connect(self.apply_gitignore_filter)
        # Disable button if .gitignore doesn't exist or cannot be read initially
        self.gitignore_button.setEnabled(self._read_gitignore())
        filter_layout.addWidget(self.gitignore_button)

        self.hidden_files_button = QPushButton("Uncheck Hidden (.<name>)")
        self.hidden_files_button.setToolTip(
            "Uncheck files and folders starting with a dot.")
        self.hidden_files_button.clicked.connect(self.apply_hidden_filter)
        filter_layout.addWidget(self.hidden_files_button)

        filter_layout.addStretch()  # Push buttons to the left
        layout.addLayout(filter_layout)  # Add filter button layout
        # --- End Filter Buttons ---

        # Get standard icons from the current style
        style = self.style()
        self.folder_icon = style.standardIcon(QStyle.StandardPixmap.SP_DirIcon)
        self.file_icon = style.standardIcon(QStyle.StandardPixmap.SP_FileIcon)
        self.error_icon = style.standardIcon(
            QStyle.StandardPixmap.SP_MessageBoxWarning)

        # Populate tree (block signals during population for performance)
        self.model.blockSignals(True)
        self.populate_tree()
        self.model.blockSignals(False)
        self.tree_view.expandToDepth(0)  # Expand top level initially

        # Connect itemChanged signal AFTER population
        self.model.itemChanged.connect(self.on_item_changed)

        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def _populate_recursive(self, parent_node: QStandardItem, current_path: pathlib.Path):
        """Recursively populates the tree model."""
        try:
            # Sort items: folders first, then files, alphabetically
            items_in_dir = sorted(list(current_path.iterdir()), key=lambda p: (
                not p.is_dir(), p.name.lower()))
        except OSError as e:
            error_text = f"Error reading: {e.strerror} ({current_path.name})"
            error_item = QStandardItem(self.error_icon, error_text)
            # Not checkable/selectable
            error_item.setFlags(Qt.ItemFlag.ItemIsEnabled)
            error_item.setToolTip(
                f"Could not access directory:\n{current_path}\n{e}")
            parent_node.appendRow(error_item)
            # Also log to console
            print(f"OS Error reading {current_path}: {e}")
            return  # Stop recursion for this branch

        for item_path in items_in_dir:
            item = QStandardItem(item_path.name)
            item.setEditable(False)
            item.setCheckable(True)
            # Default to checked initially
            item.setCheckState(Qt.CheckState.Checked)
            # Store full path and type in custom data roles
            item.setData(str(item_path.resolve()), PATH_DATA_ROLE)

            if item_path.is_dir():
                item.setIcon(self.folder_icon)
                item.setData("folder", TYPE_DATA_ROLE)
                # Enable tristate for folders
                item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserTristate)
                parent_node.appendRow(item)
                # Recurse into subdirectories
                self._populate_recursive(item, item_path)
            elif item_path.is_file():
                item.setIcon(self.file_icon)
                item.setData("file", TYPE_DATA_ROLE)
                # Files are not tristate
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsUserTristate)
                parent_node.appendRow(item)
            # else: ignore symlinks, etc. for now

    def populate_tree(self):
        """Clears the model and populates it starting from the root folder."""
        self.model.clear()
        root_node = self.model.invisibleRootItem()
        self._populate_recursive(root_node, self.folder_path)

    # --- Filter Logic Implementation ---

    def _read_gitignore(self) -> bool:
        """Reads .gitignore from self.folder_path, stores patterns, returns True if successful."""
        self.gitignore_patterns = []
        if not self.gitignore_path.is_file():
            print(f".gitignore not found at: {self.gitignore_path}")
            return False
        try:
            with open(self.gitignore_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        self.gitignore_patterns.append(line)
            print(
                f"Read {len(self.gitignore_patterns)} patterns from {self.gitignore_path}")
            return True
        except OSError as e:
            print(f"Error reading {self.gitignore_path}: {e}")
            QMessageBox.warning(self, "Error Reading .gitignore",
                                f"Could not read the .gitignore file:\n{self.gitignore_path}\n\nError: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error reading {self.gitignore_path}: {e}")
            QMessageBox.critical(self, "Error Reading .gitignore",
                                 f"An unexpected error occurred while reading:\n{self.gitignore_path}\n\nError: {e}")
            return False

    def _matches_gitignore_pattern(self, relative_path_obj: pathlib.Path) -> bool:
        """
        Checks if a Path object (relative to the .gitignore location) matches any pattern.
        Handles basic gitignore syntax: wildcards (*), directory (/), negation (!).
        Returns True if the path should be ignored (unchecked).
        """
        relative_path_str = relative_path_obj.as_posix()  # Use posix paths for matching
        # Check if the original path represents a directory
        is_dir = relative_path_obj.is_dir()

        # Ensure the item actually exists before checking if it's a dir
        # The relative path might not directly correspond to an item in the model sometimes?
        # Let's rely on the item type from the model instead for dir check if possible
        # Reverted: We need the original path type here for pattern matching logic

        # Determine initial match status (False = not ignored)
        ignored = False
        # Track the most specific pattern match (negated or not)
        last_match_negated = False
        last_match_specificity = -1  # Higher is more specific

        for i, pattern in enumerate(self.gitignore_patterns):
            original_pattern = pattern  # Keep original for logging/debug
            negated = pattern.startswith('!')
            if negated:
                pattern = pattern[1:]

            # Basic cleanup - remove leading/trailing whitespace that might remain
            pattern = pattern.strip()
            if not pattern:
                continue  # Skip empty patterns resulting from cleanup

            # Prepare pattern for fnmatch based on gitignore rules
            match_pattern = pattern
            match_from_root = pattern.startswith('/')
            match_directory = pattern.endswith('/')

            if match_from_root:
                match_pattern = match_pattern[1:]
            if match_directory:
                match_pattern = match_pattern[:-1]

            # Core matching logic
            path_to_match = relative_path_str
            pattern_applies = False

            # 1. Directory Match (`/dir/` or `dir/`)
            if match_directory:
                if not is_dir:  # Pattern specifies a dir, but item is a file
                    continue  # Cannot match
                # Check if the directory path (with trailing slash) matches
                # Need to handle root matching ('/') vs relative matching
                dir_path_str_with_slash = path_to_match + '/'
                if match_from_root:
                    if fnmatch.fnmatchcase(dir_path_str_with_slash, match_pattern + '/'):
                        pattern_applies = True
                else:  # Match anywhere in the path segments
                    # Check if any directory segment matches the pattern
                    # Or if the full path + slash matches the pattern + slash
                    if fnmatch.fnmatchcase(dir_path_str_with_slash, '*' + match_pattern + '/'):
                        pattern_applies = True

            # 2. File/Any Match (no trailing `/` in pattern)
            else:
                # Check if the path string matches the pattern
                if match_from_root:
                    if fnmatch.fnmatchcase(path_to_match, match_pattern):
                        pattern_applies = True
                else:  # Match pattern anywhere in the path
                    # Option A: Match against the basename
                    # if fnmatch.fnmatchcase(relative_path_obj.name, match_pattern):
                    #     pattern_applies = True
                    # Option B: Match against the full path string (more like git)
                    # Match anywhere
                    if fnmatch.fnmatchcase(path_to_match, '*' + match_pattern):
                        # More precise: Check if any component matches if no '/' in pattern
                        if '/' not in pattern:
                            pattern_applies = any(fnmatch.fnmatchcase(
                                part, match_pattern) for part in relative_path_obj.parts)
                        else:  # Pattern contains '/', match against full relative path
                            pattern_applies = fnmatch.fnmatchcase(
                                path_to_match, '*' + match_pattern)

            # Update ignore status based on match and negation
            if pattern_applies:
                # Using index as specificity (later rules override earlier)
                if i > last_match_specificity:
                    ignored = not negated  # If negated, it's NOT ignored
                    last_match_negated = negated
                    last_match_specificity = i
                # print(f"Path '{relative_path_str}' matched pattern '{original_pattern}' (negated={negated}). Current ignored: {ignored}")

        return ignored

    def _apply_filter_recursive(self, parent_item: QStandardItem, filter_func, *args):
        """Recursively applies a filter function to checkable items."""
        items_to_revisit = []  # Store items whose state changed for parent update
        for row in range(parent_item.rowCount()):
            item = parent_item.child(row, 0)
            if not item:
                continue

            # Recurse first for folders
            if item.data(TYPE_DATA_ROLE) == "folder":
                self._apply_filter_recursive(item, filter_func, *args)

            # Apply filter to the item itself if checkable
            if item.isCheckable():
                absolute_path_str = item.data(PATH_DATA_ROLE)
                if absolute_path_str:
                    absolute_path = pathlib.Path(absolute_path_str)
                    # Filter function determines if item should be unchecked
                    if filter_func(item, absolute_path, *args):
                        # Uncheck only if it's currently checked or partially checked
                        if item.checkState() != Qt.CheckState.Unchecked:
                            # DO NOT block signals - let on_item_changed handle propagation
                            item.setCheckState(Qt.CheckState.Unchecked)
                            # Mark for potential parent update check
                            items_to_revisit.append(item)

    def apply_gitignore_filter(self):
        """Applies the .gitignore filter to uncheck matching items."""
        if not self.gitignore_patterns:
            if not self._read_gitignore():  # Try reading again if empty
                QMessageBox.information(self, "Apply .gitignore",
                                        f".gitignore file not found or could not be read at:\n{self.gitignore_path}")
                return

        print(
            f"Applying .gitignore filter ({len(self.gitignore_patterns)} patterns)...")

        # Define the filter function for gitignore
        def gitignore_check(item, absolute_path):
            try:
                # Calculate path relative to the folder containing .gitignore
                relative_path = absolute_path.relative_to(self.folder_path)
                item_is_dir = item.data(TYPE_DATA_ROLE) == "folder"
                # Augment relative path with is_dir status for matching logic
                # This feels a bit hacky, maybe pass item type directly?
                # Let's refine _matches_gitignore_pattern instead.
                # Re-fetching is_dir from absolute path is safer
                is_dir_check = absolute_path.is_dir()  # Check the actual file system item
                return self._matches_gitignore_pattern(relative_path)
            except ValueError:
                print(
                    f"Warning: Could not make path relative for gitignore check: {absolute_path}")
                return False  # Don't ignore if relativity fails
            except Exception as e:
                print(f"Error during gitignore check for {absolute_path}: {e}")
                return False

        # Apply the filter starting from the invisible root
        root_node = self.model.invisibleRootItem()
        # --- Apply filter without blocking signals ---
        # self.model.blockSignals(True) # DO NOT BLOCK signals
        self._apply_filter_recursive(root_node, gitignore_check)
        # self.model.blockSignals(False)
        # --- No manual parent update needed if signals are not blocked ---
        print("Finished applying .gitignore filter.")

    def apply_hidden_filter(self):
        """Applies the hidden file/folder filter."""
        print("Applying hidden file/folder filter...")

        # Define the filter function for hidden items
        def hidden_check(item, absolute_path):
            # Check the actual name from the path
            return absolute_path.name.startswith('.')

        # Apply the filter starting from the invisible root
        root_node = self.model.invisibleRootItem()
        # --- Apply filter without blocking signals ---
        # self.model.blockSignals(True) # DO NOT BLOCK signals
        self._apply_filter_recursive(root_node, hidden_check)
        # self.model.blockSignals(False)
        # --- No manual parent update needed if signals are not blocked ---
        print("Finished applying hidden file/folder filter.")

    # --- End Filter Logic Implementation ---

    def on_item_changed(self, item: QStandardItem):
        """Handles changes to item check states, propagating changes up/down the tree."""
        if not item or not item.isCheckable():
            return

        # Block signals temporarily ONLY within this handler to prevent infinite loops
        # during the propagation logic below. Filtering actions call setCheckState
        # which should trigger this handler *without* signals blocked.
        self.model.blockSignals(True)

        current_check_state = item.checkState()
        item_type = item.data(TYPE_DATA_ROLE)

        # 1. Propagate check state downwards (Parent -> Children)
        # If a folder is checked or unchecked (not partially), set all children to the same state
        if item_type == "folder" and current_check_state != Qt.CheckState.PartiallyChecked:
            self._set_child_check_state_recursive(item, current_check_state)

        # 2. Propagate check state upwards (Child -> Parent)
        self._update_parent_check_state(item)

        # Re-enable signals before exiting the handler
        self.model.blockSignals(False)

    def _set_child_check_state_recursive(self, parent_item: QStandardItem, state: Qt.CheckState):
        """Sets the check state for all checkable children of a parent item."""
        if state == Qt.CheckState.PartiallyChecked:  # Should not happen with current logic, but safeguard
            return
        for row in range(parent_item.rowCount()):
            child = parent_item.child(row, 0)
            if child and child.isCheckable():
                # Only change state if it's different, prevents unnecessary signal emissions if we weren't blocking
                if child.checkState() != state:
                    child.setCheckState(state)
                # Recursion happens naturally because setting checkState on a child folder
                # will trigger on_item_changed for that child (once signals are unblocked).

    def _update_parent_check_state(self, item: QStandardItem):
        """Updates the check state of a parent item based on its children's states."""
        parent = item.parent()

        # Stop if no parent, parent is the invisible root, or parent is not tristate enabled
        if (not parent or parent == self.model.invisibleRootItem() or
                not (parent.flags() & Qt.ItemFlag.ItemIsUserTristate)):
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

        # Determine the new parent state
        # Default if no checkable children or all unchecked
        new_parent_state = Qt.CheckState.Unchecked

        if partially_checked_children > 0:
            # If any child is partial, parent is partial
            new_parent_state = Qt.CheckState.PartiallyChecked
        elif checked_children == total_checkable_children and total_checkable_children > 0:
            # If all children are checked, parent is checked
            new_parent_state = Qt.CheckState.Checked
        elif checked_children > 0:
            # If some (but not all) are checked, and none are partial, parent is partial
            new_parent_state = Qt.CheckState.PartiallyChecked
        # else: stays Unchecked (covered by default)

        # Only update if the state actually changes
        if parent.checkState() != new_parent_state:
            parent.setCheckState(new_parent_state)
            # The change to the parent will trigger on_item_changed for the parent,
            # causing the update to propagate further up the chain (if needed).

    def accept(self):
        """Called when OK is clicked. Collects selected items before closing."""
        self._selected_items_for_worker = []
        root = self.model.invisibleRootItem()
        # Use the parent of the initially selected folder as the base path
        # This ensures relative paths are consistent if the user selected Folder/Subfolder
        base_path_for_dialog_items = str(self.folder_path.parent.resolve())
        self._collect_selected_items_recursive(
            root, base_path_for_dialog_items)
        super().accept()  # Close the dialog with Accepted code

    def _collect_selected_items_recursive(self, parent_item: QStandardItem, base_path_str: str):
        """Recursively traverses the model to find checked or partially checked items."""
        for row in range(parent_item.rowCount()):
            item = parent_item.child(row, 0)
            if not item or not item.isCheckable():
                continue

            state = item.checkState()
            item_type = item.data(TYPE_DATA_ROLE)
            item_path_str = item.data(PATH_DATA_ROLE)

            if not item_type or not item_path_str:  # Skip error items or improperly configured items
                continue

            # If fully checked, add it directly (whether file or folder)
            # We now need to add folders *as folders* so the MergerWorker can scan them
            if state == Qt.CheckState.Checked:
                # Worker needs ('type', 'absolute_path', 'base_path_for_relativity')
                self._selected_items_for_worker.append(
                    # Pass the actual type ('file' or 'folder')
                    (item_type, item_path_str, base_path_str))
            # If partially checked, only recurse into folders
            elif state == Qt.CheckState.PartiallyChecked:
                if item_type == "folder":
                    # Recurse deeper, passing the same base path
                    self._collect_selected_items_recursive(item, base_path_str)
            # If unchecked, ignore this item and its children

    def get_selected_items(self):
        """Returns the list of selected items formatted for the MergerWorker."""
        # Format: List[Tuple(type_str, path_str, base_path_str)]
        # Ensure folders are passed as 'folder' type, not 'folder-root'
        return self._selected_items_for_worker
