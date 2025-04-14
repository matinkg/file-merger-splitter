import pathlib
from PyQt6.QtWidgets import (
    QDialog, QTreeView, QVBoxLayout, QLabel, QDialogButtonBox, QStyle
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
        self.folder_path = pathlib.Path(folder_path_str)
        self._selected_items_for_worker = []
        self.setWindowTitle(f"Select items in: {self.folder_path.name}")
        self.setMinimumSize(500, 500)
        self.setSizeGripEnabled(True)  # Allow resizing

        layout = QVBoxLayout(self)

        self.tree_view = QTreeView()
        self.tree_view.setHeaderHidden(True)
        self.model = QStandardItemModel()
        self.tree_view.setModel(self.model)

        layout.addWidget(
            QLabel(f"Select items to include from:\n<b>{self.folder_path}</b>"))
        layout.addWidget(self.tree_view, 1)  # Give tree view stretch factor

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

    def on_item_changed(self, item: QStandardItem):
        """Handles changes to item check states, propagating changes up/down the tree."""
        if not item or not item.isCheckable():
            return

        # Block signals to prevent infinite loops during updates
        self.model.blockSignals(True)

        current_check_state = item.checkState()
        item_type = item.data(TYPE_DATA_ROLE)

        # 1. Propagate check state downwards (Parent -> Children)
        # If a folder is checked or unchecked (not partially), set all children to the same state
        if item_type == "folder" and current_check_state != Qt.CheckState.PartiallyChecked:
            self._set_child_check_state_recursive(item, current_check_state)

        # 2. Propagate check state upwards (Child -> Parent)
        self._update_parent_check_state(item)

        # Re-enable signals
        self.model.blockSignals(False)

    def _set_child_check_state_recursive(self, parent_item: QStandardItem, state: Qt.CheckState):
        """Sets the check state for all checkable children of a parent item."""
        if state == Qt.CheckState.PartiallyChecked:  # Should not happen with current logic, but safeguard
            return
        for row in range(parent_item.rowCount()):
            child = parent_item.child(row, 0)
            if child and child.isCheckable():
                if child.checkState() != state:
                    child.setCheckState(state)
                # Recurse if the child is also a folder (has tristate capability)
                # No need for explicit recursion here if on_item_changed handles it,
                # but direct setting can be faster. Let's keep it simple for now.
                # if child.data(TYPE_DATA_ROLE) == "folder":
                #     self._set_child_check_state_recursive(child, state) # Already handled by signal chain

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

        # Default if no children or all unchecked
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

        # Only update if the state actually changes
        if parent.checkState() != new_parent_state:
            parent.setCheckState(new_parent_state)
            # No need to call _update_parent_check_state(parent) here, signal chain handles it

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
            if state == Qt.CheckState.Checked:
                # Worker needs ('type', 'absolute_path', 'base_path_for_relativity')
                self._selected_items_for_worker.append(
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
        return self._selected_items_for_worker
