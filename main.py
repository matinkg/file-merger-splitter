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
from PyQt6.QtCore import (
    Qt, QThread, pyqtSignal, QObject, QDir, QModelIndex, QAbstractItemModel
)

# --- Constants ---
START_DELIMITER_FORMAT = "--- START FILE: {filepath} ---"
END_DELIMITER_FORMAT = "--- END FILE: {filepath} ---"
# Regex to find the start delimiter and capture the filepath (non-greedy)
START_DELIMITER_REGEX = re.compile(
    r"^--- START FILE: (.*?) ---$")  # Made path capture non-greedy

# --- Data Roles for Tree Items ---
PATH_DATA_ROLE = Qt.ItemDataRole.UserRole + \
    1      # Stores the full absolute path (str)
# Stores "file", "folder", or "folder-root" (str)
TYPE_DATA_ROLE = Qt.ItemDataRole.UserRole + 2
# Stores the base path for relative calculation (str)
BASE_PATH_DATA_ROLE = Qt.ItemDataRole.UserRole + 3


# --- Worker Signals ---
class WorkerSignals(QObject):
    ''' Defines signals available from a running worker thread. '''
    progress = pyqtSignal(int)       # Percentage progress
    log = pyqtSignal(str)            # Log message
    finished = pyqtSignal(bool, str)  # Success (bool), final message (str)
    error = pyqtSignal(str)          # Error message used for critical failures


# --- Merger Worker ---
class MergerWorker(QObject):
    ''' Performs the file merging in a separate thread. '''
    signals = WorkerSignals()

    def __init__(self, items_to_merge, output_file):
        super().__init__()
        # items_to_merge is expected to be a list of tuples:
        # ('file', '/path/to/file.txt', '/path/to') -> Type, Absolute Path, Base Path for relative calc
        # ('folder', '/path/to/folder', '/path/to') -> Type, Absolute Path, Base Path for relative calc
        self.items_to_merge = items_to_merge
        self.output_file = output_file
        self.is_running = True
        # Debug log
        self.log(
            f"Worker received {len(items_to_merge)} initial selection items.")

    def stop(self):
        self.log("Stop signal received by MergerWorker.")
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
        # Use a set of resolved paths to avoid processing the *exact same file* multiple times
        # even if selected via different routes (e.g., once directly, once via folder)
        encountered_resolved_paths = set()

        try:
            self.log("Scanning files and folders based on input selections...")
            initial_item_count = len(self.items_to_merge)
            # List to hold (absolute_path, relative_path, size) tuples
            files_discovered_in_scan = []

            # --- Phase 1: Discover all files based on initial selections ---
            for item_idx, (item_type, item_path_str, base_path_str) in enumerate(self.items_to_merge):
                if not self.is_running:
                    break
                self.log(
                    f"Processing selection {item_idx+1}/{initial_item_count}: Type='{item_type}', Path='{item_path_str}', Base='{base_path_str}'")

                try:
                    # Resolve symlinks, normalize
                    item_path = pathlib.Path(item_path_str).resolve()
                    base_path = pathlib.Path(base_path_str).resolve(
                    ) if base_path_str else item_path.parent  # Resolve base, fallback if needed
                except OSError as e:
                    self.log(
                        f"Warning: Could not resolve path '{item_path_str}' or base '{base_path_str}': {e}. Skipping item.")
                    continue
                except Exception as e:  # Catch potential errors during Path object creation
                    self.log(
                        f"Warning: Error processing path '{item_path_str}': {e}. Skipping item.")
                    continue

                if item_type == "file":
                    if item_path.is_file():
                        if item_path not in encountered_resolved_paths:
                            try:
                                # Calculate relative path based on the provided base_path
                                relative_path = item_path.relative_to(
                                    base_path)
                                fsize = item_path.stat().st_size
                                files_discovered_in_scan.append(
                                    (item_path, relative_path, fsize))
                                total_size += fsize
                                encountered_resolved_paths.add(item_path)
                                # self.log(f"  Added file: {item_path} (Rel: {relative_path})")
                            except ValueError:
                                # This happens if base_path is not an ancestor of item_path
                                self.log(
                                    f"Warning: Could not determine relative path for '{item_path}' against base '{base_path}'. Using filename as relative path.")
                                # Use filename as fallback
                                relative_path = pathlib.Path(item_path.name)
                                try:
                                    fsize = item_path.stat().st_size
                                    files_discovered_in_scan.append(
                                        (item_path, relative_path, fsize))
                                    total_size += fsize
                                    encountered_resolved_paths.add(item_path)
                                except OSError as e_size:
                                    self.log(
                                        f"Warning: Could not get size for {item_path}: {e_size}. Using size 0.")
                                    files_discovered_in_scan.append(
                                        (item_path, relative_path, 0))
                                    encountered_resolved_paths.add(item_path)
                            except OSError as e:
                                self.log(
                                    f"Warning: Could not get size for {item_path}: {e}. Using size 0.")
                                files_discovered_in_scan.append(
                                    (item_path, item_path.relative_to(base_path), 0))
                                encountered_resolved_paths.add(item_path)
                            except Exception as e:
                                self.log(
                                    f"Warning: Unexpected error processing file entry {item_path}: {e}")
                        else:
                            self.log(
                                f"Skipping duplicate file (already encountered): {item_path}")
                    else:
                        self.log(
                            f"Warning: Selected file not found during scan: {item_path}")

                elif item_type == "folder":
                    if item_path.is_dir():
                        self.log(
                            f"Scanning folder: {item_path} (Base for relative paths: {base_path})")
                        # Use os.walk with followlinks=True to match pathlib.resolve() behavior if needed,
                        # but generally avoid symlink loops if possible. Stick to False unless needed.
                        for root, _, filenames in os.walk(str(item_path), followlinks=False):
                            if not self.is_running:
                                break
                            root_path = pathlib.Path(root)
                            for filename in filenames:
                                if not self.is_running:
                                    break
                                try:
                                    # Resolve full path
                                    file_path = (
                                        root_path / filename).resolve()

                                    if file_path not in encountered_resolved_paths:
                                        try:
                                            # Try making path relative to the original base_path
                                            relative_path = file_path.relative_to(
                                                base_path)
                                            fsize = file_path.stat().st_size
                                            files_discovered_in_scan.append(
                                                (file_path, relative_path, fsize))
                                            total_size += fsize
                                            encountered_resolved_paths.add(
                                                file_path)
                                            # self.log(f"  Added file from folder: {file_path} (Rel: {relative_path})")
                                        except ValueError:
                                            self.log(
                                                f"Warning: Could not make '{file_path}' relative to base '{base_path}'. Using path relative to scanned folder '{item_path}'.")
                                            # Fallback: relative to the folder being walked (item_path)
                                            try:
                                                relative_path_fallback = file_path.relative_to(
                                                    item_path)
                                                fsize = file_path.stat().st_size
                                                files_discovered_in_scan.append(
                                                    (file_path, relative_path_fallback, fsize))
                                                total_size += fsize
                                                encountered_resolved_paths.add(
                                                    file_path)
                                            except ValueError:  # Should not happen if os.walk is correct, but safety first
                                                self.log(
                                                    f"Error: Could not even make '{file_path}' relative to its walk root '{item_path}'. Using filename only.")
                                                relative_path_final = pathlib.Path(
                                                    file_path.name)
                                                fsize = file_path.stat().st_size  # Still try get size
                                                files_discovered_in_scan.append(
                                                    (file_path, relative_path_final, fsize))
                                                total_size += fsize
                                                encountered_resolved_paths.add(
                                                    file_path)
                                            except OSError as e_size:
                                                self.log(
                                                    f"Warning: Could not get size for {file_path}: {e_size}. Using size 0.")
                                                files_discovered_in_scan.append(
                                                    # Use fallback relative path
                                                    (file_path, file_path.relative_to(item_path), 0))
                                                encountered_resolved_paths.add(
                                                    file_path)
                                        except OSError as e:
                                            self.log(
                                                f"Warning: Could not get size for {file_path}: {e}. Using size 0.")
                                            files_discovered_in_scan.append(
                                                # Try original relative path
                                                (file_path, file_path.relative_to(base_path), 0))
                                            encountered_resolved_paths.add(
                                                file_path)

                                    # else: # Log duplicates only once (handled by the 'file' section check)
                                    #     self.log(f"Skipping duplicate file during folder scan: {file_path}")

                                except OSError as e_resolve:
                                    self.log(
                                        f"Warning: Could not resolve or access path under {root_path} for filename '{filename}': {e_resolve}")
                                except Exception as e:
                                    self.log(
                                        f"Warning: Could not process file '{filename}' in folder scan under {root_path}: {e}")
                            if not self.is_running:
                                break  # Break inner loop
                        if not self.is_running:
                            break  # Break outer walk loop
                    else:
                        self.log(
                            f"Warning: Selected folder not found during scan: {item_path}")

            if not self.is_running:
                self.log("Merge cancelled during scanning phase.")
                self.signals.finished.emit(
                    False, "Merge cancelled during scan.")
                return

            # Sort the unique files based on their relative paths for consistent order
            # Use as_posix() for reliable cross-platform sorting string
            files_to_process = sorted(
                files_discovered_in_scan, key=lambda x: x[1].as_posix())

            if not files_to_process:
                self.log("No valid, unique files found to merge after scanning.")
                self.signals.finished.emit(False, "No files to merge.")
                return

            self.log(
                f"Found {len(files_to_process)} unique files to merge. Calculated total size: {total_size} bytes.")
            self.signals.progress.emit(0)

            # --- Phase 2: Write the files to the output ---
            output_file_path = pathlib.Path(self.output_file)
            try:
                # Ensure output directory exists
                output_file_path.parent.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                self.log(
                    f"Error: Could not create output directory {output_file_path.parent}: {e}")
                self.signals.error.emit(
                    f"Failed to create output directory: {e}")
                self.signals.finished.emit(
                    False, "Merge failed: Could not create output directory.")
                return

            with open(output_file_path, "w", encoding="utf-8", errors='replace') as outfile:
                total_files_count = len(files_to_process)
                for i, (absolute_path, relative_path, fsize) in enumerate(files_to_process):
                    if not self.is_running:
                        break

                    # Use POSIX paths in delimiters for consistency regardless of OS
                    relative_path_str = relative_path.as_posix()
                    self.log(
                        f"Merging ({i+1}/{total_files_count}): '{relative_path_str}' (from: {absolute_path})")

                    start_delimiter = START_DELIMITER_FORMAT.format(
                        filepath=relative_path_str)
                    end_delimiter = END_DELIMITER_FORMAT.format(
                        filepath=relative_path_str)

                    outfile.write(start_delimiter + "\n")
                    content_written = False
                    file_content = ""
                    try:
                        try:
                            # Try reading as UTF-8 first (most common)
                            with open(absolute_path, "r", encoding="utf-8") as infile:
                                file_content = infile.read()
                        except UnicodeDecodeError:
                            self.log(
                                f"Warning: Non-UTF-8 file detected: '{relative_path_str}'. Reading with 'latin-1' encoding.")
                            try:
                                with open(absolute_path, "r", encoding="latin-1") as infile:
                                    file_content = infile.read()
                            except Exception as e_latin:
                                self.log(
                                    f"Error reading file '{absolute_path}' even with latin-1: {e_latin}. Inserting error message.")
                                file_content = f"Error reading file (tried utf-8, latin-1): {e_latin}"
                        except FileNotFoundError:
                            self.log(
                                f"Error: File disappeared before reading: '{absolute_path}'. Inserting error message.")
                            file_content = f"Error: File not found during merge process."
                        except OSError as e_os:
                            self.log(
                                f"Error: OS error reading file '{absolute_path}': {e_os}. Inserting error message.")
                            file_content = f"Error reading file (OS Error): {e_os}"
                        except Exception as e:
                            self.log(
                                f"Error reading file '{absolute_path}': {e}. Inserting error message.")
                            file_content = f"Error reading file: {e}"

                        outfile.write(file_content)
                        content_written = True
                        # Ensure a newline after content, before the end delimiter
                        if file_content and not file_content.endswith('\n'):
                            outfile.write("\n")

                    except Exception as e_outer:
                        # Catch unexpected errors during the writing *process* itself
                        self.log(
                            f"Critical error processing file {absolute_path}: {e_outer}\n{traceback.format_exc()}")
                        # Ensure delimiters are written even if content writing fails catastrophically
                        if not content_written:
                            outfile.write(
                                f"Error processing file: {e_outer}\n")

                    # Add end delimiter and extra newline for readability between files
                    outfile.write(end_delimiter + "\n\n")

                    processed_size += fsize
                    processed_files_count += 1
                    # Update progress based on size if available, otherwise by file count
                    if total_size > 0:
                        progress_percent = int(
                            (processed_size / total_size) * 100)
                        self.signals.progress.emit(
                            min(progress_percent, 100))  # Cap at 100
                    elif total_files_count > 0:
                        # Fallback progress based on file count
                        self.signals.progress.emit(
                            min(int((processed_files_count / total_files_count) * 100), 100))

            # --- Finalization ---
            if not self.is_running:
                self.log("Merge cancelled during writing phase.")
                try:
                    # Attempt to clean up the possibly incomplete output file
                    if output_file_path.exists():
                        output_file_path.unlink()  # Use pathlib's unlink
                        self.log(
                            f"Removed incomplete file: {output_file_path}")
                except OSError as e:
                    self.log(
                        f"Could not remove incomplete file '{output_file_path}': {e}")
                self.signals.finished.emit(False, "Merge cancelled.")
            else:
                self.signals.progress.emit(100)
                self.log("Merge process completed successfully.")
                self.signals.finished.emit(
                    True, f"Merge successful! {len(files_to_process)} files merged into '{output_file_path.name}'.")

        except Exception as e:
            self.log(
                f"An unexpected error occurred during merge: {e}\n{traceback.format_exc()}")
            self.signals.error.emit(f"Merge failed: {e}")
            self.signals.finished.emit(
                False, f"Merge failed due to unexpected error: {e}")
        finally:
            # Ensure progress hits 100 in case of early exit from try block
            if self.is_running:  # Only force to 100 if not cancelled
                self.signals.progress.emit(100)


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
        self.log("Stop signal received by SplitterWorker.")
        self.is_running = False

    def log(self, msg):
        self.signals.log.emit(msg)

    def run(self):
        self.log(f"Starting split process for: {self.merged_file}")
        self.log(f"Output directory: {self.output_dir}")
        self.signals.progress.emit(0)

        try:
            merged_file_path = pathlib.Path(self.merged_file)
            if not merged_file_path.is_file():
                raise FileNotFoundError(
                    f"Input file not found: {self.merged_file}")

            total_size = merged_file_path.stat().st_size
            processed_size = 0
            file_count = 0
            created_file_paths = set()  # Keep track of files created

            with open(merged_file_path, "r", encoding="utf-8", errors='replace') as infile:
                current_file_path_relative = None
                current_file_content = []
                in_file_block = False

                for line_num, line in enumerate(infile):
                    if not self.is_running:
                        break

                    # Update progress based on bytes read
                    processed_size += len(line.encode('utf-8',
                                          errors='replace'))
                    if total_size > 0:
                        progress_percent = int(
                            (processed_size / total_size) * 100)
                        self.signals.progress.emit(
                            min(progress_percent, 100))  # Cap at 100

                    line_stripped = line.strip()
                    start_match = START_DELIMITER_REGEX.match(line_stripped)

                    if start_match:
                        # Found a potential start delimiter
                        if in_file_block and current_file_path_relative:
                            # We were already inside a block, implies missing end delimiter
                            self.log(f"Warning: Found new START delimiter '{start_match.group(1)}' near line {line_num+1} "
                                     f"before END delimiter for previous block '{current_file_path_relative}'. Saving previous block.")
                            if self._write_file(current_file_path_relative, "".join(current_file_content)):
                                file_count += 1
                                created_file_paths.add(
                                    self.output_dir.joinpath(current_file_path_relative))

                        # Capture the relative path from the new delimiter
                        potential_relative_path = start_match.group(1)

                        # --- Basic Path Safety Check ---
                        # Avoid absolute paths (like /etc/passwd or C:\Windows) and excessive traversal (../../)
                        # Normalize separators for checks
                        normalized_path_check = potential_relative_path.replace(
                            "\\", "/")
                        is_safe = True
                        if pathlib.Path(potential_relative_path).is_absolute():
                            self.log(
                                f"Error: Security risk! Absolute path found in delimiter: '{potential_relative_path}' near line {line_num+1}. Skipping block.")
                            is_safe = False
                        # Check for parent traversal components. Allow simple cases like 'a/../b' resolving to 'b' within the dir,
                        # but disallow attempts to go *above* the output dir root via initial '../'.
                        # This check is basic; the _write_file check is more robust.
                        elif normalized_path_check.startswith("../") or "/../" in normalized_path_check:
                            # Further check needed in _write_file, but log a warning here.
                            self.log(
                                f"Warning: Potential path traversal detected in delimiter: '{potential_relative_path}' near line {line_num+1}. Final check during write.")
                            # Pass through for now, let _write_file do the final check

                        if not is_safe:
                            # Reset state, skip this block entirely
                            current_file_path_relative = None
                            current_file_content = []
                            in_file_block = False
                            continue  # Move to next line

                        # Path seems acceptable so far, start the new block
                        current_file_path_relative = potential_relative_path
                        current_file_content = []
                        in_file_block = True
                        # self.log(f"Found file block start: {current_file_path_relative}")
                        continue  # Move to next line, don't include start delimiter in content

                    # Check for the end delimiter *only if* we are inside a block
                    if in_file_block and current_file_path_relative:
                        # Construct the expected end delimiter for the *current* block
                        expected_end_delimiter = END_DELIMITER_FORMAT.format(
                            filepath=current_file_path_relative)
                        if line_stripped == expected_end_delimiter:
                            # Found the end delimiter for the current block, write the file
                            if self._write_file(current_file_path_relative, "".join(current_file_content)):
                                file_count += 1
                                created_file_paths.add(
                                    self.output_dir.joinpath(current_file_path_relative))
                            # Reset state for the next potential block
                            in_file_block = False
                            current_file_path_relative = None
                            current_file_content = []
                            # Don't add end delimiter line to content, just continue
                            continue
                        else:
                            # Not the end delimiter, append the line (with original newline) to the current file's content
                            current_file_content.append(line)

                # --- Loop finished ---
                if not self.is_running:
                    self.log("Split cancelled during file processing.")
                    self.signals.finished.emit(False, "Split cancelled.")
                    return

                # Check if the file ended while still inside a block (missing end delimiter)
                if in_file_block and current_file_path_relative:
                    self.log(
                        f"Warning: Merged file ended before finding END delimiter for '{current_file_path_relative}'. Saving remaining content.")
                    if self._write_file(current_file_path_relative, "".join(current_file_content)):
                        file_count += 1
                        created_file_paths.add(
                            self.output_dir.joinpath(current_file_path_relative))

            # --- Post-processing ---
            self.signals.progress.emit(100)
            if file_count > 0:
                final_message = f"Split successful! {file_count} files created in '{self.output_dir.name}'."
                self.log(final_message)
                self.signals.finished.emit(True, final_message)
            elif not self.is_running:  # If cancelled, don't report success
                pass  # Finished signal already emitted
            else:
                final_message = "Split finished, but no valid file blocks were found or extracted."
                self.log(final_message)
                # Report as non-success if 0 files
                self.signals.finished.emit(False, final_message)

        except FileNotFoundError as e:
            error_msg = f"Input merged file not found: {self.merged_file}"
            self.log(f"Error: {error_msg}")
            self.signals.error.emit(error_msg)
            self.signals.finished.emit(False, f"Split failed: {error_msg}")
        except OSError as e:
            error_msg = f"OS error during split process: {e}"
            self.log(f"Error: {error_msg}\n{traceback.format_exc()}")
            self.signals.error.emit(error_msg)
            self.signals.finished.emit(False, f"Split failed: {error_msg}")
        except Exception as e:
            error_msg = f"An unexpected critical error occurred during split: {e}"
            self.log(f"{error_msg}\n{traceback.format_exc()}")
            self.signals.error.emit(error_msg)
            self.signals.finished.emit(False, f"Split failed: {error_msg}")
        finally:
            # Ensure progress hits 100 if the operation wasn't cancelled
            if self.is_running:
                self.signals.progress.emit(100)

    def _write_file(self, relative_path_str, content):
        """Helper to write content to the appropriate file within the output directory.
           Includes safety checks. Returns True on success, False on failure."""
        if not relative_path_str:
            self.log(
                "Error: Attempted to write file with empty relative path. Skipping.")
            return False

        try:
            # --- Path Construction and Validation ---
            # Create the initial target path object. Don't resolve yet.
            target_path = self.output_dir / pathlib.Path(relative_path_str)

            # --- Security Check: Prevent writing outside the output directory ---
            # Resolve the intended target path *after* joining with output_dir.
            # Use resolve(strict=False) which works even if the file/dirs don't exist yet.
            # If strict=True is needed, ensure parent dirs are created *first*. Let's use strict=False.
            # HOWEVER, resolve() can fail on invalid paths (e.g. forbidden chars on Windows).
            # So, we need to handle potential errors here too.

            try:
                # Resolve output directory base path ONCE for comparison.
                # Output dir MUST exist here.
                output_dir_resolved = self.output_dir.resolve(strict=True)

                # Attempt to resolve the target path.
                target_path_resolved = target_path.resolve(strict=False)

            except (OSError, ValueError) as e_resolve:  # Catch errors during resolve()
                self.log(
                    f"Error: Invalid path generated for '{relative_path_str}': {e_resolve}. Skipping write.")
                return False
            except FileNotFoundError:  # Should not happen if output_dir exists, but safety.
                self.log(
                    f"Error: Output directory '{self.output_dir}' seems to have disappeared. Cannot write '{relative_path_str}'.")
                return False

            # Now perform the crucial check: is the resolved target path *within* the resolved output directory?
            # Check if output_dir_resolved is the same as target_path_resolved or one of its parents.
            is_within_output_dir = (output_dir_resolved == target_path_resolved or
                                    output_dir_resolved in target_path_resolved.parents)

            if not is_within_output_dir:
                self.log(f"Error: Security risk! Path '{relative_path_str}' resolved to '{target_path_resolved}', "
                         f"which is outside the designated output directory '{output_dir_resolved}'. Skipping write.")
                return False

            # --- Directory Creation and Writing ---
            self.log(f"Attempting to create file: {target_path_resolved}")
            # Create parent directories for the *resolved* path if they don't exist
            target_path_resolved.parent.mkdir(parents=True, exist_ok=True)

            # Write the content to the file
            with open(target_path_resolved, "w", encoding="utf-8") as outfile:
                outfile.write(content)
            # self.log(f"Successfully wrote: {target_path_resolved}") # Optional: Log success per file
            return True

        except OSError as e:
            self.log(
                f"Error writing file '{target_path_resolved}' (OS Error): {e}")
            return False
        except Exception as e:
            # Catch any other unexpected errors during file writing
            self.log(
                f"Error writing file for relative path '{relative_path_str}' (Resolved: {target_path_resolved if 'target_path_resolved' in locals() else 'N/A'}) (General Error): {e}\n{traceback.format_exc()}")
            return False


# --- Folder Selection Dialog ---
class FolderSelectionDialog(QDialog):
    """A dialog to select specific files and subfolders within a chosen folder using a tree view with tristate checkboxes."""

    def __init__(self, folder_path_str, parent=None):
        super().__init__(parent)
        self.folder_path = pathlib.Path(folder_path_str)
        # Stores the final list of selected (type, abs_path, base_path) for the MergerWorker
        self._selected_items_for_worker = []

        self.setWindowTitle(f"Select items in: {self.folder_path.name}")
        self.setMinimumSize(500, 500)  # Adjusted minimum size
        self.setSizeGripEnabled(True)

        layout = QVBoxLayout(self)
        self.tree_view = QTreeView()
        self.tree_view.setHeaderHidden(True)
        self.model = QStandardItemModel()
        self.tree_view.setModel(self.model)
        # Enable drag & drop? Maybe later.
        # self.tree_view.setDragDropMode(QAbstractItemView.DragDropMode.InternalMove)
        # self.tree_view.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        # self.tree_view.setDropIndicatorShown(True)

        layout.addWidget(
            QLabel(f"Select items to include from:\n<b>{self.folder_path}</b>"))
        layout.addWidget(self.tree_view, 1)  # Give tree view expansion space

        # --- Icons ---
        # Use standard pixmaps which should respect the application style (incl. dark mode)
        style = self.style()  # Get current style
        self.folder_icon = style.standardIcon(QStyle.StandardPixmap.SP_DirIcon)
        self.file_icon = style.standardIcon(QStyle.StandardPixmap.SP_FileIcon)
        self.error_icon = style.standardIcon(
            QStyle.StandardPixmap.SP_MessageBoxWarning)

        # --- Populate Tree ---
        # Block signals during initial population for performance
        self.model.blockSignals(True)
        self.populate_tree()
        self.model.blockSignals(False)
        self.tree_view.expandToDepth(0)  # Expand top level initially

        # --- Connect Signals ---
        # Use itemChanged for checkbox interactions AFTER initial population
        self.model.itemChanged.connect(self.on_item_changed)

        # --- Buttons ---
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def _populate_recursive(self, parent_node: QStandardItem, current_path: pathlib.Path):
        """Recursively populates the model with files and folders. Uses QStandardItem."""
        try:
            # Sort directories first, then files, case-insensitively
            items_in_dir = sorted(
                list(current_path.iterdir()),
                key=lambda p: (not p.is_dir(), p.name.lower())
            )
        except OSError as e:
            # Display error message as a non-interactive item
            error_text = f"Error reading: {e.strerror} ({current_path.name})"
            error_item = QStandardItem(self.error_icon, error_text)
            # Not selectable, not checkable
            error_item.setFlags(Qt.ItemFlag.ItemIsEnabled)
            error_item.setToolTip(
                f"Could not access directory:\n{current_path}\n{e}")
            parent_node.appendRow(error_item)
            # Also print to console/log
            print(f"OS Error reading {current_path}: {e}")
            return  # Stop recursion for this branch

        for item_path in items_in_dir:
            item = QStandardItem(item_path.name)
            item.setEditable(False)  # Items should not be renamed here
            item.setCheckable(True)  # All selectable items are checkable
            # Default to checked initially
            item.setCheckState(Qt.CheckState.Checked)

            # Store essential data in the item using roles
            # Store resolved absolute path
            item.setData(str(item_path.resolve()), PATH_DATA_ROLE)

            if item_path.is_dir():
                item.setIcon(self.folder_icon)
                item.setData("folder", TYPE_DATA_ROLE)
                # --- CRITICAL for tristate: Set the ItemIsUserTristate flag ---
                # This flag tells the view to handle the third state (PartiallyChecked)
                # and allows the parent state to be calculated correctly.
                item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserTristate)
                # No need to call item.setTristate(True) - the flag is sufficient.

                parent_node.appendRow(item)
                # Recursively populate the child folder
                self._populate_recursive(item, item_path)
            elif item_path.is_file():
                item.setIcon(self.file_icon)
                item.setData("file", TYPE_DATA_ROLE)
                # Ensure files DO NOT have the tristate flag
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsUserTristate)
                parent_node.appendRow(item)
            # else: Ignore symlinks to non-files/dirs, sockets, etc.

    def populate_tree(self):
        """Scans the root folder and populates the tree view."""
        self.model.clear()  # Clear existing items if repopulating
        root_node = self.model.invisibleRootItem()
        # Start population from the dialog's root folder path
        self._populate_recursive(root_node, self.folder_path)

    def on_item_changed(self, item: QStandardItem):
        """Handles changes in item check state, propagating changes up and down the tree."""
        if not item or not item.isCheckable():
            return

        # Block signals temporarily to prevent recursive loops during updates
        self.model.blockSignals(True)

        current_check_state = item.checkState()
        item_type = item.data(TYPE_DATA_ROLE)

        # --- Downward Propagation (Parent -> Children) ---
        # If a folder's state was changed to fully Checked or Unchecked (not by child updates),
        # update all its checkable children to match.
        # This requires knowing if the change originated from user click or parent update.
        # We assume changes here *can* originate from user clicks on folders.
        # Only propagate Checked/Unchecked states downwards. Partial state arises from children.
        if item_type == "folder" and current_check_state != Qt.CheckState.PartiallyChecked:
            self._set_child_check_state_recursive(item, current_check_state)

        # --- Upward Propagation (Child -> Parent) ---
        # Update the check state of the parent item based on the states of its children.
        self._update_parent_check_state(item)

        # Re-enable signals
        self.model.blockSignals(False)

    def _set_child_check_state_recursive(self, parent_item: QStandardItem, state: Qt.CheckState):
        """Recursively sets the check state of all checkable children."""
        # Do not propagate partial state downwards.
        if state == Qt.CheckState.PartiallyChecked:
            # This should ideally not happen if called from on_item_changed logic above,
            # but acts as a safeguard.
            return

        for row in range(parent_item.rowCount()):
            child = parent_item.child(row, 0)  # Assuming data is in column 0
            if child and child.isCheckable():
                # Only change state if it's different, prevents unnecessary signals/updates
                if child.checkState() != state:
                    child.setCheckState(state)
                    # No need to call _set_child_check_state_recursive here,
                    # because setting the child's state will trigger on_item_changed for it,
                    # which will handle further downward propagation if the child is a folder.

    def _update_parent_check_state(self, item: QStandardItem):
        """Updates the parent's check state based on the collective state of its siblings and itself."""
        parent = item.parent()

        # Stop if no parent, parent is the invisible root, or parent is not a tristate folder
        if (not parent or
            parent == self.model.invisibleRootItem() or
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
                # Implicitly, Unchecked count = total - checked - partial

        new_parent_state = Qt.CheckState.Unchecked  # Default state

        if partially_checked_children > 0:
            # If any child is partial, parent must be partial.
            new_parent_state = Qt.CheckState.PartiallyChecked
        elif checked_children == total_checkable_children and total_checkable_children > 0:
            # If all checkable children are checked.
            new_parent_state = Qt.CheckState.Checked
        elif checked_children > 0:
            # If some are checked, but not all (and none are partial), parent is partial.
            new_parent_state = Qt.CheckState.PartiallyChecked
        # else: If checked_children == 0 and partially_checked_children == 0,
        # state remains Unchecked (all children are unchecked, or there are no checkable children).

        # Only update the parent if its state needs to change.
        if parent.checkState() != new_parent_state:
            parent.setCheckState(new_parent_state)
            # Recursively update the grandparent after changing the parent's state
            # self._update_parent_check_state(parent) # This is called automatically because parent.setCheckState triggers itemChanged

    def accept(self):
        """Process selections when OK is clicked. Traverses the tree model."""
        self._selected_items_for_worker = []  # Clear previous results
        root = self.model.invisibleRootItem()
        # The 'base path' for items selected in this dialog should be the *parent*
        # of the folder we are viewing, so relative paths are calculated correctly from there.
        base_path_for_dialog_items = str(self.folder_path.parent.resolve())
        self._collect_selected_items_recursive(
            root, base_path_for_dialog_items)
        super().accept()  # Close the dialog with Accepted state

    def _collect_selected_items_recursive(self, parent_item: QStandardItem, base_path_str: str):
        """
        Recursively traverses the tree model to collect items for the MergerWorker.
        - Adds fully CHECKED items directly.
        - Recurses into PARTIALLY CHECKED folders to find checked items within.
        - Ignores UNCHECKED items.
        """
        for row in range(parent_item.rowCount()):
            item = parent_item.child(row, 0)
            if not item or not item.isCheckable():  # Skip non-items or non-checkable error messages
                continue

            state = item.checkState()
            item_type = item.data(TYPE_DATA_ROLE)
            # This should be the absolute path
            item_path_str = item.data(PATH_DATA_ROLE)

            # Skip items with missing essential data (shouldn't happen for valid entries)
            if not item_type or not item_path_str:
                continue

            if state == Qt.CheckState.Checked:
                # If the item itself is fully checked, add it to the list for the worker.
                # The worker will handle expanding checked folders.
                self._selected_items_for_worker.append(
                    (item_type, item_path_str, base_path_str))
            elif state == Qt.CheckState.PartiallyChecked:
                # If a folder is partially checked, we need to look inside it
                # to see *which* children are checked.
                if item_type == "folder":
                    # Recurse into this partially checked folder. The base path remains the same.
                    self._collect_selected_items_recursive(item, base_path_str)
            # else state == Qt.CheckState.Unchecked:
                # Do nothing, ignore unchecked items and their children.

    def get_selected_items(self):
        """Return the list of selected (type, path, base_path) tuples intended for the MergerWorker."""
        return self._selected_items_for_worker


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
        QLabel[text*="<b>"] { /* Matches labels containing <b> tag */
            font-weight: bold;
            color: #e0e0e0; /* Brighter text for titles */
        }
        /* Labels showing file paths */
        QLabel#OutputMergeLabel, QLabel#InputSplitLabel, QLabel#OutputSplitLabel {
            color: #cccccc;
            padding-left: 5px;
            border: 1px solid #404040; /* Subtle border for path labels */
            border-radius: 3px;
            padding: 4px; /* Add some padding inside */
            background-color: #333333; /* Slightly different background */
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


        /* Tree View (Main window and Dialog) */
        QTreeView {
            background-color: #333333;
            color: #dcdcdc;
            border: 1px solid #555555;
            border-radius: 4px;
            alternate-background-color: #3a3a3a; /* For List View look */
            /* show-decoration-selected: 1; */ /* Make selection background cover decoration */
            outline: 0px; /* Remove focus outline */
        }
        QTreeView::item {
            padding: 4px; /* Increased spacing around items */
            border-radius: 3px; /* Slightly rounded item background */
            border: none; /* Remove default item border */
        }
        QTreeView::item:selected {
            background-color: #569cd6; /* Accent selection */
            color: #ffffff;
        }
         QTreeView::item:hover:!selected { /* Hover only when not selected */
            background-color: #4a4a4a;
            color: #e8e8e8;
        }
        /* Style the branch lines */
        QTreeView::branch {
             background-color: transparent; /* Use background of tree */
             /* Use images for custom branch indicators if needed, see Qt docs */
             /* image: none; */
        }
        /* Style the expand/collapse indicators */
        QTreeView::branch:has-children:!has-siblings:closed,
        QTreeView::branch:closed:has-children:has-siblings {
                border-image: none;
                /* image: url(:/dark/branch_closed.png); */ /* Example */
        }
        QTreeView::branch:open:has-children:!has-siblings,
        QTreeView::branch:open:has-children:has-siblings  {
                border-image: none;
                /* image: url(:/dark/branch_open.png); */ /* Example */
        }

        /* Style the check boxes within the tree */
         QTreeView::indicator {
            width: 14px; /* Slightly larger */
            height: 14px;
            border: 1px solid #666;
            border-radius: 3px;
            background-color: #444;
         }
         QTreeView::indicator:unchecked {
             /* image: url(:/dark/checkbox_unchecked.png); */
             background-color: #444; /* Default bg */
         }
         QTreeView::indicator:checked {
            /* image: url(:/dark/checkbox_checked.png); */
            background-color: #569cd6; /* Simple checked indicator color */
            border-color: #569cd6;
         }
         QTreeView::indicator:indeterminate {
            /* image: url(:/dark/checkbox_tristate.png); */
            background-color: #77aaff; /* Simple tristate color */
            border-color: #77aaff;
         }
         /* Hover/disabled states for indicators */
         QTreeView::indicator:hover {
            border-color: #888;
         }
         QTreeView::indicator:disabled {
             background-color: #555;
             border-color: #555;
         }


        /* Text Edit (Log Area) */
        QTextEdit {
            border: 1px solid #555555;
            border-radius: 4px;
            background-color: #2f2f2f; /* Slightly darker background for log */
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
            margin: 1px; /* Add margin for chunk to see border */
        }

        /* Tab Widget */
        QTabWidget::pane {
            border: 1px solid #555555;
            border-radius: 4px;
            background-color: #333333; /* Pane background */
            /* margin-top: -1px; */ /* Avoid overlap issues */
             border-top-left-radius: 0px; /* Sharp corner under tab */
            padding: 10px;
        }
        QTabBar::tab {
            background: #3c3c3c; /* Non-selected tab background */
            border: 1px solid #555555;
            border-bottom: none; /* Remove bottom border initially */
            border-top-left-radius: 4px;
            border-top-right-radius: 4px;
            min-width: 10ex; /* Wider tabs */
            padding: 8px 14px; /* More padding */
            margin-right: 2px;
            color: #bbbbbb; /* Dimmer text for non-selected tabs */
        }
        QTabBar::tab:hover {
            background: #4f4f4f; /* Hover for non-selected */
            color: #e0e0e0;
        }
        QTabBar::tab:selected {
            background: #333333; /* Selected tab matches pane background */
            /* border: 1px solid #555555; */ /* Keep border consistent */
            border-bottom-color: #333333; /* Make bottom border match pane bg (invisible line effect) */
            /* margin-bottom: -1px; */ /* Pull tab down slightly */
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
            opacity: 240; /* Slightly transparent */
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
            width: 14px; /* Slightly wider */
            margin: 0px 0px 0px 0px;
        }
        QScrollBar::handle:vertical {
            background: #5f5f5f; /* Slightly lighter handle */
            min-height: 25px;
            border-radius: 5px; /* More rounded */
        }
         QScrollBar::handle:vertical:hover {
            background: #6a6a6a;
        }
        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
            border: none; /* Hide arrows */
            background: none;
            height: 0px;
            subcontrol-position: top;
            subcontrol-origin: margin;
        }
        QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {
            background: none;
        }

        QScrollBar:horizontal {
            border: 1px solid #444444;
            background: #303030;
            height: 14px; /* Slightly wider */
            margin: 0px 0px 0px 0px;
        }
        QScrollBar::handle:horizontal {
            background: #5f5f5f; /* Slightly lighter handle */
            min-width: 25px;
            border-radius: 5px; /* More rounded */
        }
         QScrollBar::handle:horizontal:hover {
            background: #6a6a6a;
        }
        QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {
             border: none; /* Hide arrows */
             background: none;
             width: 0px;
             subcontrol-position: left;
             subcontrol-origin: margin;
        }
        QScrollBar::add-page:horizontal, QScrollBar::sub-page:horizontal {
            background: none;
        }
    """

    def __init__(self):
        super().__init__()
        # Internal list storing tuples (type, absolute_path_str, base_path_str) for the MergerWorker
        # This is the GROUND TRUTH for what will be merged.
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
        # For root window styling
        self.setObjectName("MergerSplitterAppWindow")
        self.setWindowTitle('File Merger & Splitter')
        self.setGeometry(150, 150, 850, 750)  # Increased default size

        main_layout = QVBoxLayout(self)
        self.tab_widget = QTabWidget()
        # Give tabs vertical expansion space
        main_layout.addWidget(self.tab_widget, 1)

        self.merge_tab = QWidget()
        self.split_tab = QWidget()
        # Add spaces for padding
        self.tab_widget.addTab(self.merge_tab, " Merge Files/Folders ")
        self.tab_widget.addTab(self.split_tab, " Split Merged File ")

        # --- Populate Merge Tab ---
        merge_layout = QVBoxLayout(self.merge_tab)

        # --- Top Buttons ---
        select_items_layout = QHBoxLayout()
        self.add_files_button = QPushButton("Add Files...")
        self.add_folder_button = QPushButton("Add Folder...")
        self.remove_item_button = QPushButton("Remove Selected")
        self.clear_list_button = QPushButton("Clear List")
        select_items_layout.addWidget(self.add_files_button)
        select_items_layout.addWidget(self.add_folder_button)
        select_items_layout.addSpacing(20)  # Spacer
        select_items_layout.addWidget(self.remove_item_button)
        select_items_layout.addWidget(self.clear_list_button)
        select_items_layout.addStretch()
        merge_layout.addLayout(select_items_layout)

        # --- Tree View for Items to Merge ---
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
            True)  # Enhance readability
        self.item_list_view.setSortingEnabled(
            False)  # Keep insertion order for now
        # Give tree view vertical expansion
        merge_layout.addWidget(self.item_list_view, 1)

        # --- Output File Selection ---
        output_merge_layout = QHBoxLayout()
        self.select_output_merge_button = QPushButton(
            "Select Output Merged File...")
        self.select_output_merge_button.setSizePolicy(
            QSizePolicy.Policy.Maximum, QSizePolicy.Policy.Fixed)  # Don't stretch button
        self.output_merge_label = QLabel("[Output file not selected]")
        self.output_merge_label.setObjectName(
            "OutputMergeLabel")  # For styling
        # Keep path on one line if possible
        self.output_merge_label.setWordWrap(False)
        self.output_merge_label.setAlignment(
            Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)
        output_merge_layout.addWidget(self.select_output_merge_button)
        # Label takes remaining space
        output_merge_layout.addWidget(self.output_merge_label, 1)
        merge_layout.addLayout(output_merge_layout)

        # --- Merge Action Buttons ---
        merge_actions_layout = QHBoxLayout()
        merge_actions_layout.addStretch()
        self.merge_button = QPushButton(" Merge ")  # Add spaces
        self.merge_button.setObjectName("MergeButton")
        self.merge_cancel_button = QPushButton("Cancel")
        self.merge_cancel_button.setObjectName("MergeCancelButton")
        self.merge_cancel_button.setEnabled(False)  # Disabled initially
        merge_actions_layout.addWidget(self.merge_button)
        merge_actions_layout.addWidget(self.merge_cancel_button)
        merge_actions_layout.addStretch()
        merge_layout.addLayout(merge_actions_layout)

        # --- Populate Split Tab ---
        split_layout = QVBoxLayout(self.split_tab)

        # --- Input File Selection ---
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

        # --- Output Directory Selection ---
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

        split_layout.addSpacerItem(QSpacerItem(
            20, 20, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Expanding))  # Push buttons down

        # --- Split Action Buttons ---
        split_actions_layout = QHBoxLayout()
        split_actions_layout.addStretch()
        self.split_button = QPushButton(" Split ")  # Add spaces
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
        # Wrap lines that are too long
        self.log_text.setLineWrapMode(QTextEdit.LineWrapMode.WidgetWidth)
        self.log_text.setFixedHeight(200)  # Increased log height
        shared_controls_layout.addWidget(self.log_text)

        self.progress_bar = QProgressBar()
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setValue(0)
        self.progress_bar.setFixedHeight(24)  # Increased progress bar height
        self.progress_bar.setAlignment(Qt.AlignmentFlag.AlignCenter)
        shared_controls_layout.addWidget(self.progress_bar)
        # Add shared controls layout to the main layout, below the tab widget
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

        # Update button states initially based on list/selection
        self._update_merge_button_state()

    def apply_dark_style(self):
        """Applies the dark mode stylesheet and Fusion style."""
        try:
            # Fusion style generally works well cross-platform and with stylesheets
            QApplication.setStyle(QStyleFactory.create('Fusion'))
        except Exception as e:
            # Use print for early logs
            print(f"Warning: Could not apply Fusion style: {e}")

        # Apply the custom dark stylesheet
        self.setStyleSheet(self.DARK_STYLESHEET)

        # Reload Icons AFTER style is set to ensure they match the theme
        try:
            style = self.style()  # Get the currently applied style
            self.folder_icon = style.standardIcon(
                QStyle.StandardPixmap.SP_DirIcon)
            self.file_icon = style.standardIcon(
                QStyle.StandardPixmap.SP_FileIcon)
            # Use more descriptive icons if available/desired
            merge_icon = style.standardIcon(
                QStyle.StandardPixmap.SP_DialogSaveButton)  # Or SP_MediaPlay
            # Or SP_MediaSeekForward, SP_DialogApplyButton
            split_icon = style.standardIcon(
                QStyle.StandardPixmap.SP_ArrowRight)
            cancel_icon = style.standardIcon(
                QStyle.StandardPixmap.SP_DialogCancelButton)  # Or SP_MediaStop
            remove_icon = style.standardIcon(
                QStyle.StandardPixmap.SP_TrashIcon)  # Or SP_DialogDiscardButton
            clear_icon = style.standardIcon(
                QStyle.StandardPixmap.SP_DialogResetButton)  # Or SP_EditClear

            # Set icons for buttons
            self.add_files_button.setIcon(self.file_icon)
            self.add_folder_button.setIcon(self.folder_icon)
            self.remove_item_button.setIcon(remove_icon)
            self.clear_list_button.setIcon(clear_icon)
            self.merge_button.setIcon(merge_icon)
            self.split_button.setIcon(split_icon)
            self.merge_cancel_button.setIcon(cancel_icon)
            self.split_cancel_button.setIcon(cancel_icon)
            # Buttons for selecting files/folders
            self.select_output_merge_button.setIcon(
                style.standardIcon(QStyle.StandardPixmap.SP_FileIcon))
            self.select_input_split_button.setIcon(
                style.standardIcon(QStyle.StandardPixmap.SP_FileIcon))
            self.select_output_split_button.setIcon(
                style.standardIcon(QStyle.StandardPixmap.SP_DirIcon))

        except Exception as e:
            # Log might not be ready yet, use print
            print(f"Warning: Could not load some standard icons: {e}")

        # Log after UI is likely initialized
        self.log("Applied dark theme stylesheet and icons.")

    def log(self, message):
        # Ensure log_text exists before appending
        if hasattr(self, 'log_text') and self.log_text:
            # Append message and ensure the latest message is visible
            self.log_text.append(message)
            self.log_text.verticalScrollBar().setValue(
                self.log_text.verticalScrollBar().maximum())  # Scroll to bottom
            # Process events to update UI immediately (use sparingly)
            QApplication.processEvents()
        else:
            # Fallback if log widget isn't ready (e.g., during init)
            print(f"LOG (pre-init): {message}")

    def update_progress(self, value):
        # Ensure value is between 0 and 100
        safe_value = max(0, min(value, 100))
        self.progress_bar.setValue(safe_value)
        # Update text format as well
        self.progress_bar.setFormat(f"%p% ({safe_value}%)")

    def operation_finished(self, success, message):
        self.log(f"Operation Finished: Success={success}, Message='{message}'")
        self.progress_bar.setValue(100)  # Ensure progress is 100 on finish
        self.progress_bar.setFormat("Finished")  # Set text to Finished

        # Show appropriate message box
        if success:
            QMessageBox.information(self, "Operation Complete", message)
        else:
            # Check if a critical error was already shown via operation_error
            if not hasattr(self, '_error_shown') or not self._error_shown:
                # Show warning if it finished "normally" but failed/cancelled,
                # unless a critical error dialog was already shown.
                if "cancel" in message.lower():  # Less intrusive message for cancellations
                    self.log(
                        "Operation was cancelled by user or during shutdown.")
                else:  # Show warning for other non-success finishes
                    QMessageBox.warning(self, "Operation Finished", message)

        self._reset_error_flag()  # Clear the error flag after handling
        self._set_ui_enabled(True)  # Re-enable UI components
        self._update_merge_button_state()  # Update button states based on list content

        # --- Clean up worker and thread ---
        # It's generally safer to disconnect signals *before* quitting/deleting,
        # though deleteLater should handle most cases.
        if self.worker:
            try:
                # Disconnect signals manually - belt and suspenders
                self.worker.signals.progress.disconnect(self.update_progress)
                self.worker.signals.log.disconnect(self.log)
                self.worker.signals.error.disconnect(self.operation_error)
                self.worker.signals.finished.disconnect(
                    self.operation_finished)
            except TypeError:
                pass  # Ignore if signals were already disconnected
            except Exception as e:
                self.log(f"Warning: Error disconnecting worker signals: {e}")

        if self.worker_thread:
            if self.worker_thread.isRunning():
                self.worker_thread.quit()  # Ask event loop to finish
                if not self.worker_thread.wait(1500):  # Wait up to 1.5 sec
                    self.log(
                        "Warning: Worker thread did not finish quitting gracefully. Forcing termination (risk of instability).")
                    self.worker_thread.terminate()  # Force terminate if quit failed (use cautiously)
                    self.worker_thread.wait(500)  # Wait a bit after terminate
            # Schedule deletion once event loop processes it
            self.worker_thread.deleteLater()

        # Worker should have been moved to thread, deleteLater will handle it eventually
        # If worker wasn't moved or thread failed, explicit deletion might be needed,
        # but deleteLater is generally preferred.
        if self.worker:
            self.worker.deleteLater()

        self.worker_thread = None
        self.worker = None
        self.log("Worker thread and object resources released.")

    def operation_error(self, error_message):
        """Slot specifically for critical errors reported by the worker."""
        self.log(f"CRITICAL ERROR received: {error_message}")
        QMessageBox.critical(self, "Critical Operation Error", error_message)
        self._error_shown = True  # Flag that a critical error message was displayed

    def _reset_error_flag(self):
        """Reset the flag that tracks if a critical error message was shown."""
        if hasattr(self, '_error_shown'):
            del self._error_shown

    def _set_ui_enabled(self, enabled):
        """Enable/disable UI elements during processing."""
        # Toggle enabled state of the *other* tab
        current_index = self.tab_widget.currentIndex()
        self.tab_widget.setTabEnabled(
            1 - current_index, enabled)  # Disable the *other* tab

        # --- Merge Tab Controls ---
        is_merge_tab_active = (self.tab_widget.widget(
            current_index) == self.merge_tab)
        self.add_files_button.setEnabled(enabled)
        self.add_folder_button.setEnabled(enabled)
        self.remove_item_button.setEnabled(enabled)
        self.clear_list_button.setEnabled(enabled)
        self.item_list_view.setEnabled(enabled)
        self.select_output_merge_button.setEnabled(enabled)
        # Enable Merge button only if UI is enabled AND conditions are met
        self.merge_button.setEnabled(enabled and self._can_start_merge())
        # Enable Cancel button ONLY when an operation is running on the merge tab
        self.merge_cancel_button.setEnabled(
            not enabled and is_merge_tab_active)

        # --- Split Tab Controls ---
        is_split_tab_active = (self.tab_widget.widget(
            current_index) == self.split_tab)
        self.select_input_split_button.setEnabled(enabled)
        self.select_output_split_button.setEnabled(enabled)
        # Enable Split button only if UI is enabled AND conditions are met
        self.split_button.setEnabled(enabled and self._can_start_split())
        # Enable Cancel button ONLY when an operation is running on the split tab
        self.split_cancel_button.setEnabled(
            not enabled and is_split_tab_active)

    def _update_merge_button_state(self):
        """Enable/disable the Merge button based on whether items and output file are selected."""
        can_merge = self._can_start_merge()
        self.merge_button.setEnabled(can_merge)
        # Also update remove/clear buttons based on list content
        has_items = len(self._items_to_merge_internal) > 0
        # Enable remove only if items exist AND something is selected in the view
        has_selection = len(self.item_list_view.selectedIndexes()) > 0
        self.remove_item_button.setEnabled(has_items and has_selection)
        self.clear_list_button.setEnabled(has_items)

    def _can_start_merge(self):
        """Check if conditions are met to start merging."""
        return bool(self._items_to_merge_internal and self.output_merge_file)

    def _update_split_button_state(self):
        """Enable/disable the Split button based on whether input and output are selected."""
        can_split = self._can_start_split()
        self.split_button.setEnabled(can_split)

    def _can_start_split(self):
        """Check if conditions are met to start splitting."""
        # Also check if input file actually exists
        input_exists = os.path.isfile(self.input_split_file)  # Quick check
        return bool(self.input_split_file and input_exists and self.output_split_dir)

    def add_files(self):
        """Adds selected files to the merge list and tree view."""
        start_dir = ""
        # Try to suggest a directory based on the last added item's base path
        if self._items_to_merge_internal:
            try:
                last_item_base = pathlib.Path(
                    # Base path is index 2
                    self._items_to_merge_internal[-1][2])
                if last_item_base.is_dir():
                    start_dir = str(last_item_base)
                elif last_item_base.parent.is_dir():  # If base was parent of a file
                    start_dir = str(last_item_base.parent)
            except Exception:
                pass  # Ignore errors determining start dir

        files, _ = QFileDialog.getOpenFileNames(
            # More generic filter
            self, "Select Files to Merge", start_dir, "All Files (*.*)")
        if files:
            added_count = 0
            root_node = self.item_model.invisibleRootItem()
            # Keep track of paths added in this operation for the view
            added_view_paths_this_op = set()

            for file_path_str in files:
                try:
                    file_path = pathlib.Path(file_path_str).resolve()
                    if not file_path.is_file():
                        self.log(
                            f"Warning: Selected item is not a file or does not exist: {file_path_str}")
                        continue

                    # Base path for a directly added file is its parent directory
                    base_path = file_path.parent.resolve()
                    item_data_tuple = ("file", str(file_path), str(base_path))

                    # Check if this exact resolved file path is already in the internal list
                    if str(file_path) not in [item[1] for item in self._items_to_merge_internal]:
                        self._items_to_merge_internal.append(item_data_tuple)

                        # Add to the Tree View only if not already visually present *at the top level*
                        # (Could be present under a folder - that's okay)
                        # Check if a top-level item with this path already exists
                        already_in_view_toplevel = False
                        for row in range(root_node.rowCount()):
                            toplevel_item = root_node.child(row, 0)
                            if toplevel_item and toplevel_item.data(PATH_DATA_ROLE) == str(file_path):
                                already_in_view_toplevel = True
                                break

                        if not already_in_view_toplevel and str(file_path) not in added_view_paths_this_op:
                            item = QStandardItem(
                                self.file_icon, file_path.name)
                            item.setToolTip(
                                f"File: {file_path}\nBase: {base_path}")
                            item.setData(
                                item_data_tuple[0], TYPE_DATA_ROLE)  # "file"
                            # Absolute path
                            item.setData(item_data_tuple[1], PATH_DATA_ROLE)
                            # Base path (parent)
                            item.setData(
                                item_data_tuple[2], BASE_PATH_DATA_ROLE)
                            item.setEditable(False)
                            root_node.appendRow(item)
                            added_view_paths_this_op.add(str(file_path))

                        added_count += 1
                    else:
                        self.log(
                            f"Skipping already added file: {file_path.name}")

                except OSError as e:
                    self.log(f"Error resolving path '{file_path_str}': {e}")
                except Exception as e:
                    self.log(
                        f"Unexpected error adding file '{file_path_str}': {e}")

            if added_count > 0:
                self.log(f"Added {added_count} file(s) to the merge list.")
                # self.item_list_view.expandAll() # Optional: expand after adding
                self._update_merge_button_state()
            elif files:  # Files were selected, but none were new
                self.log("Selected file(s) were already in the list.")

    def add_folder(self):
        """Opens the FolderSelectionDialog and adds selected items."""
        start_dir = ""
        if self._items_to_merge_internal:
            try:  # Try parent of last added item's base path
                last_item_base = pathlib.Path(
                    self._items_to_merge_internal[-1][2])
                if last_item_base.is_dir():
                    start_dir = str(last_item_base)
                elif last_item_base.parent.is_dir():
                    start_dir = str(last_item_base.parent)
            except Exception:
                pass

        folder_path_str = QFileDialog.getExistingDirectory(
            self, "Select Folder to Scan", start_dir)

        if folder_path_str:
            try:
                folder_path = pathlib.Path(folder_path_str).resolve()
                if not folder_path.is_dir():
                    QMessageBox.warning(
                        self, "Invalid Folder", f"The selected path is not a valid directory:\n{folder_path_str}")
                    return

                dialog = FolderSelectionDialog(str(folder_path), self)
                # dialog.setStyleSheet(self.DARK_STYLESHEET) # Inherit stylesheet automatically? Check. Seems to.
                if dialog.exec() == QDialog.DialogCode.Accepted:
                    # Get the list of ('type', 'abs_path', 'base_path') tuples from the dialog
                    # The dialog calculates these correctly based on user selection (checked items)
                    selected_items_for_worker = dialog.get_selected_items()

                    if not selected_items_for_worker:
                        self.log(
                            f"No items selected from folder: {folder_path}")
                        return

                    added_count_worker = 0
                    added_count_view = 0
                    items_already_present_paths = set(
                        item[1] for item in self._items_to_merge_internal)

                    # --- Add to the internal list for the worker ---
                    new_items_for_worker = []
                    for item_tuple in selected_items_for_worker:
                        # item_tuple is (type, abs_path, base_path)
                        item_abs_path = item_tuple[1]
                        if item_abs_path not in items_already_present_paths:
                            new_items_for_worker.append(item_tuple)
                            # Add here to prevent duplicates within this batch
                            items_already_present_paths.add(item_abs_path)
                            added_count_worker += 1

                    self._items_to_merge_internal.extend(new_items_for_worker)

                    # --- Add representation to the MAIN Tree View (item_list_view) ---
                    # We add a SINGLE entry representing the folder selection action.
                    # Displaying the whole selected hierarchy again in the main list can be overwhelming.
                    # We'll show the root folder added and maybe a count in the tooltip.
                    # The _items_to_merge_internal list holds the actual items.

                    root_node = self.item_model.invisibleRootItem()

                    # Check if this exact folder *root* was already added as a source
                    # Note: This doesn't prevent files *within* the folder from being added individually.
                    folder_root_path_str = str(folder_path)
                    existing_folder_root_item = None
                    for row in range(root_node.rowCount()):
                        item = root_node.child(row, 0)
                        # Check if it's a folder root AND the path matches
                        if item and item.data(TYPE_DATA_ROLE) == "folder-root" and item.data(PATH_DATA_ROLE) == folder_root_path_str:
                            existing_folder_root_item = item
                            break

                    if not existing_folder_root_item:
                        # Create a new top-level item representing this folder source
                        display_text = f"{folder_path.name} ({len(selected_items_for_worker)} selected)"
                        folder_item = QStandardItem(
                            self.folder_icon, display_text)
                        # Tooltip shows the root path and how many items were selected *from* it
                        folder_item.setToolTip(
                            f"Folder Source: {folder_root_path_str}\nSelected {len(selected_items_for_worker)} item(s) within.")
                        # Use a special type to identify this as a representation of a folder add operation
                        folder_item.setData("folder-root", TYPE_DATA_ROLE)
                        # Store the folder root path itself for identification/removal
                        folder_item.setData(
                            folder_root_path_str, PATH_DATA_ROLE)
                        # Store the base path used for items within this folder (dialog calculated this)
                        # Use the base path from the first selected item (should be consistent)
                        base_path_from_dialog = selected_items_for_worker[
                            0][2] if selected_items_for_worker else ""
                        folder_item.setData(
                            base_path_from_dialog, BASE_PATH_DATA_ROLE)
                        folder_item.setEditable(False)
                        root_node.appendRow(folder_item)
                        added_count_view += 1
                    else:
                        # Update existing folder root item's display text and tooltip
                        self.log(
                            f"Note: Selection from folder '{folder_path.name}' was already added. The internal merge list is updated, but the view item remains.")
                        display_text = f"{folder_path.name} ({len(selected_items_for_worker)} selected)"
                        existing_folder_root_item.setText(display_text)
                        existing_folder_root_item.setToolTip(
                            f"Folder Source: {folder_root_path_str}\nSelected {len(selected_items_for_worker)} item(s) within (updated).")
                        # Maybe update base path if it could change? Unlikely here.

                    # Log summary
                    if added_count_worker > 0:
                        log_msg = f"Added {added_count_worker} new item(s) from folder '{folder_path.name}' to merge list."
                        self.log(log_msg)
                    else:
                        self.log(
                            f"No *new* items selected from folder '{folder_path.name}' were added (already present).")

                    if added_count_view > 0:
                        self.log(
                            f"Added representation for folder '{folder_path.name}' to the view.")

                    self._update_merge_button_state()

                else:  # Dialog was cancelled
                    self.log(f"Folder selection cancelled for: {folder_path}")

            except OSError as e:
                self.log(
                    f"Error resolving folder path '{folder_path_str}': {e}")
                QMessageBox.critical(
                    self, "Error", f"Could not access or resolve the folder path:\n{folder_path_str}\n\nError: {e}")
            except Exception as e:
                self.log(
                    f"Unexpected error adding folder '{folder_path_str}': {e}\n{traceback.format_exc()}")
                QMessageBox.critical(
                    self, "Error", f"An unexpected error occurred while adding the folder:\n{e}")

    def remove_selected_items(self):
        """Removes selected items from the tree view AND the internal worker list."""
        selected_indexes = self.item_list_view.selectedIndexes()
        if not selected_indexes:
            self.log("No item(s) selected to remove.")
            self._update_merge_button_state()  # Update button states (Remove might disable)
            return

        # Use a set to avoid processing the same item multiple times if multi-column selection occurs
        items_to_remove_from_view = set()
        # Collect paths associated with the selected VIEW items
        view_item_paths_to_consider_removing = set()
        # Paths that definitely need removing from the worker list
        paths_to_remove_from_worker = set()

        # --- Pass 1: Identify items to remove from VIEW and related paths ---
        for index in selected_indexes:
            if not index.isValid() or index.column() != 0:  # Process only valid indexes in column 0
                continue
            item = self.item_model.itemFromIndex(index)
            if item and item not in items_to_remove_from_view:
                items_to_remove_from_view.add(item)
                item_path = item.data(PATH_DATA_ROLE)
                item_type = item.data(TYPE_DATA_ROLE)
                if item_path:
                    view_item_paths_to_consider_removing.add(item_path)
                    # If a "folder-root" is selected in the view, we need to find all items
                    # in the worker list that originated from this folder add operation.
                    # The most reliable way is to check if their base_path matches the
                    # folder-root's stored base_path *and* their path starts with the folder-root's path.
                    if item_type == "folder-root":
                        folder_root_path_str = item_path
                        folder_root_base_path = item.data(BASE_PATH_DATA_ROLE)
                        self.log(
                            f"Removing 'folder-root': {item.text()}. Finding associated items in worker list...")
                        # Iterate through the worker list to find matching items
                        for worker_tuple in self._items_to_merge_internal:
                            worker_type, worker_path, worker_base = worker_tuple
                            # Check base path match AND if worker path is inside the folder root path
                            if worker_base == folder_root_base_path and worker_path.startswith(folder_root_path_str):
                                paths_to_remove_from_worker.add(worker_path)
                    else:
                        # For regular files/folders directly selected, just mark their path
                        paths_to_remove_from_worker.add(item_path)

        # --- Pass 2: Remove items from the WORKER list ---
        initial_worker_count = len(self._items_to_merge_internal)
        if paths_to_remove_from_worker:
            self._items_to_merge_internal = [
                item_tuple for item_tuple in self._items_to_merge_internal
                # item_tuple[1] is the absolute path
                if item_tuple[1] not in paths_to_remove_from_worker
            ]
        removed_count_worker = initial_worker_count - \
            len(self._items_to_merge_internal)

        # --- Pass 3: Remove items from the VIEW model ---
        # Get list of rows to remove for each parent, process in reverse row order
        removal_map = {}  # parent_item -> list of rows
        for item in items_to_remove_from_view:
            parent = item.parent() or self.item_model.invisibleRootItem()  # Handle top-level items
            row = item.row()
            if parent not in removal_map:
                removal_map[parent] = []
            removal_map[parent].append(row)

        removed_count_display = 0
        self.item_model.blockSignals(True)
        try:
            for parent, rows in removal_map.items():
                # Sort rows descending to avoid index shifts during removal
                for row in sorted(rows, reverse=True):
                    if parent.removeRow(row):
                        removed_count_display += 1
                    else:
                        self.log(
                            f"Warning: Failed to remove row {row} from view model parent '{parent.text()}'.")
        finally:
            self.item_model.blockSignals(False)

        # --- Log Results ---
        if removed_count_display > 0 or removed_count_worker > 0:
            self.log(
                f"Removed {removed_count_display} item(s) from view and {removed_count_worker} item(s) from merge list.")
        else:
            self.log(
                "No corresponding items found to remove based on selection (or selection was invalid).")

        self._update_merge_button_state()  # Update button states

    def clear_item_list(self):
        """Clears both the view model and the internal worker list."""
        if not self._items_to_merge_internal and self.item_model.rowCount() == 0:
            self.log("List is already empty.")
            return

        reply = QMessageBox.question(self, "Confirm Clear",
                                     "Are you sure you want to remove all items from the merge list?",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                     QMessageBox.StandardButton.No)  # Default to No

        if reply == QMessageBox.StandardButton.Yes:
            self.item_model.clear()  # Clears the tree view model
            self._items_to_merge_internal.clear()  # Clears the list for the worker
            self.log("Cleared item list and merge data.")
            # Update buttons (should disable Merge/Remove/Clear)
            self._update_merge_button_state()
        else:
            self.log("Clear operation cancelled.")

    def select_output_merge_file(self):
        """Selects the output .txt file for merging."""
        start_dir = os.path.dirname(
            self.output_merge_file) if self.output_merge_file else QDir.currentPath()
        # Suggest .txt extension
        suggested_filename = os.path.join(start_dir, "merged_output.txt")
        file_path, file_filter = QFileDialog.getSaveFileName(
            self, "Save Merged File As", suggested_filename, "Text Files (*.txt);;All Files (*)")
        if file_path:
            # Ensure .txt extension if filter is Text Files and no extension provided
            p = pathlib.Path(file_path)
            if file_filter == "Text Files (*.txt)" and not p.suffix:
                file_path += ".txt"
                p = pathlib.Path(file_path)  # Update path object

            self.output_merge_file = str(p.resolve())  # Store resolved path
            # Display truncated path in label for brevity
            display_path = self._truncate_path_display(self.output_merge_file)
            self.output_merge_label.setText(display_path)
            self.output_merge_label.setToolTip(
                self.output_merge_file)  # Full path in tooltip
            self.log(f"Selected merge output file: {self.output_merge_file}")
            self._update_merge_button_state()  # Check if merge can be enabled

    def select_input_split_file(self):
        """Selects the input .txt file for splitting."""
        start_dir = os.path.dirname(
            self.input_split_file) if self.input_split_file else QDir.currentPath()
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select Merged File to Split", start_dir, "Text Files (*.txt);;All Files (*)")
        if file_path:
            p = pathlib.Path(file_path)
            self.input_split_file = str(p.resolve())  # Store resolved path
            display_path = self._truncate_path_display(self.input_split_file)
            self.input_split_label.setText(display_path)
            self.input_split_label.setToolTip(self.input_split_file)
            self.log(f"Selected split input file: {self.input_split_file}")
            self._update_split_button_state()  # Check if split can be enabled

    def select_output_split_dir(self):
        """Selects the output directory for split files."""
        start_dir = self.output_split_dir if self.output_split_dir else QDir.currentPath()
        dir_path = QFileDialog.getExistingDirectory(
            self, "Select Output Directory for Split Files", start_dir)
        if dir_path:
            p = pathlib.Path(dir_path)
            self.output_split_dir = str(p.resolve())  # Store resolved path
            display_path = self._truncate_path_display(self.output_split_dir)
            self.output_split_label.setText(display_path)
            self.output_split_label.setToolTip(self.output_split_dir)
            self.log(
                f"Selected split output directory: {self.output_split_dir}")
            self._update_split_button_state()  # Check if split can be enabled

    def _truncate_path_display(self, path_str, max_len=60):
        """Truncates a path string for display, adding ellipsis."""
        if len(path_str) <= max_len:
            return path_str
        else:
            # Try to keep the filename and some parent dirs
            parts = pathlib.Path(path_str).parts
            if len(parts) > 2:
                # Show ".../parent/filename"
                truncated = f"...{os.sep}{parts[-2]}{os.sep}{parts[-1]}"
                # If still too long, just show ".../filename"
                if len(truncated) > max_len:
                    truncated = f"...{os.sep}{parts[-1]}"
                # If filename itself is too long, truncate it
                if len(truncated) > max_len:
                    truncated = "..." + parts[-1][-(max_len-4):]
                return truncated
            elif len(parts) == 2:  # e.g., C:\file.txt or /home/file.txt
                # Show root and filename
                return f"{parts[0]}{os.sep}...{os.sep}{parts[-1]}"
            else:  # Just a filename or root? Should be rare.
                return path_str[:max_len-3] + "..."

    def _create_output_dir_if_needed(self, dir_path_str, operation_name):
        """Checks if a directory exists and is writable, prompts to create if not."""
        if not dir_path_str:
            self.log(
                f"Error: Output directory path is empty for {operation_name}.")
            return False  # Path is empty

        try:
            dir_path = pathlib.Path(dir_path_str)

            if not dir_path.exists():
                reply = QMessageBox.question(self, f"Create Directory for {operation_name}?",
                                             f"The output directory does not exist:\n{dir_path}\n\nCreate it?",
                                             QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                             QMessageBox.StandardButton.Yes)
                if reply == QMessageBox.StandardButton.Yes:
                    try:
                        dir_path.mkdir(parents=True, exist_ok=True)
                        self.log(f"Created output directory: {dir_path}")
                        # Check if writable after creation
                        if not os.access(str(dir_path), os.W_OK):
                            raise OSError(
                                f"Directory created but not writable: {dir_path}")
                        return True
                    except OSError as e:
                        QMessageBox.critical(
                            self, f"{operation_name} Error", f"Could not create or write to directory:\n{dir_path}\n\nError: {e}")
                        self.log(
                            f"Error: Failed to create/write directory '{dir_path}': {e}")
                        return False
                else:
                    self.log(
                        f"{operation_name} cancelled by user (directory not created).")
                    return False
            elif not dir_path.is_dir():
                QMessageBox.critical(
                    self, f"{operation_name} Error", f"The selected output path exists but is not a directory:\n{dir_path}")
                self.log(f"Error: Output path is not a directory: {dir_path}")
                return False
            # Check if existing dir is writable
            elif not os.access(str(dir_path), os.W_OK):
                QMessageBox.critical(
                    self, f"{operation_name} Error", f"The selected output directory is not writable:\n{dir_path}")
                self.log(f"Error: Output directory not writable: {dir_path}")
                return False
            else:
                # Directory exists, is a directory, and is writable
                return True

        except Exception as e:  # Catch potential errors with Path object itself
            QMessageBox.critical(
                self, f"{operation_name} Error", f"Invalid output directory path specified:\n{dir_path_str}\n\nError: {e}")
            self.log(f"Error: Invalid output path '{dir_path_str}': {e}")
            return False

    def start_merge(self):
        """Starts the merge operation in a background thread."""
        if not self._can_start_merge():
            QMessageBox.warning(
                self, "Merge Error", "Please select items to merge AND specify an output file first.")
            self.log(
                "Merge aborted: Conditions not met (no items or no output file).")
            return

        # Check/create the output file's *directory*
        output_dir = os.path.dirname(self.output_merge_file)
        if not self._create_output_dir_if_needed(output_dir, "Merge"):
            self.log("Merge aborted: Output directory check/creation failed.")
            return

        # Check if another operation is already running
        if self.worker_thread and self.worker_thread.isRunning():
            QMessageBox.warning(
                self, "Busy", "Another operation (Merge or Split) is already in progress. Please wait.")
            self.log("Merge aborted: Another worker thread is active.")
            return

        # --- Prepare for Worker ---
        self.log_text.clear()
        self.progress_bar.setValue(0)
        self.progress_bar.setFormat("Starting Merge...")
        self._set_ui_enabled(False)  # Disable UI, enable Cancel button
        self._reset_error_flag()    # Reset critical error display flag

        # Pass a *copy* of the internal list to the worker to avoid modification issues
        worker_data = list(self._items_to_merge_internal)
        self.log(f"Starting merge with {len(worker_data)} items/sources...")

        # --- Create and Start Thread ---
        self.worker_thread = QThread(self)
        self.worker = MergerWorker(worker_data, self.output_merge_file)
        self.worker.moveToThread(self.worker_thread)

        # Connect signals from worker to slots in GUI thread
        self.worker.signals.progress.connect(self.update_progress)
        self.worker.signals.log.connect(self.log)
        self.worker.signals.error.connect(
            self.operation_error)  # For critical errors
        self.worker.signals.finished.connect(
            self.operation_finished)  # For completion status

        # Clean up worker and thread object when the thread's event loop finishes
        # Use deleteLater to ensure cleanup happens safely in the main event loop
        self.worker_thread.finished.connect(self.worker.deleteLater)
        self.worker_thread.finished.connect(self.worker_thread.deleteLater)

        # Connect the thread's started signal to the worker's run method
        self.worker_thread.started.connect(self.worker.run)

        # Start the thread's event loop (which then calls run via started signal)
        self.worker_thread.start()
        self.log("Merge worker thread started.")

    def start_split(self):
        """Starts the split operation in a background thread."""
        if not self._can_start_split():
            # Check specific reasons for failure
            if not self.input_split_file:
                msg = "No input file selected."
            elif not os.path.isfile(self.input_split_file):
                msg = f"Input file does not exist or is not a file:\n{self.input_split_file}"
            elif not self.output_split_dir:
                msg = "No output directory selected."
            else:
                msg = "Cannot start split (unknown reason)."  # Fallback
            QMessageBox.warning(self, "Split Error",
                                f"Cannot start split.\n{msg}")
            self.log(f"Split aborted: Conditions not met. Reason: {msg}")
            return

        # Check/create the output directory
        if not self._create_output_dir_if_needed(self.output_split_dir, "Split"):
            self.log("Split aborted: Output directory check/creation failed.")
            return

        # Check if another operation is already running
        if self.worker_thread and self.worker_thread.isRunning():
            QMessageBox.warning(
                self, "Busy", "Another operation (Merge or Split) is already in progress. Please wait.")
            self.log("Split aborted: Another worker thread is active.")
            return

        # --- Prepare for Worker ---
        self.log_text.clear()
        self.progress_bar.setValue(0)
        self.progress_bar.setFormat("Starting Split...")
        self._set_ui_enabled(False)  # Disable UI, enable Cancel button
        self._reset_error_flag()

        self.log(
            f"Starting split for '{self.input_split_file}' -> '{self.output_split_dir}'")

        # --- Create and Start Thread ---
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

        # Connect started signal to worker's run method
        self.worker_thread.started.connect(self.worker.run)

        self.worker_thread.start()
        self.log("Split worker thread started.")

    def cancel_operation(self):
        """Signals the running worker thread to stop."""
        if self.worker and self.worker_thread and self.worker_thread.isRunning():
            self.log("Attempting to cancel running operation...")
            try:
                # Call the worker's stop method (which sets the is_running flag to False)
                self.worker.stop()
            except Exception as e:
                # Log potential error during the stop call itself (less likely)
                self.log(f"Error trying to signal worker to stop: {e}")

            # Disable cancel buttons immediately to prevent multiple clicks
            self.merge_cancel_button.setEnabled(False)
            self.split_cancel_button.setEnabled(False)
            self.progress_bar.setFormat("Cancelling...")
            # The worker should detect the flag change in its next loop iteration
            # and then emit the finished(False, "Cancelled") signal,
            # which will trigger operation_finished to re-enable the UI and clean up.
        else:
            self.log("No operation is currently running to cancel.")

    def closeEvent(self, event):
        """Ensure worker thread is stopped cleanly on application close."""
        if self.worker_thread and self.worker_thread.isRunning():
            self.log(
                "Close Event: Application is closing - Attempting to stop running operation...")
            self.cancel_operation()  # Signal the worker to stop

            # Give the thread a chance to finish cleanly after being signalled
            # Wait slightly longer here as it's during shutdown.
            if not self.worker_thread.wait(2500):  # Wait up to 2.5 seconds
                self.log(
                    "Warning: Worker thread did not terminate gracefully during close event after stop signal. Forcing termination.")
                # Force terminate if it didn't stop (can be risky, might leave resources open or corrupt data)
                self.worker_thread.terminate()
                self.worker_thread.wait(500)  # Brief wait after terminate
            else:
                self.log("Worker thread stopped successfully during close event.")

        event.accept()  # Allow the window to close


# --- Main Execution ---
if __name__ == '__main__':
    # --- High DPI Settings (Place before QApplication initialization) ---
    # Try environment variables first (often effective on Windows/some Linux)
    os.environ["QT_ENABLE_HIGHDPI_SCALING"] = "1"
    # Avoids issues with fractional scaling
    os.environ["QT_SCALE_FACTOR_ROUNDING_POLICY"] = "PassThrough"

    # Alternatively, or additionally, use application attributes (might be needed on macOS/older Qt)
    # These need to be set on the class *before* creating the QApplication instance.
    # Note: In PyQt6, these might be less necessary due to improved defaults and env var handling.
    # QApplication.setAttribute(Qt.ApplicationAttribute.AA_EnableHighDpiScaling, True)
    # QApplication.setAttribute(Qt.ApplicationAttribute.AA_UseHighDpiPixmaps, True)

    app = QApplication(sys.argv)

    # Set Application Info (Optional but good practice for integrations)
    app.setApplicationName("FileMergerSplitter")
    app.setOrganizationName("UtilityApps")  # Replace if desired
    app.setApplicationVersion("1.1")

    # --- Create and Show Main Window ---
    ex = MergerSplitterApp()
    # No need to set object name here unless specifically targeting root widget in CSS,
    # which we did with MergerSplitterAppWindow in the stylesheet.
    ex.show()

    # --- Start Event Loop ---
    sys.exit(app.exec())
