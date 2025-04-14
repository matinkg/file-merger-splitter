import sys
import os
import re
import pathlib
import traceback  # Added for detailed error logging
from PyQt6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
    QFileDialog, QListWidget, QListWidgetItem, QLabel, QTextEdit,
    QMessageBox, QProgressBar, QSizePolicy, QStyleFactory, QStyle,
    QDialog, QTreeView, QDialogButtonBox, QScrollArea, QTabWidget, QSpacerItem,
    QComboBox  # Added ComboBox
)
from PyQt6.QtGui import QStandardItemModel, QStandardItem, QIcon, QPalette, QColor
from PyQt6.QtCore import (
    Qt, QThread, pyqtSignal, QObject, QDir, QModelIndex, QAbstractItemModel
)

# --- Constants ---
# REMOVED old constants START_DELIMITER_FORMAT, END_DELIMITER_FORMAT, START_DELIMITER_REGEX

# --- Merge Format Definitions ---
# Structure: Key = User-friendly name
# Value = Dictionary with format details
MERGE_FORMATS = {
    "Default": {
        "name": "Default",
        # --- Merger settings ---
        "start": "--- START FILE: {filepath} ---",
        "end": "--- END FILE: {filepath} ---",
        "file_separator": "\n\n",  # Separator between file blocks
        "content_prefix": "",     # Text right before file content (e.g., ```)
        # Text right after file content (before end delimiter)
        "content_suffix": "",
        # --- Splitter settings ---
        # Regex pattern string to find the start delimiter and capture filepath
        "start_regex_pattern": r"^--- START FILE: (.*?) ---$",
        # Function to generate the exact end delimiter string for a given filepath
        "get_end_delimiter": lambda fp: f"--- END FILE: {fp} ---",
        # Does the start regex line itself contain content to skip? (Usually False)
        "skip_start_line_in_content": False,
        # Does the line immediately *after* the start regex line need skipping? (e.g., ``` for Markdown)
        "skip_line_after_start": False,
    },
    "Markdown": {
        "name": "Markdown",
        # --- Merger settings ---
        "start": "File: `{filepath}`",  # Header line
        "end": "```",                  # End fence (constant)
        "file_separator": "\n\n",
        "content_prefix": "```\n",     # Start fence + newline
        "content_suffix": "",
        # --- Splitter settings ---
        # Capture path from header line
        "start_regex_pattern": r"^File: `(.*?)`$",
        "get_end_delimiter": lambda fp: "```",     # End delimiter is constant
        # The "File: ..." line isn't part of content
        "skip_start_line_in_content": False,
        # Skip the "```" line that follows "File: ..."
        "skip_line_after_start": True,
    },
    "Markdown_Fenced": {
        "name": "Markdown (Fenced)",
        # --- Merger settings ---
        "start": "```{filepath}",  # Use filepath as info string
        "end": "```",
        "file_separator": "\n\n",
        "content_prefix": "",      # Content starts immediately after start line
        "content_suffix": "",
        # --- Splitter settings ---
        # Regex: Match ``` followed by the filepath, capture filepath
        # Needs to be careful not to match the closing ```
        # Let's assume filepath won't contain ``` itself.
        # Match ```, capture rest of line (info string)
        "start_regex_pattern": r"^```(?!``)(.*)$",
        # (?!``) is a negative lookahead to avoid matching closing fence if path is empty
        # but this isn't perfect if path could validly be ` ``` `.
        # A simpler approach might be needed if paths are complex.
        # Let's assume simple file paths for now.
        "get_end_delimiter": lambda fp: "```",
        "skip_start_line_in_content": False,  # The fence line isn't content
        "skip_line_after_start": False,    # Content starts on the next line
    },
    # Add more formats here if needed
}


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
    ''' Performs the file merging in a separate thread using a specified format. '''
    signals = WorkerSignals()

    def __init__(self, items_to_merge, output_file, merge_format_details):
        super().__init__()
        self.items_to_merge = items_to_merge
        self.output_file = output_file
        self.format_details = merge_format_details  # Store the format dictionary
        self.is_running = True
        self.log(
            f"Worker received {len(items_to_merge)} initial selection items.")
        self.log(
            f"Using merge format: {self.format_details.get('name', 'Unknown')}")

    def stop(self):
        self.log("Stop signal received by MergerWorker.")
        self.is_running = False

    def log(self, msg):
        self.signals.log.emit(msg)

    def run(self):
        self.log(
            f"Starting merge process -> {self.output_file} (Format: {self.format_details['name']})")
        files_to_process = []
        total_size = 0
        processed_size = 0
        processed_files_count = 0
        encountered_resolved_paths = set()

        # Retrieve format specifics
        start_fmt = self.format_details.get("start", "{filepath}")
        end_fmt = self.format_details.get("end", "")
        separator = self.format_details.get("file_separator", "\n")
        content_prefix = self.format_details.get("content_prefix", "")
        content_suffix = self.format_details.get("content_suffix", "")

        try:
            # --- Phase 1: Discover all files (remains largely the same) ---
            self.log("Scanning files and folders based on input selections...")
            initial_item_count = len(self.items_to_merge)
            files_discovered_in_scan = []

            for item_idx, (item_type, item_path_str, base_path_str) in enumerate(self.items_to_merge):
                if not self.is_running:
                    break
                # ... (rest of file discovery logic is identical to original) ...
                # --- Start of existing file discovery loop ---
                self.log(
                    f"Processing selection {item_idx+1}/{initial_item_count}: Type='{item_type}', Path='{item_path_str}', Base='{base_path_str}'")
                try:
                    item_path = pathlib.Path(item_path_str).resolve()
                    base_path = pathlib.Path(base_path_str).resolve(
                    ) if base_path_str else item_path.parent
                except OSError as e:
                    self.log(
                        f"Warning: Could not resolve path '{item_path_str}' or base '{base_path_str}': {e}. Skipping item.")
                    continue
                except Exception as e:
                    self.log(
                        f"Warning: Error processing path '{item_path_str}': {e}. Skipping item.")
                    continue

                if item_type == "file":
                    if item_path.is_file():
                        if item_path not in encountered_resolved_paths:
                            try:
                                relative_path = item_path.relative_to(
                                    base_path)
                                fsize = item_path.stat().st_size
                                files_discovered_in_scan.append(
                                    (item_path, relative_path, fsize))
                                total_size += fsize
                                encountered_resolved_paths.add(item_path)
                            except ValueError:
                                self.log(
                                    f"Warning: Could not determine relative path for '{item_path}' against base '{base_path}'. Using filename as relative path.")
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
                                files_discovered_in_scan.append((item_path, item_path.relative_to(
                                    base_path), 0))  # Try original relative path logic
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
                        for root, _, filenames in os.walk(str(item_path), followlinks=False):
                            if not self.is_running:
                                break
                            root_path = pathlib.Path(root)
                            for filename in filenames:
                                if not self.is_running:
                                    break
                                try:
                                    file_path = (
                                        root_path / filename).resolve()
                                    if file_path not in encountered_resolved_paths:
                                        try:
                                            relative_path = file_path.relative_to(
                                                base_path)
                                            fsize = file_path.stat().st_size
                                            files_discovered_in_scan.append(
                                                (file_path, relative_path, fsize))
                                            total_size += fsize
                                            encountered_resolved_paths.add(
                                                file_path)
                                        except ValueError:
                                            self.log(
                                                f"Warning: Could not make '{file_path}' relative to base '{base_path}'. Using path relative to scanned folder '{item_path}'.")
                                            try:
                                                relative_path_fallback = file_path.relative_to(
                                                    item_path)
                                                fsize = file_path.stat().st_size
                                                files_discovered_in_scan.append(
                                                    (file_path, relative_path_fallback, fsize))
                                                total_size += fsize
                                                encountered_resolved_paths.add(
                                                    file_path)
                                            except ValueError:
                                                self.log(
                                                    f"Error: Could not even make '{file_path}' relative to its walk root '{item_path}'. Using filename only.")
                                                relative_path_final = pathlib.Path(
                                                    file_path.name)
                                                fsize = file_path.stat().st_size
                                                files_discovered_in_scan.append(
                                                    (file_path, relative_path_final, fsize))
                                                total_size += fsize
                                                encountered_resolved_paths.add(
                                                    file_path)
                                            except OSError as e_size:
                                                self.log(
                                                    f"Warning: Could not get size for {file_path}: {e_size}. Using size 0.")
                                                files_discovered_in_scan.append(
                                                    (file_path, file_path.relative_to(item_path), 0))
                                                encountered_resolved_paths.add(
                                                    file_path)
                                        except OSError as e:
                                            self.log(
                                                f"Warning: Could not get size for {file_path}: {e}. Using size 0.")
                                            files_discovered_in_scan.append(
                                                (file_path, file_path.relative_to(base_path), 0))
                                            encountered_resolved_paths.add(
                                                file_path)
                                except OSError as e_resolve:
                                    self.log(
                                        f"Warning: Could not resolve or access path under {root_path} for filename '{filename}': {e_resolve}")
                                except Exception as e:
                                    self.log(
                                        f"Warning: Could not process file '{filename}' in folder scan under {root_path}: {e}")
                            if not self.is_running:
                                break
                        if not self.is_running:
                            break
                    else:
                        self.log(
                            f"Warning: Selected folder not found during scan: {item_path}")
                # --- End of existing file discovery loop ---

            if not self.is_running:
                self.log("Merge cancelled during scanning phase.")
                self.signals.finished.emit(
                    False, "Merge cancelled during scan.")
                return

            files_to_process = sorted(
                files_discovered_in_scan, key=lambda x: x[1].as_posix())

            if not files_to_process:
                self.log("No valid, unique files found to merge after scanning.")
                self.signals.finished.emit(False, "No files to merge.")
                return

            self.log(
                f"Found {len(files_to_process)} unique files to merge. Total size: {total_size} bytes.")
            self.signals.progress.emit(0)

            # --- Phase 2: Write the files using the selected format ---
            output_file_path = pathlib.Path(self.output_file)
            try:
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

                    relative_path_str = relative_path.as_posix()
                    self.log(
                        f"Merging ({i+1}/{total_files_count}): '{relative_path_str}' (from: {absolute_path})")

                    # --- Format Application ---
                    start_delimiter = start_fmt.format(
                        filepath=relative_path_str)
                    # End delimiter might need filepath too
                    end_delimiter = end_fmt.format(filepath=relative_path_str)

                    # Write start delimiter
                    outfile.write(start_delimiter + "\n")

                    # Write content prefix (if any)
                    if content_prefix:
                        # Assume prefix includes \n if needed
                        outfile.write(content_prefix)

                    # Write file content
                    file_content = ""
                    try:
                        try:
                            with open(absolute_path, "r", encoding="utf-8") as infile:
                                file_content = infile.read()
                        except UnicodeDecodeError:
                            self.log(
                                f"Warning: Non-UTF-8 file detected: '{relative_path_str}'. Reading with 'latin-1'.")
                            with open(absolute_path, "r", encoding="latin-1") as infile:
                                file_content = infile.read()
                        except Exception as e_read:
                            self.log(
                                f"Error reading file '{absolute_path}': {e_read}. Inserting error message.")
                            file_content = f"Error reading file: {e_read}"

                        outfile.write(file_content)

                        # Ensure newline before suffix/end delimiter if content existed and didn't end with one
                        if file_content and not file_content.endswith('\n'):
                            if content_suffix or end_delimiter:  # Only add newline if something follows
                                outfile.write("\n")

                    except Exception as e_outer:
                        self.log(
                            f"Critical error processing file content for {absolute_path}: {e_outer}\n{traceback.format_exc()}")
                        outfile.write(
                            f"\nError processing file content: {e_outer}\n")

                    # Write content suffix (if any)
                    if content_suffix:
                        # Assume suffix handles its own newlines
                        outfile.write(content_suffix)

                    # Write end delimiter
                    # Add newline after end delimiter
                    outfile.write(end_delimiter + "\n")

                    # Write file separator (if not the last file)
                    if i < total_files_count - 1:
                        outfile.write(separator)
                    # --- End Format Application ---

                    processed_size += fsize
                    processed_files_count += 1
                    if total_size > 0:
                        progress_percent = int(
                            (processed_size / total_size) * 100)
                        self.signals.progress.emit(min(progress_percent, 100))
                    elif total_files_count > 0:
                        self.signals.progress.emit(
                            min(int((processed_files_count / total_files_count) * 100), 100))

            # --- Finalization (remains the same) ---
            if not self.is_running:
                self.log("Merge cancelled during writing phase.")
                try:
                    if output_file_path.exists():
                        output_file_path.unlink()
                    self.log(f"Removed incomplete file: {output_file_path}")
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
            if self.is_running:
                self.signals.progress.emit(100)


# --- Splitter Worker ---
class SplitterWorker(QObject):
    ''' Performs the file splitting in a separate thread based on a specified format. '''
    signals = WorkerSignals()

    def __init__(self, merged_file, output_dir, split_format_details):
        super().__init__()
        self.merged_file = merged_file
        self.output_dir = pathlib.Path(output_dir)
        self.format_details = split_format_details  # Store the format dictionary
        self.is_running = True
        self.log(f"Splitter received file: {merged_file}")
        self.log(
            f"Using split format: {self.format_details.get('name', 'Unknown')}")

    def stop(self):
        self.log("Stop signal received by SplitterWorker.")
        self.is_running = False

    def log(self, msg):
        self.signals.log.emit(msg)

    def run(self):
        self.log(f"Starting split process for: {self.merged_file}")
        self.log(f"Output directory: {self.output_dir}")
        self.log(f"Expecting format: {self.format_details['name']}")
        self.signals.progress.emit(0)

        try:
            # --- Format Specific Settings ---
            try:
                start_regex_pattern = self.format_details["start_regex_pattern"]
                start_regex = re.compile(start_regex_pattern)
                get_end_delimiter_func = self.format_details["get_end_delimiter"]
                # Flag to skip the line *after* the one matched by start_regex
                skip_line_after_start = self.format_details.get(
                    "skip_line_after_start", False)
            except KeyError as e:
                raise ValueError(
                    f"Split format '{self.format_details.get('name')}' is missing required key: {e}")
            except re.error as e_re:
                raise ValueError(
                    f"Invalid start_regex_pattern in format '{self.format_details.get('name')}': {e_re}")

            merged_file_path = pathlib.Path(self.merged_file)
            if not merged_file_path.is_file():
                raise FileNotFoundError(
                    f"Input file not found: {self.merged_file}")

            total_size = merged_file_path.stat().st_size
            processed_size = 0
            file_count = 0
            created_file_paths = set()

            with open(merged_file_path, "r", encoding="utf-8", errors='replace') as infile:
                current_file_path_relative = None
                current_file_content = []
                in_file_block = False
                # Flag to indicate we just found the start line and might need to skip the next line (e.g., ```)
                just_started_block = False

                for line_num, line in enumerate(infile):
                    if not self.is_running:
                        break

                    processed_size += len(line.encode('utf-8',
                                          errors='replace'))
                    if total_size > 0:
                        progress_percent = int(
                            (processed_size / total_size) * 100)
                        self.signals.progress.emit(min(progress_percent, 100))

                    line_stripped = line.strip()

                    # --- State Machine for Parsing ---
                    if not in_file_block:
                        # Look for a start delimiter
                        start_match = start_regex.match(line_stripped)
                        if start_match:
                            # Found a potential start delimiter line
                            try:
                                potential_relative_path = start_match.group(
                                    # Get captured path, strip whitespace
                                    1).strip()
                            except IndexError:
                                self.log(
                                    f"Warning: Regex '{start_regex_pattern}' matched line {line_num+1} but captured no path group. Skipping block.")
                                continue  # Skip this potential block

                            # --- Basic Path Safety Check ---
                            # Normalize separators for checks
                            normalized_path_check = potential_relative_path.replace(
                                "\\", "/")
                            is_safe = True
                            if not potential_relative_path:  # Empty path captured
                                self.log(
                                    f"Warning: Empty filepath captured by start regex on line {line_num+1}. Skipping block.")
                                is_safe = False
                            elif pathlib.Path(potential_relative_path).is_absolute():
                                self.log(
                                    f"Error: Security risk! Absolute path found in delimiter: '{potential_relative_path}' near line {line_num+1}. Skipping block.")
                                is_safe = False
                            elif normalized_path_check.startswith("../") or "/../" in normalized_path_check:
                                self.log(
                                    f"Warning: Potential path traversal detected in delimiter: '{potential_relative_path}' near line {line_num+1}. Final check during write.")
                                # Let _write_file handle final check

                            if not is_safe:
                                continue  # Move to next line, skip block

                            # Start the new block
                            current_file_path_relative = potential_relative_path
                            current_file_content = []
                            in_file_block = True
                            # Set flag if the format requires skipping the line *after* this start line
                            just_started_block = skip_line_after_start
                            self.log(
                                f"Found block start: '{current_file_path_relative}' (Line {line_num+1})")
                            # Move to next line (don't include start delimiter line in content)
                            continue

                    else:  # We are inside a file block (in_file_block is True)
                        # Check if we need to skip this line because it immediately follows the start line
                        if just_started_block:
                            just_started_block = False  # Reset flag
                            # self.log(f"  Skipping line {line_num+1} as per format '{self.format_details['name']}'")
                            # Skip this line (e.g., the ``` after File: `...`)
                            continue

                        # Check for the end delimiter for the *current* block
                        # Use the function from the format details
                        expected_end_delimiter = get_end_delimiter_func(
                            current_file_path_relative)

                        if line_stripped == expected_end_delimiter:
                            # Found the end delimiter, write the file
                            self.log(
                                f"Found block end for: '{current_file_path_relative}' (Line {line_num+1})")
                            if self._write_file(current_file_path_relative, "".join(current_file_content)):
                                file_count += 1
                                created_file_paths.add(
                                    self.output_dir.joinpath(current_file_path_relative))
                            # Reset state for the next potential block
                            in_file_block = False
                            current_file_path_relative = None
                            current_file_content = []
                            just_started_block = False  # Ensure reset
                            continue  # Don't include end delimiter line in content
                        else:
                            # Not the end delimiter, append the line (with original newline) to the current file's content
                            current_file_content.append(line)

                # --- Loop finished ---
                if not self.is_running:
                    self.log("Split cancelled during file processing.")
                    self.signals.finished.emit(False, "Split cancelled.")
                    # Optionally remove partially created files here if desired
                    return

                # Check if the file ended while still inside a block (missing end delimiter)
                if in_file_block and current_file_path_relative:
                    self.log(
                        f"Warning: Merged file ended before finding END delimiter for '{current_file_path_relative}'. Saving remaining content.")
                    if self._write_file(current_file_path_relative, "".join(current_file_content)):
                        file_count += 1
                        created_file_paths.add(
                            self.output_dir.joinpath(current_file_path_relative))

            # --- Post-processing (remains the same) ---
            self.signals.progress.emit(100)
            if file_count > 0:
                final_message = f"Split successful! {file_count} files created in '{self.output_dir.name}' (Format: {self.format_details['name']})."
                self.log(final_message)
                self.signals.finished.emit(True, final_message)
            elif not self.is_running:
                pass  # Finished signal already emitted
            else:
                final_message = f"Split finished, but no valid file blocks matching format '{self.format_details['name']}' were found or extracted."
                self.log(final_message)
                self.signals.finished.emit(False, final_message)

        # --- Exception Handling (remains mostly the same) ---
        except FileNotFoundError as e:
            error_msg = f"Input merged file not found: {self.merged_file}"
            self.log(f"Error: {error_msg}")
            self.signals.error.emit(error_msg)
            self.signals.finished.emit(False, f"Split failed: {error_msg}")
        except ValueError as e:  # Catch format definition errors
            error_msg = f"Format definition error: {e}"
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
            if self.is_running:
                self.signals.progress.emit(100)

    def _write_file(self, relative_path_str, content):
        """Helper to write content to the appropriate file within the output directory.
           Includes safety checks. Returns True on success, False on failure."""
        # --- This method remains unchanged from the original, it handles path safety ---
        if not relative_path_str:
            self.log(
                "Error: Attempted to write file with empty relative path. Skipping.")
            return False
        try:
            target_path = self.output_dir / pathlib.Path(relative_path_str)
            try:
                output_dir_resolved = self.output_dir.resolve(strict=True)
                target_path_resolved = target_path.resolve(strict=False)
            except (OSError, ValueError) as e_resolve:
                self.log(
                    f"Error: Invalid path generated for '{relative_path_str}': {e_resolve}. Skipping write.")
                return False
            except FileNotFoundError:
                self.log(
                    f"Error: Output directory '{self.output_dir}' seems to have disappeared. Cannot write '{relative_path_str}'.")
                return False

            is_within_output_dir = (output_dir_resolved == target_path_resolved or
                                    output_dir_resolved in target_path_resolved.parents)

            if not is_within_output_dir:
                self.log(f"Error: Security risk! Path '{relative_path_str}' resolved to '{target_path_resolved}', "
                         f"which is outside the designated output directory '{output_dir_resolved}'. Skipping write.")
                return False

            self.log(f"Attempting to create file: {target_path_resolved}")
            target_path_resolved.parent.mkdir(parents=True, exist_ok=True)

            with open(target_path_resolved, "w", encoding="utf-8") as outfile:
                outfile.write(content)
            # self.log(f"Successfully wrote: {target_path_resolved}")
            return True

        except OSError as e:
            self.log(
                f"Error writing file '{target_path_resolved}' (OS Error): {e}")
            return False
        except Exception as e:
            log_path = target_path_resolved if 'target_path_resolved' in locals() else 'N/A'
            self.log(
                f"Error writing file for relative path '{relative_path_str}' (Resolved: {log_path}) (General Error): {e}\n{traceback.format_exc()}")
            return False


# --- Folder Selection Dialog (remains unchanged) ---
class FolderSelectionDialog(QDialog):
    """A dialog to select specific files and subfolders within a chosen folder using a tree view with tristate checkboxes."""
    # ... (This class code is identical to the original provided code) ...

    def __init__(self, folder_path_str, parent=None):
        super().__init__(parent)
        self.folder_path = pathlib.Path(folder_path_str)
        self._selected_items_for_worker = []
        self.setWindowTitle(f"Select items in: {self.folder_path.name}")
        self.setMinimumSize(500, 500)
        self.setSizeGripEnabled(True)
        layout = QVBoxLayout(self)
        self.tree_view = QTreeView()
        self.tree_view.setHeaderHidden(True)
        self.model = QStandardItemModel()
        self.tree_view.setModel(self.model)
        layout.addWidget(
            QLabel(f"Select items to include from:\n<b>{self.folder_path}</b>"))
        layout.addWidget(self.tree_view, 1)
        style = self.style()
        self.folder_icon = style.standardIcon(QStyle.StandardPixmap.SP_DirIcon)
        self.file_icon = style.standardIcon(QStyle.StandardPixmap.SP_FileIcon)
        self.error_icon = style.standardIcon(
            QStyle.StandardPixmap.SP_MessageBoxWarning)
        self.model.blockSignals(True)
        self.populate_tree()
        self.model.blockSignals(False)
        self.tree_view.expandToDepth(0)
        self.model.itemChanged.connect(self.on_item_changed)
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

    def _populate_recursive(self, parent_node: QStandardItem, current_path: pathlib.Path):
        try:
            items_in_dir = sorted(list(current_path.iterdir()), key=lambda p: (
                not p.is_dir(), p.name.lower()))
        except OSError as e:
            error_text = f"Error reading: {e.strerror} ({current_path.name})"
            error_item = QStandardItem(self.error_icon, error_text)
            error_item.setFlags(Qt.ItemFlag.ItemIsEnabled)
            error_item.setToolTip(
                f"Could not access directory:\n{current_path}\n{e}")
            parent_node.appendRow(error_item)
            print(f"OS Error reading {current_path}: {e}")
            return
        for item_path in items_in_dir:
            item = QStandardItem(item_path.name)
            item.setEditable(False)
            item.setCheckable(True)
            item.setCheckState(Qt.CheckState.Checked)
            item.setData(str(item_path.resolve()), PATH_DATA_ROLE)
            if item_path.is_dir():
                item.setIcon(self.folder_icon)
                item.setData("folder", TYPE_DATA_ROLE)
                item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserTristate)
                parent_node.appendRow(item)
                self._populate_recursive(item, item_path)
            elif item_path.is_file():
                item.setIcon(self.file_icon)
                item.setData("file", TYPE_DATA_ROLE)
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsUserTristate)
                parent_node.appendRow(item)

    def populate_tree(self):
        self.model.clear()
        root_node = self.model.invisibleRootItem()
        self._populate_recursive(root_node, self.folder_path)

    def on_item_changed(self, item: QStandardItem):
        if not item or not item.isCheckable():
            return
        self.model.blockSignals(True)
        current_check_state = item.checkState()
        item_type = item.data(TYPE_DATA_ROLE)
        if item_type == "folder" and current_check_state != Qt.CheckState.PartiallyChecked:
            self._set_child_check_state_recursive(item, current_check_state)
        self._update_parent_check_state(item)
        self.model.blockSignals(False)

    def _set_child_check_state_recursive(self, parent_item: QStandardItem, state: Qt.CheckState):
        if state == Qt.CheckState.PartiallyChecked:
            return
        for row in range(parent_item.rowCount()):
            child = parent_item.child(row, 0)
            if child and child.isCheckable():
                if child.checkState() != state:
                    child.setCheckState(state)

    def _update_parent_check_state(self, item: QStandardItem):
        parent = item.parent()
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
        new_parent_state = Qt.CheckState.Unchecked
        if partially_checked_children > 0:
            new_parent_state = Qt.CheckState.PartiallyChecked
        elif checked_children == total_checkable_children and total_checkable_children > 0:
            new_parent_state = Qt.CheckState.Checked
        elif checked_children > 0:
            new_parent_state = Qt.CheckState.PartiallyChecked
        if parent.checkState() != new_parent_state:
            parent.setCheckState(new_parent_state)

    def accept(self):
        self._selected_items_for_worker = []
        root = self.model.invisibleRootItem()
        base_path_for_dialog_items = str(self.folder_path.parent.resolve())
        self._collect_selected_items_recursive(
            root, base_path_for_dialog_items)
        super().accept()

    def _collect_selected_items_recursive(self, parent_item: QStandardItem, base_path_str: str):
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
                self._selected_items_for_worker.append(
                    (item_type, item_path_str, base_path_str))
            elif state == Qt.CheckState.PartiallyChecked:
                if item_type == "folder":
                    self._collect_selected_items_recursive(item, base_path_str)

    def get_selected_items(self):
        return self._selected_items_for_worker


# --- Main Application Window ---
class MergerSplitterApp(QWidget):
    # --- Stylesheet (remains the same) ---
    DARK_STYLESHEET = """
        /* ... Stylesheet content identical to original ... */
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

        /* ComboBox */
        QComboBox {
            border: 1px solid #555555;
            border-radius: 4px;
            padding: 4px 8px; /* Adjust padding */
            background-color: #3c3c3c;
            color: #e0e0e0;
            min-width: 6em; /* Ensure minimum width */
        }
        /* Style the drop-down arrow */
        QComboBox::drop-down {
            subcontrol-origin: padding;
            subcontrol-position: top right;
            width: 20px; /* Width of the arrow area */
            border-left-width: 1px;
            border-left-color: #555555;
            border-left-style: solid;
            border-top-right-radius: 3px;
            border-bottom-right-radius: 3px;
        }
        QComboBox::down-arrow {
             /* Use a standard down arrow or a custom image */
             /* image: url(:/dark/down_arrow.png); */
             width: 10px; /* Size of the arrow glyph */
             height: 10px;
        }
        /* Style the drop-down list */
        QComboBox QAbstractItemView {
            border: 1px solid #666666;
            background-color: #3f3f3f; /* Slightly different background for dropdown */
            color: #e0e0e0;
            selection-background-color: #569cd6; /* Selection color in dropdown */
            selection-color: #ffffff;
            padding: 2px; /* Padding inside the dropdown list */
        }
        QComboBox:disabled {
             background-color: #404040;
             color: #777777;
        }
    """

    def __init__(self):
        super().__init__()
        self._items_to_merge_internal = []
        self.output_merge_file = ""
        self.input_split_file = ""
        self.output_split_dir = ""
        self.worker_thread = None
        self.worker = None

        # Icons
        self.folder_icon = QIcon()
        self.file_icon = QIcon()

        self.initUI()
        self.apply_dark_style()
        self._populate_format_combos()  # Populate dropdowns after UI init

    def initUI(self):
        self.setObjectName("MergerSplitterAppWindow")
        self.setWindowTitle('File Merger & Splitter (Multi-Format)')
        # Increased height slightly for format combo
        self.setGeometry(150, 150, 850, 800)

        main_layout = QVBoxLayout(self)
        self.tab_widget = QTabWidget()
        main_layout.addWidget(self.tab_widget, 1)

        self.merge_tab = QWidget()
        self.split_tab = QWidget()
        self.tab_widget.addTab(self.merge_tab, " Merge Files/Folders ")
        self.tab_widget.addTab(self.split_tab, " Split Merged File ")

        # --- Populate Merge Tab ---
        merge_layout = QVBoxLayout(self.merge_tab)

        # Top Buttons
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

        # Tree View
        merge_layout.addWidget(QLabel("<b>Items to Merge:</b>"))
        self.item_list_view = QTreeView()
        self.item_list_view.setHeaderHidden(True)
        self.item_model = QStandardItemModel()
        self.item_list_view.setModel(self.item_model)
        self.item_list_view.setSelectionMode(
            QTreeView.SelectionMode.ExtendedSelection)
        self.item_list_view.setEditTriggers(
            QTreeView.EditTrigger.NoEditTriggers)
        self.item_list_view.setAlternatingRowColors(True)
        self.item_list_view.setSortingEnabled(False)
        merge_layout.addWidget(self.item_list_view, 1)

        # --- Format Selection (Merge) ---
        format_merge_layout = QHBoxLayout()
        format_merge_layout.addWidget(QLabel("Merge Format:"))
        self.merge_format_combo = QComboBox()
        self.merge_format_combo.setToolTip(
            "Select the delimiter format for the merged output file.")
        format_merge_layout.addWidget(self.merge_format_combo)
        format_merge_layout.addStretch(1)  # Push combo to the left
        merge_layout.addLayout(format_merge_layout)

        # Output File Selection
        output_merge_layout = QHBoxLayout()
        self.select_output_merge_button = QPushButton(
            "Select Output Merged File...")
        self.select_output_merge_button.setSizePolicy(
            QSizePolicy.Policy.Maximum, QSizePolicy.Policy.Fixed)
        self.output_merge_label = QLabel("[Output file not selected]")
        self.output_merge_label.setObjectName("OutputMergeLabel")
        self.output_merge_label.setWordWrap(False)
        self.output_merge_label.setAlignment(
            Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)
        output_merge_layout.addWidget(self.select_output_merge_button)
        output_merge_layout.addWidget(self.output_merge_label, 1)
        merge_layout.addLayout(output_merge_layout)

        # Merge Action Buttons
        merge_actions_layout = QHBoxLayout()
        merge_actions_layout.addStretch()
        self.merge_button = QPushButton(" Merge ")
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

        # Input File Selection
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

        # Output Directory Selection
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

        # --- Format Selection (Split) ---
        format_split_layout = QHBoxLayout()
        format_split_layout.addWidget(QLabel("Split Format:"))
        self.split_format_combo = QComboBox()
        self.split_format_combo.setToolTip(
            "Select the delimiter format expected in the merged file.")
        format_split_layout.addWidget(self.split_format_combo)
        format_split_layout.addStretch(1)  # Push combo to the left
        split_layout.addLayout(format_split_layout)

        split_layout.addSpacerItem(QSpacerItem(
            20, 20, QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Expanding))  # Push buttons down

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
        self.log_text.setLineWrapMode(QTextEdit.LineWrapMode.WidgetWidth)
        self.log_text.setFixedHeight(200)
        shared_controls_layout.addWidget(self.log_text)

        self.progress_bar = QProgressBar()
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setValue(0)
        self.progress_bar.setFixedHeight(24)
        self.progress_bar.setAlignment(Qt.AlignmentFlag.AlignCenter)
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
        # Update merge button state when format changes too
        self.merge_format_combo.currentIndexChanged.connect(
            self._update_merge_button_state)

        self.select_input_split_button.clicked.connect(
            self.select_input_split_file)
        self.select_output_split_button.clicked.connect(
            self.select_output_split_dir)
        self.split_button.clicked.connect(self.start_split)
        self.split_cancel_button.clicked.connect(self.cancel_operation)
        # Update split button state when format changes too
        self.split_format_combo.currentIndexChanged.connect(
            self._update_split_button_state)

        self._update_merge_button_state()
        self._update_split_button_state()  # Initial state check

    def apply_dark_style(self):
        """Applies the dark mode stylesheet and Fusion style."""
        # ... (rest of the method is identical to original) ...
        try:
            QApplication.setStyle(QStyleFactory.create('Fusion'))
        except Exception as e:
            print(f"Warning: Could not apply Fusion style: {e}")
        self.setStyleSheet(self.DARK_STYLESHEET)
        try:
            style = self.style()
            self.folder_icon = style.standardIcon(
                QStyle.StandardPixmap.SP_DirIcon)
            self.file_icon = style.standardIcon(
                QStyle.StandardPixmap.SP_FileIcon)
            merge_icon = style.standardIcon(
                QStyle.StandardPixmap.SP_DialogSaveButton)
            split_icon = style.standardIcon(
                QStyle.StandardPixmap.SP_ArrowRight)
            cancel_icon = style.standardIcon(
                QStyle.StandardPixmap.SP_DialogCancelButton)
            remove_icon = style.standardIcon(
                QStyle.StandardPixmap.SP_TrashIcon)
            clear_icon = style.standardIcon(
                QStyle.StandardPixmap.SP_DialogResetButton)

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
        # self.log("Applied dark theme stylesheet and icons.") # Log might not be ready

    def _populate_format_combos(self):
        """Populates the format combo boxes."""
        self.merge_format_combo.clear()
        self.split_format_combo.clear()
        format_names = list(MERGE_FORMATS.keys())
        if not format_names:
            self.log("Error: No merge formats defined in MERGE_FORMATS.")
            self.merge_format_combo.addItem("Error: No Formats")
            self.split_format_combo.addItem("Error: No Formats")
            return

        self.merge_format_combo.addItems(format_names)
        self.split_format_combo.addItems(format_names)
        # Optionally set a default selection
        if "Default" in format_names:
            self.merge_format_combo.setCurrentText("Default")
            self.split_format_combo.setCurrentText("Default")
        self.log(f"Populated format selectors with: {', '.join(format_names)}")

    def log(self, message):
        # Ensure log_text exists before appending
        if hasattr(self, 'log_text') and self.log_text:
            self.log_text.append(message)
            self.log_text.verticalScrollBar().setValue(
                self.log_text.verticalScrollBar().maximum())  # Scroll to bottom
            # Process events to update UI immediately (use sparingly)
            QApplication.processEvents()
        else:
            print(f"LOG (pre-init): {message}")  # Fallback

    def update_progress(self, value):
        # ... (identical to original) ...
        safe_value = max(0, min(value, 100))
        self.progress_bar.setValue(safe_value)
        self.progress_bar.setFormat(f"%p% ({safe_value}%)")

    def operation_finished(self, success, message):
        # ... (identical to original, includes worker cleanup) ...
        self.log(f"Operation Finished: Success={success}, Message='{message}'")
        self.progress_bar.setValue(100)
        self.progress_bar.setFormat("Finished")

        if success:
            QMessageBox.information(self, "Operation Complete", message)
        else:
            if not hasattr(self, '_error_shown') or not self._error_shown:
                if "cancel" in message.lower():
                    self.log("Operation was cancelled.")
                else:
                    QMessageBox.warning(self, "Operation Finished", message)

        self._reset_error_flag()
        self._set_ui_enabled(True)  # Re-enable UI
        self._update_merge_button_state()
        self._update_split_button_state()  # Also update split state

        if self.worker:
            try:  # Disconnect signals
                self.worker.signals.progress.disconnect(self.update_progress)
                self.worker.signals.log.disconnect(self.log)
                self.worker.signals.error.disconnect(self.operation_error)
                self.worker.signals.finished.disconnect(
                    self.operation_finished)
            except TypeError:
                pass  # Ignore if already disconnected
            except Exception as e:
                self.log(f"Warning: Error disconnecting worker signals: {e}")
            self.worker.deleteLater()  # Schedule worker deletion

        if self.worker_thread:
            if self.worker_thread.isRunning():
                self.worker_thread.quit()
                if not self.worker_thread.wait(1500):
                    self.log(
                        "Warning: Worker thread didn't quit gracefully. Terminating.")
                    self.worker_thread.terminate()
                    self.worker_thread.wait(500)
            self.worker_thread.deleteLater()  # Schedule thread deletion

        self.worker_thread = None
        self.worker = None
        self.log("Worker thread and object resources released.")

    def operation_error(self, error_message):
        """Slot specifically for critical errors reported by the worker."""
        # ... (identical to original) ...
        self.log(f"CRITICAL ERROR received: {error_message}")
        QMessageBox.critical(self, "Critical Operation Error", error_message)
        self._error_shown = True  # Flag that a critical error message was displayed

    def _reset_error_flag(self):
        """Reset the flag that tracks if a critical error message was shown."""
        # ... (identical to original) ...
        if hasattr(self, '_error_shown'):
            del self._error_shown

    def _set_ui_enabled(self, enabled):
        """Enable/disable UI elements during processing."""
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
        self.merge_format_combo.setEnabled(
            enabled)  # Enable/disable format choice
        self.select_output_merge_button.setEnabled(enabled)
        self.merge_button.setEnabled(enabled and self._can_start_merge())
        self.merge_cancel_button.setEnabled(
            not enabled and is_merge_tab_active)

        # --- Split Tab Controls ---
        is_split_tab_active = (self.tab_widget.widget(
            current_index) == self.split_tab)
        self.select_input_split_button.setEnabled(enabled)
        self.select_output_split_button.setEnabled(enabled)
        self.split_format_combo.setEnabled(
            enabled)  # Enable/disable format choice
        self.split_button.setEnabled(enabled and self._can_start_split())
        self.split_cancel_button.setEnabled(
            not enabled and is_split_tab_active)

    def _update_merge_button_state(self):
        """Enable/disable the Merge button based on whether items and output file are selected."""
        # ... (identical to original, except _can_start_merge now also checks format) ...
        can_merge = self._can_start_merge()
        self.merge_button.setEnabled(can_merge)
        has_items = len(self._items_to_merge_internal) > 0
        has_selection = len(self.item_list_view.selectedIndexes()) > 0
        self.remove_item_button.setEnabled(has_items and has_selection)
        self.clear_list_button.setEnabled(has_items)

    def _can_start_merge(self):
        """Check if conditions are met to start merging."""
        # Check if a valid format is selected
        selected_format_name = self.merge_format_combo.currentText()
        format_ok = selected_format_name and selected_format_name in MERGE_FORMATS
        return bool(self._items_to_merge_internal and self.output_merge_file and format_ok)

    def _update_split_button_state(self):
        """Enable/disable the Split button based on whether input, output and format are selected."""
        # ... (identical to original, except _can_start_split now also checks format) ...
        can_split = self._can_start_split()
        self.split_button.setEnabled(can_split)

    def _can_start_split(self):
        """Check if conditions are met to start splitting."""
        input_exists = os.path.isfile(self.input_split_file)  # Quick check
        # Check if a valid format is selected
        selected_format_name = self.split_format_combo.currentText()
        format_ok = selected_format_name and selected_format_name in MERGE_FORMATS
        return bool(self.input_split_file and input_exists and self.output_split_dir and format_ok)

    def add_files(self):
        """Adds selected files to the merge list and tree view."""
        # ... (identical to original) ...
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
        files, _ = QFileDialog.getOpenFileNames(
            self, "Select Files to Merge", start_dir, "All Files (*.*)")
        if files:
            added_count = 0
            root_node = self.item_model.invisibleRootItem()
            added_view_paths_this_op = set()
            for file_path_str in files:
                try:
                    file_path = pathlib.Path(file_path_str).resolve()
                    if not file_path.is_file():
                        self.log(
                            f"Warning: Selected item is not a file or does not exist: {file_path_str}")
                        continue
                    base_path = file_path.parent.resolve()
                    item_data_tuple = ("file", str(file_path), str(base_path))
                    if str(file_path) not in [item[1] for item in self._items_to_merge_internal]:
                        self._items_to_merge_internal.append(item_data_tuple)
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
                            item.setData(item_data_tuple[0], TYPE_DATA_ROLE)
                            item.setData(item_data_tuple[1], PATH_DATA_ROLE)
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
                self._update_merge_button_state()
            elif files:
                self.log("Selected file(s) were already in the list.")

    def add_folder(self):
        """Opens the FolderSelectionDialog and adds selected items."""
        # ... (identical to original) ...
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
        folder_path_str = QFileDialog.getExistingDirectory(
            self, "Select Folder to Scan", start_dir)
        if folder_path_str:
            try:
                folder_path = pathlib.Path(folder_path_str).resolve()
                if not folder_path.is_dir():
                    QMessageBox.warning(
                        self, "Invalid Folder", f"Not a valid directory:\n{folder_path_str}")
                    return
                dialog = FolderSelectionDialog(str(folder_path), self)
                if dialog.exec() == QDialog.DialogCode.Accepted:
                    selected_items_for_worker = dialog.get_selected_items()
                    if not selected_items_for_worker:
                        self.log(
                            f"No items selected from folder: {folder_path}")
                        return
                    added_count_worker = 0
                    added_count_view = 0
                    items_already_present_paths = set(
                        item[1] for item in self._items_to_merge_internal)
                    new_items_for_worker = []
                    for item_tuple in selected_items_for_worker:
                        item_abs_path = item_tuple[1]
                        if item_abs_path not in items_already_present_paths:
                            new_items_for_worker.append(item_tuple)
                            items_already_present_paths.add(item_abs_path)
                            added_count_worker += 1
                    self._items_to_merge_internal.extend(new_items_for_worker)
                    root_node = self.item_model.invisibleRootItem()
                    folder_root_path_str = str(folder_path)
                    existing_folder_root_item = None
                    for row in range(root_node.rowCount()):
                        item = root_node.child(row, 0)
                        if item and item.data(TYPE_DATA_ROLE) == "folder-root" and item.data(PATH_DATA_ROLE) == folder_root_path_str:
                            existing_folder_root_item = item
                            break
                    if not existing_folder_root_item:
                        display_text = f"{folder_path.name} ({len(selected_items_for_worker)} selected)"
                        folder_item = QStandardItem(
                            self.folder_icon, display_text)
                        folder_item.setToolTip(
                            f"Folder Source: {folder_root_path_str}\nSelected {len(selected_items_for_worker)} item(s) within.")
                        folder_item.setData("folder-root", TYPE_DATA_ROLE)
                        folder_item.setData(
                            folder_root_path_str, PATH_DATA_ROLE)
                        base_path_from_dialog = selected_items_for_worker[
                            0][2] if selected_items_for_worker else ""
                        folder_item.setData(
                            base_path_from_dialog, BASE_PATH_DATA_ROLE)
                        folder_item.setEditable(False)
                        root_node.appendRow(folder_item)
                        added_count_view += 1
                    else:
                        self.log(
                            f"Note: Selection from folder '{folder_path.name}' updated.")
                        display_text = f"{folder_path.name} ({len(selected_items_for_worker)} selected)"
                        existing_folder_root_item.setText(display_text)
                        existing_folder_root_item.setToolTip(
                            f"Folder Source: {folder_root_path_str}\nSelected {len(selected_items_for_worker)} item(s) within (updated).")
                    if added_count_worker > 0:
                        self.log(
                            f"Added {added_count_worker} new item(s) from folder '{folder_path.name}' to merge list.")
                    else:
                        self.log(
                            f"No *new* items selected from folder '{folder_path.name}' were added (already present).")
                    if added_count_view > 0:
                        self.log(
                            f"Added representation for folder '{folder_path.name}' to the view.")
                    self._update_merge_button_state()
                else:
                    self.log(f"Folder selection cancelled for: {folder_path}")
            except OSError as e:
                self.log(
                    f"Error resolving folder path '{folder_path_str}': {e}")
                QMessageBox.critical(
                    self, "Error", f"Could not access or resolve folder:\n{folder_path_str}\n\nError: {e}")
            except Exception as e:
                self.log(
                    f"Unexpected error adding folder '{folder_path_str}': {e}\n{traceback.format_exc()}")
                QMessageBox.critical(
                    self, "Error", f"An unexpected error occurred adding folder:\n{e}")

    def remove_selected_items(self):
        """Removes selected items from the tree view AND the internal worker list."""
        # ... (identical to original) ...
        selected_indexes = self.item_list_view.selectedIndexes()
        if not selected_indexes:
            self.log("No item(s) selected to remove.")
            self._update_merge_button_state()
            return
        items_to_remove_from_view = set()
        view_item_paths_to_consider_removing = set()
        paths_to_remove_from_worker = set()
        for index in selected_indexes:
            if not index.isValid() or index.column() != 0:
                continue
            item = self.item_model.itemFromIndex(index)
            if item and item not in items_to_remove_from_view:
                items_to_remove_from_view.add(item)
                item_path = item.data(PATH_DATA_ROLE)
                item_type = item.data(TYPE_DATA_ROLE)
                if item_path:
                    view_item_paths_to_consider_removing.add(item_path)
                    if item_type == "folder-root":
                        folder_root_path_str = item_path
                        folder_root_base_path = item.data(BASE_PATH_DATA_ROLE)
                        self.log(
                            f"Removing 'folder-root': {item.text()}. Finding associated items...")
                        for worker_tuple in self._items_to_merge_internal:
                            worker_type, worker_path, worker_base = worker_tuple
                            if worker_base == folder_root_base_path and worker_path.startswith(folder_root_path_str):
                                paths_to_remove_from_worker.add(worker_path)
                    else:
                        paths_to_remove_from_worker.add(item_path)
        initial_worker_count = len(self._items_to_merge_internal)
        if paths_to_remove_from_worker:
            self._items_to_merge_internal = [
                item_tuple for item_tuple in self._items_to_merge_internal if item_tuple[1] not in paths_to_remove_from_worker]
        removed_count_worker = initial_worker_count - \
            len(self._items_to_merge_internal)
        removal_map = {}
        for item in items_to_remove_from_view:
            parent = item.parent() or self.item_model.invisibleRootItem()
            row = item.row()
            if parent not in removal_map:
                removal_map[parent] = []
            removal_map[parent].append(row)
        removed_count_display = 0
        self.item_model.blockSignals(True)
        try:
            for parent, rows in removal_map.items():
                for row in sorted(rows, reverse=True):
                    if parent.removeRow(row):
                        removed_count_display += 1
                    else:
                        self.log(
                            f"Warning: Failed to remove row {row} from view model parent '{parent.text()}'.")
        finally:
            self.item_model.blockSignals(False)
        if removed_count_display > 0 or removed_count_worker > 0:
            self.log(
                f"Removed {removed_count_display} item(s) from view and {removed_count_worker} item(s) from merge list.")
        else:
            self.log("No corresponding items found to remove.")
        self._update_merge_button_state()

    def clear_item_list(self):
        """Clears both the view model and the internal worker list."""
        # ... (identical to original) ...
        if not self._items_to_merge_internal and self.item_model.rowCount() == 0:
            self.log("List is already empty.")
            return
        reply = QMessageBox.question(self, "Confirm Clear", "Remove all items from merge list?",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            self.item_model.clear()
            self._items_to_merge_internal.clear()
            self.log("Cleared item list and merge data.")
            self._update_merge_button_state()
        else:
            self.log("Clear operation cancelled.")

    def select_output_merge_file(self):
        """Selects the output .txt file for merging."""
        # ... (identical to original) ...
        start_dir = os.path.dirname(
            self.output_merge_file) if self.output_merge_file else QDir.currentPath()
        suggested_filename = os.path.join(
            start_dir, "merged_output.txt")  # Default suggestion
        file_path, file_filter = QFileDialog.getSaveFileName(
            self, "Save Merged File As", suggested_filename, "Text Files (*.txt);;All Files (*)")
        if file_path:
            p = pathlib.Path(file_path)
            if file_filter == "Text Files (*.txt)" and not p.suffix:
                file_path += ".txt"
                p = pathlib.Path(file_path)
            self.output_merge_file = str(p.resolve())
            display_path = self._truncate_path_display(self.output_merge_file)
            self.output_merge_label.setText(display_path)
            self.output_merge_label.setToolTip(self.output_merge_file)
            self.log(f"Selected merge output file: {self.output_merge_file}")
            self._update_merge_button_state()

    def select_input_split_file(self):
        """Selects the input .txt file for splitting."""
        # ... (identical to original) ...
        start_dir = os.path.dirname(
            self.input_split_file) if self.input_split_file else QDir.currentPath()
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select Merged File to Split", start_dir, "Text Files (*.txt);;All Files (*)")
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
        # ... (identical to original) ...
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
        """Truncates a path string for display, adding ellipsis."""
        # ... (identical to original) ...
        if len(path_str) <= max_len:
            return path_str
        else:
            parts = pathlib.Path(path_str).parts
            if len(parts) > 2:
                truncated = f"...{os.sep}{parts[-2]}{os.sep}{parts[-1]}"
                if len(truncated) > max_len:
                    truncated = f"...{os.sep}{parts[-1]}"
                if len(truncated) > max_len:
                    truncated = "..." + parts[-1][-(max_len-4):]
                return truncated
            elif len(parts) == 2:
                return f"{parts[0]}{os.sep}...{os.sep}{parts[-1]}"
            else:
                return path_str[:max_len-3] + "..."

    def _create_output_dir_if_needed(self, dir_path_str, operation_name):
        """Checks if a directory exists and is writable, prompts to create if not."""
        # ... (identical to original) ...
        if not dir_path_str:
            self.log(
                f"Error: Output directory path is empty for {operation_name}.")
            return False
        try:
            dir_path = pathlib.Path(dir_path_str)
            if not dir_path.exists():
                reply = QMessageBox.question(
                    self, f"Create Directory?", f"Output directory does not exist:\n{dir_path}\n\nCreate it?", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.Yes)
                if reply == QMessageBox.StandardButton.Yes:
                    try:
                        dir_path.mkdir(parents=True, exist_ok=True)
                        self.log(f"Created output directory: {dir_path}")
                        if not os.access(str(dir_path), os.W_OK):
                            raise OSError("Directory created but not writable")
                        return True
                    except OSError as e:
                        QMessageBox.critical(
                            self, f"{operation_name} Error", f"Could not create/write directory:\n{dir_path}\n\nError: {e}")
                        self.log(f"Error: Failed create/write: {e}")
                        return False
                else:
                    self.log(
                        f"{operation_name} cancelled (directory not created).")
                    return False
            elif not dir_path.is_dir():
                QMessageBox.critical(
                    self, f"{operation_name} Error", f"Output path exists but is not a directory:\n{dir_path}")
                self.log(f"Error: Output path not dir: {dir_path}")
                return False
            elif not os.access(str(dir_path), os.W_OK):
                QMessageBox.critical(
                    self, f"{operation_name} Error", f"Output directory not writable:\n{dir_path}")
                self.log(f"Error: Output dir not writable: {dir_path}")
                return False
            else:
                return True  # Exists, is dir, is writable
        except Exception as e:
            QMessageBox.critical(
                self, f"{operation_name} Error", f"Invalid output directory path:\n{dir_path_str}\n\nError: {e}")
            self.log(f"Error: Invalid output path '{dir_path_str}': {e}")
            return False

    def start_merge(self):
        """Starts the merge operation in a background thread using the selected format."""
        if not self._can_start_merge():
            # Provide more specific feedback
            if not self._items_to_merge_internal:
                msg = "Please add items to merge."
            elif not self.output_merge_file:
                msg = "Please select an output file."
            elif not self.merge_format_combo.currentText() or self.merge_format_combo.currentText() not in MERGE_FORMATS:
                msg = "Please select a valid merge format."
            else:
                msg = "Cannot start merge (check items, output file, and format)."
            QMessageBox.warning(self, "Merge Error", msg)
            self.log(f"Merge aborted: Conditions not met. Reason: {msg}")
            return

        output_dir = os.path.dirname(self.output_merge_file)
        if not self._create_output_dir_if_needed(output_dir, "Merge"):
            self.log("Merge aborted: Output directory check/creation failed.")
            return

        if self.worker_thread and self.worker_thread.isRunning():
            QMessageBox.warning(
                self, "Busy", "Another operation is already in progress.")
            self.log("Merge aborted: Worker active.")
            return

        # --- Get Selected Format ---
        selected_format_name = self.merge_format_combo.currentText()
        selected_format_details = MERGE_FORMATS.get(selected_format_name)
        if not selected_format_details:  # Should be caught by _can_start_merge, but safety check
            QMessageBox.critical(
                self, "Internal Error", f"Selected merge format '{selected_format_name}' not found.")
            self.log(
                f"Error: Cannot find details for merge format '{selected_format_name}'.")
            return

        # Prepare Worker
        self.log_text.clear()
        self.progress_bar.setValue(0)
        self.progress_bar.setFormat("Starting Merge...")
        self._set_ui_enabled(False)
        self._reset_error_flag()
        worker_data = list(self._items_to_merge_internal)
        self.log(
            f"Starting merge with {len(worker_data)} items/sources using format '{selected_format_name}'.")

        # Create and Start Thread
        self.worker_thread = QThread(self)
        # Pass the format details dictionary to the worker
        self.worker = MergerWorker(
            worker_data, self.output_merge_file, selected_format_details)
        self.worker.moveToThread(self.worker_thread)

        # Connect signals (identical to original)
        self.worker.signals.progress.connect(self.update_progress)
        self.worker.signals.log.connect(self.log)
        self.worker.signals.error.connect(self.operation_error)
        self.worker.signals.finished.connect(self.operation_finished)
        self.worker_thread.finished.connect(self.worker.deleteLater)
        self.worker_thread.finished.connect(self.worker_thread.deleteLater)
        self.worker_thread.started.connect(self.worker.run)
        self.worker_thread.start()
        self.log("Merge worker thread started.")

    def start_split(self):
        """Starts the split operation in a background thread using the selected format."""
        if not self._can_start_split():
            # Provide more specific feedback
            if not self.input_split_file or not os.path.isfile(self.input_split_file):
                msg = "Please select a valid input file."
            elif not self.output_split_dir:
                msg = "Please select an output directory."
            elif not self.split_format_combo.currentText() or self.split_format_combo.currentText() not in MERGE_FORMATS:
                msg = "Please select a valid split format."
            else:
                msg = "Cannot start split (check input file, output directory, and format)."
            QMessageBox.warning(self, "Split Error", msg)
            self.log(f"Split aborted: Conditions not met. Reason: {msg}")
            return

        if not self._create_output_dir_if_needed(self.output_split_dir, "Split"):
            self.log("Split aborted: Output directory check/creation failed.")
            return

        if self.worker_thread and self.worker_thread.isRunning():
            QMessageBox.warning(
                self, "Busy", "Another operation is already in progress.")
            self.log("Split aborted: Worker active.")
            return

        # --- Get Selected Format ---
        selected_format_name = self.split_format_combo.currentText()
        selected_format_details = MERGE_FORMATS.get(selected_format_name)
        if not selected_format_details:  # Should be caught by _can_start_split
            QMessageBox.critical(
                self, "Internal Error", f"Selected split format '{selected_format_name}' not found.")
            self.log(
                f"Error: Cannot find details for split format '{selected_format_name}'.")
            return

        # Prepare Worker
        self.log_text.clear()
        self.progress_bar.setValue(0)
        self.progress_bar.setFormat("Starting Split...")
        self._set_ui_enabled(False)
        self._reset_error_flag()
        self.log(
            f"Starting split for '{self.input_split_file}' -> '{self.output_split_dir}' using format '{selected_format_name}'")

        # Create and Start Thread
        self.worker_thread = QThread(self)
        # Pass the format details dictionary to the worker
        self.worker = SplitterWorker(
            self.input_split_file, self.output_split_dir, selected_format_details)
        self.worker.moveToThread(self.worker_thread)

        # Connect signals (identical to original)
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
        # ... (identical to original) ...
        if self.worker and self.worker_thread and self.worker_thread.isRunning():
            self.log("Attempting to cancel running operation...")
            try:
                self.worker.stop()
            except Exception as e:
                self.log(f"Error trying to signal worker to stop: {e}")
            self.merge_cancel_button.setEnabled(False)
            self.split_cancel_button.setEnabled(False)
            self.progress_bar.setFormat("Cancelling...")
        else:
            self.log("No operation is currently running to cancel.")

    def closeEvent(self, event):
        """Ensure worker thread is stopped cleanly on application close."""
        # ... (identical to original) ...
        if self.worker_thread and self.worker_thread.isRunning():
            self.log("Close Event: Attempting to stop running operation...")
            self.cancel_operation()
            if not self.worker_thread.wait(2500):
                self.log(
                    "Warning: Worker thread did not terminate gracefully. Forcing.")
                self.worker_thread.terminate()
                self.worker_thread.wait(500)
            else:
                self.log("Worker thread stopped successfully during close event.")
        event.accept()


# --- Main Execution (remains the same) ---
if __name__ == '__main__':
    os.environ["QT_ENABLE_HIGHDPI_SCALING"] = "1"
    os.environ["QT_SCALE_FACTOR_ROUNDING_POLICY"] = "PassThrough"
    # QApplication.setAttribute(Qt.ApplicationAttribute.AA_EnableHighDpiScaling, True) # Optional
    # QApplication.setAttribute(Qt.ApplicationAttribute.AA_UseHighDpiPixmaps, True)   # Optional

    app = QApplication(sys.argv)
    app.setApplicationName("FileMergerSplitter")
    app.setOrganizationName("UtilityApps")
    app.setApplicationVersion("1.2")  # Version bump

    ex = MergerSplitterApp()
    ex.show()
    sys.exit(app.exec())
