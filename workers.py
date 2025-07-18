import os
import re
import pathlib
import traceback
from PyQt6.QtCore import QObject, pyqtSignal
from io import StringIO

# --- Worker Signals ---


class WorkerSignals(QObject):
    ''' Defines signals available from a running worker thread. '''
    progress = pyqtSignal(int)       # Percentage progress
    log = pyqtSignal(str)            # Log message
    finished = pyqtSignal(bool, str)  # Success (bool), final message (str)
    error = pyqtSignal(str)          # Error message used for critical failures
    # Emits the merged text when finished in-memory
    text_ready = pyqtSignal(str)


# --- Hierarchy Tree Delimiters (Used by both workers) ---
TREE_START_DELIMITER = "--- START FILE HIERARCHY ---"
TREE_END_DELIMITER = "--- END FILE HIERARCHY ---"

# --- Merger Worker ---


class MergerWorker(QObject):
    ''' Performs the file merging in a separate thread using a specified format. '''
    signals = WorkerSignals()

    def __init__(self, items_to_merge, merge_format_details, include_tree=False, output_file=None):
        super().__init__()
        self.items_to_merge = items_to_merge
        self.output_file = output_file
        self.merge_format_details = merge_format_details
        self.include_tree = include_tree
        self.is_running = True

    def stop(self):
        print("MergerWorker: Stop signal received.")
        self.is_running = False

    def log(self, msg):
        self.signals.log.emit(msg)

    # --- Helper to generate the tree structure string ---
    def _generate_hierarchy_tree_string(self, files_to_process):
        """Generates a tree string representation of the file hierarchy."""
        if not files_to_process:
            return ""

        T_BRANCH = "├── "
        L_BRANCH = "└── "
        INDENT_CONT = "│   "
        INDENT_EMPTY = "    "
        tree_dict = {}

        # --- Helper to build the nested dictionary structure ---
        def add_path_to_dict(node, path_parts):
            current_level = node
            for i, part in enumerate(path_parts):
                is_last_part = (i == len(path_parts) - 1)
                if is_last_part:
                    if "_files" not in current_level:
                        current_level["_files"] = []
                    if part not in current_level["_files"]:
                        current_level["_files"].append(part)
                else:
                    if part not in current_level:
                        current_level[part] = {}
                    current_level = current_level[part]

        # --- Populate the dictionary ---
        root_name = None
        processed_relative_paths = set()
        for _, relative_path, _ in files_to_process:
            rel_path_obj = pathlib.Path(relative_path)
            rel_path_str_posix = rel_path_obj.as_posix()

            if rel_path_str_posix in processed_relative_paths:
                continue
            processed_relative_paths.add(rel_path_str_posix)

            path_parts = list(rel_path_obj.parts)
            if not path_parts:
                continue

            if root_name is None:
                root_name = path_parts[0]
                if root_name not in tree_dict:
                    tree_dict[root_name] = {}
            if path_parts[0] == root_name:
                add_path_to_dict(tree_dict[root_name], path_parts[1:])
            else:
                self.log(
                    f"Warning: Path '{rel_path_str_posix}' does not share common root '{root_name}'. Adding as separate root.")
                root_name_alt = path_parts[0]
                if root_name_alt not in tree_dict:
                    tree_dict[root_name_alt] = {}
                add_path_to_dict(tree_dict[root_name_alt], path_parts[1:])

        # --- Helper to recursively format the tree string ---
        output_lines = []

        def format_node(name, node_content, indent, is_last_node):
            prefix = L_BRANCH if is_last_node else T_BRANCH
            output_lines.append(f"{indent}{prefix}{name}/")
            child_indent = indent + \
                (INDENT_EMPTY if is_last_node else INDENT_CONT)
            dirs = sorted([k for k in node_content if k !=
                          "_files"], key=str.lower)
            files = sorted(node_content.get("_files", []), key=str.lower)
            items = dirs + files
            total_items = len(items)
            for i, item_name in enumerate(items):
                is_last_item = (i == total_items - 1)
                if item_name in dirs:
                    format_node(
                        item_name, node_content[item_name], child_indent, is_last_item)
                else:
                    child_prefix = L_BRANCH if is_last_item else T_BRANCH
                    output_lines.append(
                        f"{child_indent}{child_prefix}{item_name}")

        # --- Format the tree starting from the roots ---
        root_keys = sorted(tree_dict.keys(), key=str.lower)
        total_roots = len(root_keys)
        for i, current_root_name in enumerate(root_keys):
            is_last_root = (i == total_roots - 1)
            format_node(current_root_name,
                        tree_dict[current_root_name], "", is_last_root)

        # --- Combine with delimiters ---
        final_string = TREE_START_DELIMITER + "\n" + \
            "\n".join(output_lines) + "\n" + TREE_END_DELIMITER + "\n"
        return final_string

    def run(self):
        if self.output_file:
            self.log(
                f"Starting merge process -> {self.output_file} (Format: {self.merge_format_details['name']})")
        else:
            self.log(
                f"Starting merge to text view (Format: {self.merge_format_details['name']})")

        if self.include_tree:
            self.log("File hierarchy tree will be included.")

        files_to_process = []
        total_size = 0
        processed_size = 0
        processed_files_count = 0
        encountered_resolved_paths = set()
        output_file_path = None

        start_fmt = self.merge_format_details.get("start", "{filepath}")
        end_fmt = self.merge_format_details.get("end", "")
        separator = self.merge_format_details.get("file_separator", "\n")
        content_prefix = self.merge_format_details.get("content_prefix", "")
        content_suffix = self.merge_format_details.get("content_suffix", "")

        try:
            # --- Phase 1: Discover all files ---
            self.log("Scanning files and folders based on input selections...")
            initial_item_count = len(self.items_to_merge)
            files_discovered_in_scan = []
            for item_idx, (item_type, item_path_str, base_path_str) in enumerate(self.items_to_merge):
                if not self.is_running:
                    break
                try:
                    item_path = pathlib.Path(item_path_str).resolve()
                    base_path = pathlib.Path(base_path_str).resolve(
                    ) if base_path_str else item_path.parent.resolve()
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
                                try:
                                    relative_path = item_path.relative_to(
                                        item_path.parent)
                                    self.log(
                                        f"Warning: Could not make '{item_path}' relative to base '{base_path}'. Using path relative to parent: '{relative_path}'.")
                                except ValueError:
                                    relative_path = pathlib.Path(
                                        item_path.name)
                                    self.log(
                                        f"Warning: Could not determine relative path for '{item_path}' against base '{base_path}'. Using filename only: '{relative_path}'.")
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
                            except OSError as e_stat:
                                self.log(
                                    f"Warning: Could not get size for {item_path}: {e_stat}. Using size 0.")
                                try:
                                    relative_path_fallback = item_path.relative_to(
                                        base_path)
                                except ValueError:
                                    relative_path_fallback = pathlib.Path(
                                        item_path.name)
                                files_discovered_in_scan.append(
                                    (item_path, relative_path_fallback, 0))
                                encountered_resolved_paths.add(item_path)
                            except Exception as e_other:
                                self.log(
                                    f"Warning: Unexpected error processing file entry {item_path}: {e_other}")
                    else:
                        self.log(
                            f"Warning: Selected file not found during scan: {item_path}")
                elif item_type == "folder" or item_type == "folder-root":
                    if item_path.is_dir():
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
                                            try:
                                                relative_path_fallback = file_path.relative_to(
                                                    item_path)
                                                self.log(
                                                    f"Warning: Could not make '{file_path}' relative to original base '{base_path}'. Using path relative to scanned folder '{item_path}': '{relative_path_fallback}'.")
                                            except ValueError:
                                                relative_path_fallback = pathlib.Path(
                                                    file_path.name)
                                                self.log(
                                                    f"Error: Could not even make '{file_path}' relative to its walk root '{item_path}'. Using filename only: '{relative_path_fallback}'.")
                                            try:
                                                fsize = file_path.stat().st_size
                                                files_discovered_in_scan.append(
                                                    (file_path, relative_path_fallback, fsize))
                                                total_size += fsize
                                                encountered_resolved_paths.add(
                                                    file_path)
                                            except OSError as e_size:
                                                self.log(
                                                    f"Warning: Could not get size for {file_path}: {e_size}. Using size 0.")
                                                files_discovered_in_scan.append(
                                                    (file_path, relative_path_fallback, 0))
                                                encountered_resolved_paths.add(
                                                    file_path)
                                        except OSError as e_stat:
                                            self.log(
                                                f"Warning: Could not get size for {file_path}: {e_stat}. Using size 0.")
                                            try:
                                                rel_p = file_path.relative_to(
                                                    base_path)
                                            except ValueError:
                                                try:
                                                    rel_p = file_path.relative_to(
                                                        item_path)
                                                except ValueError:
                                                    rel_p = pathlib.Path(
                                                        file_path.name)
                                            files_discovered_in_scan.append(
                                                (file_path, rel_p, 0))
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
            outfile_context = None
            if self.output_file:
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
                outfile_context = open(
                    output_file_path, "w", encoding="utf-8", errors='replace')
            else:
                outfile_context = StringIO()

            result_text = None

            with outfile_context as outfile:
                # --- Write Hierarchy Tree if requested ---
                if self.include_tree:
                    self.log("Generating and writing hierarchy tree...")
                    tree_content = self._generate_hierarchy_tree_string(
                        files_to_process)
                    if tree_content:
                        outfile.write(tree_content)
                        if files_to_process:
                            outfile.write(separator)
                    else:
                        self.log("No files processed, skipping tree writing.")

                # --- Write file content blocks ---
                total_files_count = len(files_to_process)
                for i, (absolute_path, relative_path, fsize) in enumerate(files_to_process):
                    if not self.is_running:
                        break

                    relative_path_str = relative_path.as_posix()
                    start_delimiter = start_fmt.format(
                        filepath=relative_path_str)
                    try:
                        end_delimiter = end_fmt.format(
                            filepath=relative_path_str)
                    except KeyError:
                        end_delimiter = end_fmt

                    outfile.write(start_delimiter + "\n")
                    if content_prefix:
                        outfile.write(content_prefix)

                    file_content = ""
                    try:
                        try:
                            with open(absolute_path, "r", encoding='utf-8', errors='strict') as infile:
                                file_content = infile.read()
                        except UnicodeDecodeError:
                            self.log(
                                f"Warning: Non-UTF-8 file detected: '{relative_path_str}'. Attempting 'latin-1' decode.")
                            try:
                                with open(absolute_path, "r", encoding='latin-1') as infile:
                                    file_content = infile.read()
                            except Exception as e_latin:
                                self.log(
                                    f"Warning: Failed to decode '{relative_path_str}' as latin-1: {e_latin}. Using lossy UTF-8.")
                                with open(absolute_path, 'r', encoding='utf-8', errors='replace') as infile:
                                    file_content = infile.read()
                        except Exception as e_read:
                            self.log(
                                f"Error reading file '{absolute_path}': {e_read}. Inserting error message.")
                            file_content = f"Error reading file: {e_read}"

                        outfile.write(file_content)
                        if file_content and not file_content.endswith('\n'):
                            outfile.write("\n")
                    except Exception as e_outer:
                        self.log(
                            f"Critical error processing file content for {absolute_path}: {e_outer}\n{traceback.format_exc()}")
                        outfile.write(
                            f"\nError processing file content: {e_outer}\n")

                    if content_suffix:
                        outfile.write(content_suffix)
                    outfile.write(end_delimiter + "\n")

                    if i < total_files_count - 1:
                        outfile.write(separator)

                    # --- Progress Update ---
                    processed_size += fsize
                    processed_files_count += 1
                    if total_size > 0:
                        progress_percent = int(
                            (processed_size / total_size) * 100)
                    elif total_files_count > 0:
                        progress_percent = int(
                            (processed_files_count / total_files_count) * 100)
                    else:
                        progress_percent = 0
                    self.signals.progress.emit(min(progress_percent, 100))

                if self.is_running and not self.output_file:
                    result_text = outfile.getvalue()

            # --- Final checks and signals ---
            if not self.is_running:
                self.log("Merge cancelled during writing phase.")
                if self.output_file and output_file_path and output_file_path.exists():
                    try:
                        output_file_path.unlink()
                        self.log(
                            f"Removed incomplete file: {output_file_path}")
                    except OSError as e:
                        self.log(
                            f"Could not remove incomplete file '{output_file_path}': {e}")
                self.signals.finished.emit(False, "Merge cancelled.")
            elif self.output_file:
                self.signals.progress.emit(100)
                self.log("Merge process completed successfully.")
                self.signals.finished.emit(
                    True, f"Merge successful! {len(files_to_process)} files merged into '{pathlib.Path(self.output_file).name}'.")
            else:  # In-memory merge finished
                self.signals.progress.emit(100)
                self.log("Merge to text completed successfully.")
                if result_text is not None:
                    self.signals.text_ready.emit(result_text)
                self.signals.finished.emit(
                    True, f"Merge successful! {len(files_to_process)} files merged to text view.")

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
        self.format_details = split_format_details
        self.is_running = True

    def stop(self):
        print("SplitterWorker: Stop signal received.")
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
            tree_skipped_bytes = 0

            with open(merged_file_path, "rb") as infile_b:
                # --- Skip Hierarchy Tree Section (if present) ---
                self.log("Checking for file hierarchy tree...")
                first_line_bytes = b""
                while True:
                    b = infile_b.read(1)
                    if not b or b == b'\n':
                        break
                    first_line_bytes += b
                if b == b'\n':
                    first_line_bytes += b

                processed_size += len(first_line_bytes)
                try:
                    first_line_str = first_line_bytes.decode('utf-8').strip()
                except UnicodeDecodeError:
                    try:
                        first_line_str = first_line_bytes.decode(
                            'latin-1').strip()
                    except Exception:
                        first_line_str = None

                if first_line_str == TREE_START_DELIMITER:
                    self.log(
                        f"Found '{TREE_START_DELIMITER}'. Skipping tree section...")
                    tree_lines_skipped = 1
                    skipped_successfully = False
                    while self.is_running:
                        line_bytes = b""
                        while True:
                            b = infile_b.read(1)
                            if not b or b == b'\n':
                                break
                            line_bytes += b
                        if b == b'\n':
                            line_bytes += b

                        if not line_bytes:
                            self.log(
                                f"Warning: Reached end of file while skipping tree. Expected '{TREE_END_DELIMITER}'.")
                            break

                        processed_size += len(line_bytes)
                        tree_lines_skipped += 1
                        try:
                            line_str = line_bytes.decode('utf-8').strip()
                        except UnicodeDecodeError:
                            try:
                                line_str = line_bytes.decode('latin-1').strip()
                            except Exception:
                                line_str = None

                        if line_str == TREE_END_DELIMITER:
                            self.log(
                                f"Found '{TREE_END_DELIMITER}'. Skipped {tree_lines_skipped} lines of tree header.")
                            skipped_successfully = True
                            tree_skipped_bytes = processed_size
                            if total_size > 0:
                                self.signals.progress.emit(
                                    min(int((processed_size / total_size) * 100), 100))
                            break

                    if not skipped_successfully and self.is_running:
                        self.log(
                            "Warning: Tree section might be incomplete or end delimiter not found. Proceeding with split after scanned lines.")
                    elif not self.is_running:
                        self.log("Split cancelled during tree skipping.")
                        self.signals.finished.emit(False, "Split cancelled.")
                        return
                else:
                    self.log("No hierarchy tree section found at the beginning.")
                    infile_b.seek(0)
                    processed_size = 0

                if total_size > 0:
                    self.signals.progress.emit(
                        min(int((processed_size / total_size) * 100), 100))

                # --- Main Splitting Logic ---
                current_file_path_relative = None
                current_file_content_lines = []
                in_file_block = False
                just_started_block = False
                line_offset = 0

                remaining_buffer = infile_b.read()
                if not self.is_running:
                    self.log(
                        "Split cancelled immediately after reading remaining file.")
                    self.signals.finished.emit(False, "Split cancelled.")
                    return

                try:
                    content_text = remaining_buffer.decode('utf-8')
                except UnicodeDecodeError:
                    self.log(
                        "Warning: Decoding remaining content as UTF-8 failed. Trying latin-1.")
                    try:
                        content_text = remaining_buffer.decode('latin-1')
                    except Exception as e_decode_rest:
                        self.log(
                            f"ERROR: Failed to decode remaining file content: {e_decode_rest}. Aborting split.")
                        self.signals.error.emit(
                            "Failed to decode file content for splitting.")
                        self.signals.finished.emit(
                            False, "Split failed: File decoding error.")
                        return

                lines = content_text.splitlines(keepends=True)
                del content_text
                del remaining_buffer

                for line in lines:
                    if not self.is_running:
                        break

                    line_offset += 1
                    current_line_bytes_est = len(
                        line.encode('utf-8', errors='ignore'))
                    processed_size += current_line_bytes_est
                    if total_size > 0:
                        progress_percent = int(
                            (processed_size / total_size) * 100)
                        self.signals.progress.emit(min(progress_percent, 100))

                    line_stripped = line.strip()

                    # --- State Machine for Parsing ---
                    if not in_file_block:
                        start_match = start_regex.match(line_stripped)
                        if start_match:
                            try:
                                potential_relative_path = start_match.group(
                                    1).strip()
                            except IndexError:
                                self.log(
                                    f"Warning: Regex '{start_regex_pattern}' matched approx line {line_offset} but captured no path group. Skipping block.")
                                continue

                            # --- Basic Path Safety Check ---
                            normalized_path_check = potential_relative_path.replace(
                                "\\", "/")
                            is_safe = True
                            if not potential_relative_path:
                                self.log(
                                    f"Warning: Empty filepath captured by start regex approx line {line_offset}. Skipping block.")
                                is_safe = False
                            elif pathlib.PurePath(potential_relative_path).is_absolute():
                                self.log(
                                    f"Error: Security risk! Absolute path found in delimiter: '{potential_relative_path}' approx line {line_offset}. Skipping block.")
                                is_safe = False
                            elif "../" in normalized_path_check or normalized_path_check.startswith("/"):
                                self.log(
                                    f"Warning: Potential path traversal or absolute-like path detected in delimiter: '{potential_relative_path}' near line {line_offset}. Final check during write.")

                            if not is_safe:
                                continue

                            current_file_path_relative = potential_relative_path
                            current_file_content_lines = []
                            in_file_block = True
                            just_started_block = skip_line_after_start
                            continue
                    else:
                        if just_started_block:
                            just_started_block = False
                            continue

                        expected_end_delimiter = get_end_delimiter_func(
                            current_file_path_relative)
                        if line_stripped == expected_end_delimiter:
                            content_to_write = "".join(
                                current_file_content_lines)
                            if self._write_file(current_file_path_relative, content_to_write):
                                file_count += 1
                                created_file_paths.add(
                                    self.output_dir.joinpath(current_file_path_relative))
                            in_file_block = False
                            current_file_path_relative = None
                            current_file_content_lines = []
                            just_started_block = False
                            continue
                        else:
                            current_file_content_lines.append(line)

            if not self.is_running:
                self.log("Split cancelled during file processing.")
                self.log(
                    f"Attempting cleanup of {len(created_file_paths)} created files...")
                cleaned_count = 0
                for f_path in created_file_paths:
                    try:
                        f_path_resolved = f_path.resolve()
                        if f_path_resolved.is_file():
                            f_path_resolved.unlink()
                            cleaned_count += 1
                    except OSError as e_unlink:
                        self.log(
                            f"Warning: Could not remove partially created file '{f_path}': {e_unlink}")
                    except Exception as e_clean:
                        self.log(
                            f"Warning: Error during cleanup of '{f_path}': {e_clean}")
                self.log(f"Cleanup finished. Removed {cleaned_count} files.")
                self.signals.finished.emit(False, "Split cancelled.")
                return

            if in_file_block and current_file_path_relative:
                self.log(
                    f"Warning: Merged file ended before finding END delimiter for '{current_file_path_relative}'. Saving remaining content.")
                content_to_write = "".join(current_file_content_lines)
                if self._write_file(current_file_path_relative, content_to_write):
                    file_count += 1

            # --- Post-processing ---
            self.signals.progress.emit(100)
            if file_count > 0:
                final_message = f"Split successful! {file_count} files created in '{self.output_dir.name}' (Format: {self.format_details['name']})."
                self.log(final_message)
                self.signals.finished.emit(True, final_message)
            elif not self.is_running:
                pass
            else:
                final_message = f"Split finished, but no valid file blocks matching format '{self.format_details['name']}' were found or extracted."
                self.log(final_message)
                self.signals.finished.emit(False, final_message)
        except FileNotFoundError as e:
            error_msg = f"Input merged file not found: {self.merged_file}"
            self.log(f"Error: {error_msg}")
            self.signals.error.emit(error_msg)
            self.signals.finished.emit(False, f"Split failed: {error_msg}")
        except ValueError as e:
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
        if not relative_path_str:
            self.log(
                "Error: Attempted to write file with empty relative path. Skipping.")
            return False

        cleaned_relative_path = relative_path_str.replace("\\", "/")
        cleaned_relative_path = cleaned_relative_path.strip("./ ")

        if not cleaned_relative_path:
            self.log(
                f"Error: Relative path '{relative_path_str}' became empty after cleaning. Skipping.")
            return False

        target_path_resolved = None
        try:
            target_path = self.output_dir.joinpath(cleaned_relative_path)
            # --- Final Safety Check ---
            try:
                if not self.output_dir.is_dir() or not os.access(str(self.output_dir), os.W_OK):
                    raise FileNotFoundError(
                        f"Output directory '{self.output_dir}' is not accessible or writable.")

                output_dir_resolved = self.output_dir.resolve(strict=True)
                target_path_resolved = target_path.resolve(strict=False)
            except (OSError, ValueError) as e_resolve:
                self.log(
                    f"Error: Invalid path generated for '{cleaned_relative_path}': {e_resolve}. Skipping write.")
                return False
            except FileNotFoundError as e_fnf:
                self.log(
                    f"Error: {e_fnf}. Cannot write '{cleaned_relative_path}'.")
                if self.is_running:
                    self.signals.error.emit(f"Output directory issue: {e_fnf}")
                    self.is_running = False
                return False

            is_within_output_dir = False
            try:
                is_within_output_dir = output_dir_resolved == target_path_resolved or \
                    output_dir_resolved in target_path_resolved.parents
            except Exception as path_comp_err:
                self.log(
                    f"Warning: Could not perform robust path comparison for '{target_path_resolved}': {path_comp_err}")
                output_dir_str = str(output_dir_resolved).replace("\\", "/")
                target_path_str = str(target_path_resolved).replace("\\", "/")
                if target_path_str.startswith(output_dir_str + "/"):
                    is_within_output_dir = True

            if not is_within_output_dir:
                self.log(f"Error: Security risk! Path '{cleaned_relative_path}' resolved to '{target_path_resolved}', "
                         f"which is outside the designated output directory '{output_dir_resolved}'. Skipping write.")
                return False

            target_path_resolved.parent.mkdir(parents=True, exist_ok=True)
            with open(target_path_resolved, "wb") as outfile:
                try:
                    outfile.write(content.encode('utf-8'))
                except UnicodeEncodeError:
                    self.log(
                        f"Warning: Could not encode content for '{target_path_resolved}' as UTF-8. Using latin-1.")
                    outfile.write(content.encode('latin-1', errors='replace'))
            return True
        except OSError as e:
            log_path_str = str(
                target_path_resolved) if target_path_resolved else f"(Failed resolving {cleaned_relative_path})"
            self.log(f"Error writing file '{log_path_str}' (OS Error): {e}")
            return False
        except Exception as e:
            log_path_str = str(
                target_path_resolved) if target_path_resolved else f"(Failed resolving {cleaned_relative_path})"
            self.log(
                f"Error writing file for relative path '{cleaned_relative_path}' (Resolved: {log_path_str}) (General Error): {e}\n{traceback.format_exc()}")
            return False
