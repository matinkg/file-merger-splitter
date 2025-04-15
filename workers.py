import os
import re
import pathlib
import traceback
from PyQt6.QtCore import QObject, pyqtSignal

# --- Worker Signals ---


class WorkerSignals(QObject):
    ''' Defines signals available from a running worker thread. '''
    progress = pyqtSignal(int)       # Percentage progress
    log = pyqtSignal(str)            # Log message
    finished = pyqtSignal(bool, str)  # Success (bool), final message (str)
    error = pyqtSignal(str)          # Error message used for critical failures


# --- Hierarchy Tree Delimiters (Used by both workers) ---
TREE_START_DELIMITER = "--- START FILE HIERARCHY ---"
TREE_END_DELIMITER = "--- END FILE HIERARCHY ---"


# --- Merger Worker ---
class MergerWorker(QObject):
    ''' Performs the file merging in a separate thread using a specified format. '''
    signals = WorkerSignals()

    # --- Update __init__ to accept include_tree ---
    def __init__(self, items_to_merge, output_file, merge_format_details, include_tree=False):
        super().__init__()
        self.items_to_merge = items_to_merge
        self.output_file = output_file
        self.format_details = merge_format_details  # Store the format dictionary
        self.include_tree = include_tree          # Store the flag
        self.is_running = True
        # Note: Logging here might happen before main window log area is fully ready
        # Consider logging initial setup messages from the main thread instead if needed immediately
        # self.log(f"Worker received {len(items_to_merge)} initial selection items.")
        # self.log(f"Using merge format: {self.format_details.get('name', 'Unknown')}")

    def stop(self):
        # self.log("Stop signal received by MergerWorker.") # Log might spam if called repeatedly
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
                    # It's a file
                    if "_files" not in current_level:
                        current_level["_files"] = []
                    if part not in current_level["_files"]: # Avoid duplicates if logic allows
                        current_level["_files"].append(part)
                else:
                    # It's a directory
                    if part not in current_level:
                        current_level[part] = {} # Create subdir node
                    current_level = current_level[part] # Move down

        # --- Populate the dictionary ---
        root_name = None
        processed_relative_paths = set() # Avoid processing duplicates from input list
        for _, relative_path, _ in files_to_process:
            # Ensure we use pathlib.Path and normalize
            rel_path_obj = pathlib.Path(relative_path)
            # Use as_posix for consistent splitting
            rel_path_str_posix = rel_path_obj.as_posix()

            if rel_path_str_posix in processed_relative_paths:
                continue
            processed_relative_paths.add(rel_path_str_posix)

            path_parts = list(rel_path_obj.parts)
            if not path_parts:
                continue

            # Determine root name if not set yet (should be consistent)
            # The root name is the *first* part of the relative path list
            if root_name is None:
                root_name = path_parts[0]
                if root_name not in tree_dict:
                    tree_dict[root_name] = {}

            # Add path parts relative to the root assumed above
            # Example: if root_name='open-idm', path='open-idm/widgets/file.py'
            # We add ['widgets', 'file.py'] to tree_dict['open-idm']
            if path_parts[0] == root_name:
                add_path_to_dict(tree_dict[root_name], path_parts[1:])
            else:
                # This case might happen if multiple unrelated bases were added
                # Add the full path as a separate root for now
                self.log(f"Warning: Path '{rel_path_str_posix}' does not share common root '{root_name}'. Adding as separate root.")
                root_name_alt = path_parts[0]
                if root_name_alt not in tree_dict:
                    tree_dict[root_name_alt] = {}
                add_path_to_dict(tree_dict[root_name_alt], path_parts[1:])


        # --- Helper to recursively format the tree string ---
        output_lines = []
        def format_node(name, node_content, indent, is_last_node):
            # Print current node (directory name)
            prefix = L_BRANCH if is_last_node else T_BRANCH
            output_lines.append(f"{indent}{prefix}{name}/")

            # Prepare indent for children
            child_indent = indent + (INDENT_EMPTY if is_last_node else INDENT_CONT)

            # Combine and sort items (directories first, then files)
            dirs = sorted([k for k in node_content if k != "_files"], key=str.lower)
            files = sorted(node_content.get("_files", []), key=str.lower)
            items = dirs + files
            total_items = len(items)

            for i, item_name in enumerate(items):
                is_last_item = (i == total_items - 1)
                if item_name in dirs:
                    # It's a directory, recurse
                    format_node(item_name, node_content[item_name], child_indent, is_last_item)
                else:
                    # It's a file
                    child_prefix = L_BRANCH if is_last_item else T_BRANCH
                    output_lines.append(f"{child_indent}{child_prefix}{item_name}")

        # --- Format the tree starting from the roots ---
        root_keys = sorted(tree_dict.keys(), key=str.lower)
        total_roots = len(root_keys)
        for i, current_root_name in enumerate(root_keys):
            is_last_root = (i == total_roots - 1)
            # Format the root node itself, passing empty indent
            format_node(current_root_name, tree_dict[current_root_name], "", is_last_root)


        # --- Combine with delimiters ---
        final_string = TREE_START_DELIMITER + "\n" + "\n".join(output_lines) + "\n" + TREE_END_DELIMITER + "\n"
        return final_string
    # --- End Helper ---

    def run(self):
        self.log(
            f"Starting merge process -> {self.output_file} (Format: {self.format_details['name']})")
        if self.include_tree:
            self.log("File hierarchy tree will be included.")

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
            # --- Phase 1: Discover all files ---
            self.log("Scanning files and folders based on input selections...")
            initial_item_count = len(self.items_to_merge)
            files_discovered_in_scan = []

            for item_idx, (item_type, item_path_str, base_path_str) in enumerate(self.items_to_merge):
                if not self.is_running:
                    break
                # --- Start of existing file discovery loop ---
                # self.log(f"Processing selection {item_idx+1}/{initial_item_count}: Type='{item_type}', Path='{item_path_str}', Base='{base_path_str}'") # Verbose
                try:
                    item_path = pathlib.Path(item_path_str).resolve()
                    # Resolve base_path, fallback to item's parent if base_path is empty/None
                    base_path = pathlib.Path(base_path_str).resolve() if base_path_str else item_path.parent.resolve()
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
                                relative_path = item_path.relative_to(base_path)
                                fsize = item_path.stat().st_size
                                files_discovered_in_scan.append((item_path, relative_path, fsize))
                                total_size += fsize
                                encountered_resolved_paths.add(item_path)
                            except ValueError:
                                # If relative_to fails (e.g., different drive on Windows or unexpected structure)
                                # Fallback: Use path relative to its own parent, or just the filename
                                try:
                                    relative_path = item_path.relative_to(item_path.parent)
                                    self.log(f"Warning: Could not make '{item_path}' relative to base '{base_path}'. Using path relative to parent: '{relative_path}'.")
                                except ValueError: # Should not happen unless base_path itself was weird
                                    relative_path = pathlib.Path(item_path.name)
                                    self.log(f"Warning: Could not determine relative path for '{item_path}' against base '{base_path}'. Using filename only: '{relative_path}'.")

                                try:
                                    fsize = item_path.stat().st_size
                                    files_discovered_in_scan.append((item_path, relative_path, fsize))
                                    total_size += fsize
                                    encountered_resolved_paths.add(item_path)
                                except OSError as e_size:
                                    self.log(f"Warning: Could not get size for {item_path}: {e_size}. Using size 0.")
                                    files_discovered_in_scan.append((item_path, relative_path, 0))
                                    encountered_resolved_paths.add(item_path)

                            except OSError as e_stat:
                                self.log(f"Warning: Could not get size for {item_path}: {e_stat}. Using size 0.")
                                # Need relative path even if size fails
                                try:
                                    relative_path_fallback = item_path.relative_to(base_path)
                                except ValueError:
                                    relative_path_fallback = pathlib.Path(item_path.name)
                                files_discovered_in_scan.append((item_path, relative_path_fallback, 0))
                                encountered_resolved_paths.add(item_path)
                            except Exception as e_other:
                                self.log(f"Warning: Unexpected error processing file entry {item_path}: {e_other}")
                        # else: # Verbose
                        #     self.log(f"Skipping duplicate file (already encountered): {item_path}")
                    else:
                        self.log(f"Warning: Selected file not found during scan: {item_path}")

                elif item_type == "folder" or item_type == "folder-root": # Handle both types if needed
                    if item_path.is_dir():
                        # self.log(f"Scanning folder: {item_path} (Base for relative paths: {base_path})") # Verbose
                        # Use item_path itself as the base for relative paths within this specific walk
                        # folder_base_path = item_path # Base for files found INSIDE this folder (Removed, using original base_path)
                        for root, _, filenames in os.walk(str(item_path), followlinks=False):
                            if not self.is_running: break
                            root_path = pathlib.Path(root)
                            for filename in filenames:
                                if not self.is_running: break
                                try:
                                    file_path = (root_path / filename).resolve()
                                    if file_path not in encountered_resolved_paths:
                                        try:
                                            # Calculate relative path based on the ORIGINAL base_path passed for this folder item
                                            relative_path = file_path.relative_to(base_path)
                                            fsize = file_path.stat().st_size
                                            files_discovered_in_scan.append((file_path, relative_path, fsize))
                                            total_size += fsize
                                            encountered_resolved_paths.add(file_path)
                                        except ValueError:
                                            # Fallback: Use path relative to the folder being scanned (item_path)
                                            try:
                                                relative_path_fallback = file_path.relative_to(item_path)
                                                self.log(f"Warning: Could not make '{file_path}' relative to original base '{base_path}'. Using path relative to scanned folder '{item_path}': '{relative_path_fallback}'.")
                                            except ValueError: # Should be rare
                                                relative_path_fallback = pathlib.Path(file_path.name)
                                                self.log(f"Error: Could not even make '{file_path}' relative to its walk root '{item_path}'. Using filename only: '{relative_path_fallback}'.")

                                            try:
                                                fsize = file_path.stat().st_size
                                                files_discovered_in_scan.append((file_path, relative_path_fallback, fsize))
                                                total_size += fsize
                                                encountered_resolved_paths.add(file_path)
                                            except OSError as e_size:
                                                self.log(f"Warning: Could not get size for {file_path}: {e_size}. Using size 0.")
                                                files_discovered_in_scan.append((file_path, relative_path_fallback, 0))
                                                encountered_resolved_paths.add(file_path)
                                        except OSError as e_stat:
                                            self.log(f"Warning: Could not get size for {file_path}: {e_stat}. Using size 0.")
                                            # Need relative path even if size fails
                                            try:
                                                rel_p = file_path.relative_to(base_path)
                                            except ValueError:
                                                # Fallback to relative to item_path, then filename only
                                                try:
                                                    rel_p = file_path.relative_to(item_path)
                                                except ValueError:
                                                     rel_p = pathlib.Path(file_path.name)
                                            files_discovered_in_scan.append((file_path, rel_p, 0))
                                            encountered_resolved_paths.add(file_path)
                                except OSError as e_resolve:
                                    self.log(f"Warning: Could not resolve or access path under {root_path} for filename '{filename}': {e_resolve}")
                                except Exception as e:
                                    self.log(f"Warning: Could not process file '{filename}' in folder scan under {root_path}: {e}")
                            if not self.is_running: break # Inner loop check
                        if not self.is_running: break # Outer loop check
                    else:
                        self.log(f"Warning: Selected folder not found during scan: {item_path}")
                # --- End of file discovery loop ---


            if not self.is_running:
                self.log("Merge cancelled during scanning phase.")
                self.signals.finished.emit(
                    False, "Merge cancelled during scan.")
                return

            # Sort the discovered files by relative path for consistent tree/merge order
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

                # --- Write Hierarchy Tree if requested ---
                if self.include_tree:
                    self.log("Generating and writing hierarchy tree...")
                    tree_content = self._generate_hierarchy_tree_string(
                        files_to_process)
                    if tree_content:
                        outfile.write(tree_content)
                        # Add a separator between tree and first file block IF files exist
                        if files_to_process:
                            outfile.write(separator)
                    else:
                        self.log("No files processed, skipping tree writing.")
                # --- End Tree Writing ---

                # --- Write file content blocks ---
                total_files_count = len(files_to_process)
                for i, (absolute_path, relative_path, fsize) in enumerate(files_to_process):
                    if not self.is_running:
                        break

                    relative_path_str = relative_path.as_posix() # Use posix path in delimiters
                    # self.log(f"Merging ({i+1}/{total_files_count}): '{relative_path_str}' (from: {absolute_path})") # Verbose

                    # --- Format Application ---
                    start_delimiter = start_fmt.format(
                        filepath=relative_path_str)
                    # Assume end might need it too (handle potential KeyError if not needed by format)
                    try:
                        end_delimiter = end_fmt.format(filepath=relative_path_str)
                    except KeyError: # If end_fmt doesn't use {filepath}
                         end_delimiter = end_fmt

                    outfile.write(start_delimiter + "\n")
                    if content_prefix:
                        # Handle potential newlines in prefix based on format needs
                        outfile.write(content_prefix) # Assuming prefix includes \n if needed

                    file_content = ""
                    try:
                        try:
                            # Read as binary first to detect encoding/handle errors better
                            with open(absolute_path, "rb") as infile_b:
                                file_bytes = infile_b.read()
                            # Try decoding as UTF-8
                            try:
                                file_content = file_bytes.decode('utf-8')
                            except UnicodeDecodeError:
                                self.log(f"Warning: Non-UTF-8 file detected: '{relative_path_str}'. Attempting 'latin-1' decode.")
                                try:
                                    file_content = file_bytes.decode('latin-1')
                                except Exception as e_latin:
                                     self.log(f"Warning: Failed to decode '{relative_path_str}' as latin-1: {e_latin}. Using lossy UTF-8.")
                                     file_content = file_bytes.decode('utf-8', errors='replace')

                        except Exception as e_read:
                            self.log(f"Error reading file '{absolute_path}': {e_read}. Inserting error message.")
                            file_content = f"Error reading file: {e_read}"

                        outfile.write(file_content)
                        # Add newline before suffix/end_delimiter if content exists and doesn't end with one
                        if file_content and not file_content.endswith('\n'):
                             outfile.write("\n")


                    except Exception as e_outer:
                        self.log(f"Critical error processing file content for {absolute_path}: {e_outer}\n{traceback.format_exc()}")
                        outfile.write(f"\nError processing file content: {e_outer}\n")

                    # Handle potential newlines in suffix based on format needs
                    if content_suffix:
                        outfile.write(content_suffix) # Assuming suffix includes \n if needed
                    outfile.write(end_delimiter + "\n")

                    # Write separator EXCEPT after the last file
                    if i < total_files_count - 1:
                        outfile.write(separator)
                    # --- End Format Application ---

                    # --- Progress Update ---
                    processed_size += fsize
                    processed_files_count += 1
                    if total_size > 0:
                        progress_percent = int((processed_size / total_size) * 100)
                    elif total_files_count > 0:
                        progress_percent = int((processed_files_count / total_files_count) * 100)
                    else:
                        progress_percent = 0
                    self.signals.progress.emit(min(progress_percent, 100))
                # --- End File Content Blocks ---

            # --- Final checks and signals ---
            if not self.is_running:
                self.log("Merge cancelled during writing phase.")
                try:
                    if output_file_path.exists():
                        output_file_path.unlink()
                    self.log(f"Removed incomplete file: {output_file_path}")
                except OSError as e:
                    self.log(f"Could not remove incomplete file '{output_file_path}': {e}")
                self.signals.finished.emit(False, "Merge cancelled.")
            else:
                self.signals.progress.emit(100)
                self.log("Merge process completed successfully.")
                self.signals.finished.emit(True, f"Merge successful! {len(files_to_process)} files merged into '{output_file_path.name}'.")

        except Exception as e:
            self.log(f"An unexpected error occurred during merge: {e}\n{traceback.format_exc()}")
            self.signals.error.emit(f"Merge failed: {e}")
            self.signals.finished.emit(False, f"Merge failed due to unexpected error: {e}")
        finally:
            # Ensure progress hits 100 if not cancelled mid-way
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
        # self.log(f"Splitter received file: {merged_file}")
        # self.log(f"Using split format: {self.format_details.get('name', 'Unknown')}")

    def stop(self):
        # self.log("Stop signal received by SplitterWorker.")
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
                skip_line_after_start = self.format_details.get("skip_line_after_start", False)
                # Optional: Does the start regex line itself contain content? (Used rarely)
                # skip_start_line_in_content = self.format_details.get("skip_start_line_in_content", False)
            except KeyError as e:
                raise ValueError(f"Split format '{self.format_details.get('name')}' is missing required key: {e}")
            except re.error as e_re:
                raise ValueError(f"Invalid start_regex_pattern in format '{self.format_details.get('name')}': {e_re}")

            merged_file_path = pathlib.Path(self.merged_file)
            if not merged_file_path.is_file():
                raise FileNotFoundError(f"Input file not found: {self.merged_file}")

            total_size = merged_file_path.stat().st_size
            processed_size = 0
            file_count = 0
            created_file_paths = set() # Track files actually created for potential cleanup
            tree_skipped_bytes = 0  # Track bytes skipped for progress accuracy

            # Use binary read mode first to handle seeking precisely
            with open(merged_file_path, "rb") as infile_b:

                # --- Skip Hierarchy Tree Section (if present) ---
                self.log("Checking for file hierarchy tree...")
                # Read first line carefully, handling different line endings
                first_line_bytes = b""
                while True:
                    b = infile_b.read(1)
                    if not b or b == b'\n':
                        break
                    first_line_bytes += b
                # Read the newline character itself if present
                if b == b'\n':
                    first_line_bytes += b

                processed_size += len(first_line_bytes) # Update progress

                # Decode first line for comparison
                try:
                    # Try UTF-8 first, strip whitespace for comparison
                    first_line_str = first_line_bytes.decode('utf-8').strip()
                except UnicodeDecodeError:
                    try:
                        # Fallback to latin-1 if UTF-8 fails
                        first_line_str = first_line_bytes.decode('latin-1').strip()
                    except Exception:
                        # If even latin-1 fails, treat as non-match (highly unlikely)
                        first_line_str = None


                if first_line_str == TREE_START_DELIMITER:
                    self.log(f"Found '{TREE_START_DELIMITER}'. Skipping tree section...")
                    tree_lines_skipped = 1
                    skipped_successfully = False
                    while self.is_running:
                        # Read line by line in binary mode
                        line_bytes = b""
                        while True:
                             b = infile_b.read(1)
                             if not b or b == b'\n':
                                 break
                             line_bytes += b
                        if b == b'\n':
                            line_bytes += b

                        if not line_bytes:  # EOF reached before end delimiter
                            self.log(f"Warning: Reached end of file while skipping tree. Expected '{TREE_END_DELIMITER}'.")
                            break

                        processed_size += len(line_bytes)
                        tree_lines_skipped += 1

                        # Decode for comparison
                        try:
                            line_str = line_bytes.decode('utf-8').strip()
                        except UnicodeDecodeError:
                             try:
                                line_str = line_bytes.decode('latin-1').strip() # Fallback
                             except Exception:
                                line_str = None # Treat as non-match

                        if line_str == TREE_END_DELIMITER:
                            self.log(f"Found '{TREE_END_DELIMITER}'. Skipped {tree_lines_skipped} lines of tree header.")
                            skipped_successfully = True
                            tree_skipped_bytes = processed_size # Store how much we skipped
                            # Update progress after skip
                            if total_size > 0:
                                self.signals.progress.emit(min(int((processed_size / total_size) * 100), 100))
                            break # Exit skip loop
                    # --- End Skip Loop ---

                    if not skipped_successfully and self.is_running:
                        self.log("Warning: Tree section might be incomplete or end delimiter not found. Proceeding with split after scanned lines.")
                    elif not self.is_running:
                        self.log("Split cancelled during tree skipping.")
                        self.signals.finished.emit(False, "Split cancelled.")
                        return # Exit run method

                else:
                    self.log("No hierarchy tree section found at the beginning.")
                    # Go back to the start of the file
                    infile_b.seek(0)
                    processed_size = 0 # Reset processed size

                # Update progress after potential skip/reset
                if total_size > 0:
                     self.signals.progress.emit(min(int((processed_size / total_size) * 100), 100))
                # --- End Tree Skipping ---

                # --- Main Splitting Logic (starts after tree or from beginning) ---
                current_file_path_relative = None
                current_file_content_lines = [] # Store lines (strings) for easier joining later
                in_file_block = False
                just_started_block = False
                line_offset = 0 # Track approximate line number from start of splitting content

                # Now read text line by line from current position using standard text mode methods
                # Reopen might be complex; let's try reading remaining buffer and decoding
                remaining_buffer = infile_b.read() # Read the rest of the file bytes
                if not self.is_running: # Check cancellation after potentially long read
                    self.log("Split cancelled immediately after reading remaining file.")
                    self.signals.finished.emit(False, "Split cancelled.")
                    return

                # Decode the remaining buffer into text
                try:
                    content_text = remaining_buffer.decode('utf-8')
                except UnicodeDecodeError:
                    self.log("Warning: Decoding remaining content as UTF-8 failed. Trying latin-1.")
                    try:
                        content_text = remaining_buffer.decode('latin-1')
                    except Exception as e_decode_rest:
                         self.log(f"ERROR: Failed to decode remaining file content: {e_decode_rest}. Aborting split.")
                         self.signals.error.emit("Failed to decode file content for splitting.")
                         self.signals.finished.emit(False, "Split failed: File decoding error.")
                         return

                # Split the decoded text into lines, keeping line endings
                lines = content_text.splitlines(keepends=True)
                del content_text # Free memory if large file
                del remaining_buffer # Free memory

                # Iterate through decoded lines
                for line in lines:
                    if not self.is_running:
                        break

                    line_offset += 1
                    # Progress update based on bytes processed so far + current line bytes
                    # Estimate bytes from string length (might not be perfect for multi-byte chars, but good enough for progress)
                    current_line_bytes_est = len(line.encode('utf-8', errors='ignore'))
                    processed_size += current_line_bytes_est
                    if total_size > 0:
                        progress_percent = int((processed_size / total_size) * 100)
                        self.signals.progress.emit(min(progress_percent, 100))

                    line_stripped = line.strip() # Use stripped line for logic checks

                    # --- State Machine for Parsing ---
                    if not in_file_block:
                        start_match = start_regex.match(line_stripped) # Match against stripped line
                        if start_match:
                            try:
                                potential_relative_path = start_match.group(1).strip()
                            except IndexError:
                                self.log(f"Warning: Regex '{start_regex_pattern}' matched approx line {line_offset} but captured no path group. Skipping block.")
                                continue

                            # --- Basic Path Safety Check ---
                            # Use posix paths internally for checks
                            normalized_path_check = potential_relative_path.replace("\\", "/")
                            is_safe = True
                            if not potential_relative_path:
                                self.log(f"Warning: Empty filepath captured by start regex approx line {line_offset}. Skipping block.")
                                is_safe = False
                            elif pathlib.PurePath(potential_relative_path).is_absolute(): # Use PurePath for checks
                                self.log(f"Error: Security risk! Absolute path found in delimiter: '{potential_relative_path}' approx line {line_offset}. Skipping block.")
                                is_safe = False
                            # Check for potential traversal AFTER normalization
                            elif "../" in normalized_path_check or normalized_path_check.startswith("/"):
                                self.log(f"Warning: Potential path traversal or absolute-like path detected in delimiter: '{potential_relative_path}' near line {line_offset}. Final check during write.")
                                # Let _write_file handle final check robustly

                            if not is_safe:
                                continue # Skip this unsafe block

                            current_file_path_relative = potential_relative_path
                            current_file_content_lines = [] # Reset content list
                            in_file_block = True
                            # Determine if the line *after* the start delimiter line needs skipping
                            just_started_block = skip_line_after_start
                            # self.log(f"Found block start: '{current_file_path_relative}' (Approx line {line_offset})") # Verbose
                            continue # Move to next line, don't add start delimiter to content

                    else: # in_file_block is True
                        if just_started_block:
                            # This line immediately follows the start delimiter line
                            just_started_block = False
                            # self.log(f"Skipping line after start for {current_file_path_relative}") # Verbose
                            continue # Skip this line as per format config

                        # Get expected end delimiter (might depend on filepath)
                        expected_end_delimiter = get_end_delimiter_func(current_file_path_relative)

                        # Check if the *stripped* current line matches the expected end delimiter
                        if line_stripped == expected_end_delimiter:
                            # self.log(f"Found block end for: '{current_file_path_relative}' (Approx line {line_offset})") # Verbose
                            # Join the collected lines (which include original newlines)
                            content_to_write = "".join(current_file_content_lines)
                            if self._write_file(current_file_path_relative, content_to_write):
                                file_count += 1
                                # Add successfully written file path for potential cleanup on cancel
                                created_file_paths.add(self.output_dir.joinpath(current_file_path_relative))
                            # Reset state for next block
                            in_file_block = False
                            current_file_path_relative = None
                            current_file_content_lines = []
                            just_started_block = False
                            continue # Don't include end delimiter line in any content
                        else:
                            # Add the original line (with its newline) to the content list
                            current_file_content_lines.append(line)
                # --- End Loop Through Lines ---

            # --- Handle Loop Finish (after binary file processing) ---
            if not self.is_running:
                self.log("Split cancelled during file processing.")
                # Optionally remove partially created files if desired
                self.log(f"Attempting cleanup of {len(created_file_paths)} created files...")
                cleaned_count = 0
                for f_path in created_file_paths:
                    try:
                        f_path_resolved = f_path.resolve() # Resolve before unlinking
                        if f_path_resolved.is_file():
                             f_path_resolved.unlink()
                             cleaned_count += 1
                    except OSError as e_unlink:
                        self.log(f"Warning: Could not remove partially created file '{f_path}': {e_unlink}")
                    except Exception as e_clean:
                         self.log(f"Warning: Error during cleanup of '{f_path}': {e_clean}")
                self.log(f"Cleanup finished. Removed {cleaned_count} files.")
                self.signals.finished.emit(False, "Split cancelled.")
                return # Exit run method

            # Check if we were in a block when the file ended
            if in_file_block and current_file_path_relative:
                self.log(f"Warning: Merged file ended before finding END delimiter for '{current_file_path_relative}'. Saving remaining content.")
                content_to_write = "".join(current_file_content_lines)
                if self._write_file(current_file_path_relative, content_to_write):
                    file_count += 1
                    # No need to add to created_file_paths set here if not cleaning up partials on cancel

            # --- Post-processing ---
            self.signals.progress.emit(100)
            if file_count > 0:
                final_message = f"Split successful! {file_count} files created in '{self.output_dir.name}' (Format: {self.format_details['name']})."
                self.log(final_message)
                self.signals.finished.emit(True, final_message)
            elif not self.is_running:
                pass # Finished signal already emitted during cancel
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
            self.log("Error: Attempted to write file with empty relative path. Skipping.")
            return False

        # Clean the relative path string (replace backslashes, remove leading/trailing slashes/dots)
        # Replace backslashes first
        cleaned_relative_path = relative_path_str.replace("\\", "/")
        # Remove leading/trailing slashes and dots that might cause issues
        cleaned_relative_path = cleaned_relative_path.strip("./ ")

        if not cleaned_relative_path:
             self.log(f"Error: Relative path '{relative_path_str}' became empty after cleaning. Skipping.")
             return False

        target_path_resolved = None # Define outside try for logging in except
        try:
            # Combine with output dir using Path objects
            target_path = self.output_dir.joinpath(cleaned_relative_path)

            # --- Final Safety Check ---
            # Resolve the target path *without* creating it (strict=False)
            # Resolve the output directory *requiring* it exists (strict=True)
            try:
                # Check output dir exists and is writable BEFORE resolving target
                if not self.output_dir.is_dir() or not os.access(str(self.output_dir), os.W_OK):
                     raise FileNotFoundError(f"Output directory '{self.output_dir}' is not accessible or writable.")

                output_dir_resolved = self.output_dir.resolve(strict=True)
                # Resolve target, but don't require it to exist yet
                target_path_resolved = target_path.resolve(strict=False)

            except (OSError, ValueError) as e_resolve:
                # ValueError can happen on Windows with invalid chars like ':'
                self.log(f"Error: Invalid path generated for '{cleaned_relative_path}': {e_resolve}. Skipping write.")
                return False
            except FileNotFoundError as e_fnf:
                # This happens if self.output_dir itself doesn't exist or isn't writable
                self.log(f"Error: {e_fnf}. Cannot write '{cleaned_relative_path}'.")
                if self.is_running: # Only emit error if not already cancelled
                     self.signals.error.emit(f"Output directory issue: {e_fnf}")
                     self.is_running = False # Stop further processing as output dir is bad
                return False

            # Check if the resolved target path is truly inside the resolved output directory
            # This prevents "../.." tricks etc. that might bypass simple string checks
            is_within_output_dir = False
            try:
                # Check if output_dir_resolved is one of the parents of target_path_resolved OR the path itself
                is_within_output_dir = output_dir_resolved == target_path_resolved or \
                                        output_dir_resolved in target_path_resolved.parents
            except Exception as path_comp_err: # Catch potential errors during comparison
                self.log(f"Warning: Could not perform robust path comparison for '{target_path_resolved}': {path_comp_err}")
                # Less robust fallback: string comparison (use resolved paths)
                # Ensure consistent path separators for string comparison
                output_dir_str = str(output_dir_resolved).replace("\\", "/")
                target_path_str = str(target_path_resolved).replace("\\", "/")
                if target_path_str.startswith(output_dir_str + "/"):
                    is_within_output_dir = True

            if not is_within_output_dir:
                self.log(f"Error: Security risk! Path '{cleaned_relative_path}' resolved to '{target_path_resolved}', "
                         f"which is outside the designated output directory '{output_dir_resolved}'. Skipping write.")
                return False

            # If safe, create parent directories and write
            # self.log(f"Attempting to create file: {target_path_resolved}") # Verbose
            target_path_resolved.parent.mkdir(parents=True, exist_ok=True)

            # Write using binary mode to ensure consistent line endings from source
            with open(target_path_resolved, "wb") as outfile:
                 # Encode content back to UTF-8 (or preferred encoding) for writing
                 try:
                     outfile.write(content.encode('utf-8'))
                 except UnicodeEncodeError: # Should be rare if read/decoded correctly
                     self.log(f"Warning: Could not encode content for '{target_path_resolved}' as UTF-8. Using latin-1.")
                     outfile.write(content.encode('latin-1', errors='replace'))

            # self.log(f"Successfully wrote: {target_path_resolved}") # Verbose
            return True

        except OSError as e:
            log_path_str = str(target_path_resolved) if target_path_resolved else f"(Failed resolving {cleaned_relative_path})"
            self.log(f"Error writing file '{log_path_str}' (OS Error): {e}")
            # Don't stop the whole process for one file write error, just log it.
            return False
        except Exception as e:
            log_path_str = str(target_path_resolved) if target_path_resolved else f"(Failed resolving {cleaned_relative_path})"
            self.log(f"Error writing file for relative path '{cleaned_relative_path}' (Resolved: {log_path_str}) (General Error): {e}\n{traceback.format_exc()}")
            # Don't stop the whole process for one file write error, just log it.
            return False