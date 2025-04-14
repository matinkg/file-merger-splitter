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
            # --- Phase 1: Discover all files ---
            self.log("Scanning files and folders based on input selections...")
            initial_item_count = len(self.items_to_merge)
            files_discovered_in_scan = []

            for item_idx, (item_type, item_path_str, base_path_str) in enumerate(self.items_to_merge):
                if not self.is_running:
                    break
                # ... (rest of file discovery logic is identical to original) ...
                # --- Start of existing file discovery loop ---
                # self.log(f"Processing selection {item_idx+1}/{initial_item_count}: Type='{item_type}', Path='{item_path_str}', Base='{base_path_str}'") # Verbose
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
                                try:
                                    relative_path_fallback = item_path.relative_to(
                                        base_path)
                                except ValueError:
                                    relative_path_fallback = pathlib.Path(
                                        item_path.name)  # Fallback if relative fails
                                files_discovered_in_scan.append(
                                    (item_path, relative_path_fallback, 0))
                                encountered_resolved_paths.add(item_path)
                            except Exception as e:
                                self.log(
                                    f"Warning: Unexpected error processing file entry {item_path}: {e}")
                        # else:
                            # self.log(f"Skipping duplicate file (already encountered): {item_path}") # Verbose
                    else:
                        self.log(
                            f"Warning: Selected file not found during scan: {item_path}")

                elif item_type == "folder":
                    if item_path.is_dir():
                        # self.log(f"Scanning folder: {item_path} (Base for relative paths: {base_path})") # Verbose
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
                                            # self.log(f"Warning: Could not make '{file_path}' relative to base '{base_path}'. Using path relative to scanned folder '{item_path}'.") # Verbose
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
                                        except OSError as e:
                                            self.log(
                                                f"Warning: Could not get size for {file_path}: {e}. Using size 0.")
                                            try:
                                                rel_p = file_path.relative_to(
                                                    base_path)
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
                # --- End of file discovery loop ---

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
                    # self.log(f"Merging ({i+1}/{total_files_count}): '{relative_path_str}' (from: {absolute_path})") # Verbose

                    # --- Format Application ---
                    start_delimiter = start_fmt.format(
                        filepath=relative_path_str)
                    # Assume end might need it too
                    end_delimiter = end_fmt.format(filepath=relative_path_str)

                    outfile.write(start_delimiter + "\n")
                    if content_prefix:
                        outfile.write(content_prefix)

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
                        if file_content and not file_content.endswith('\n'):
                            if content_suffix or end_delimiter:
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
                    # --- End Format Application ---

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

                    line_stripped = line.strip()  # Check stripped line for delimiters

                    # --- State Machine for Parsing ---
                    if not in_file_block:
                        start_match = start_regex.match(line_stripped)
                        if start_match:
                            try:
                                potential_relative_path = start_match.group(
                                    1).strip()
                            except IndexError:
                                self.log(
                                    f"Warning: Regex '{start_regex_pattern}' matched line {line_num+1} but captured no path group. Skipping block.")
                                continue

                            # --- Basic Path Safety Check ---
                            normalized_path_check = potential_relative_path.replace(
                                "\\", "/")
                            is_safe = True
                            if not potential_relative_path:
                                self.log(
                                    f"Warning: Empty filepath captured by start regex on line {line_num+1}. Skipping block.")
                                is_safe = False
                            elif pathlib.Path(potential_relative_path).is_absolute():
                                self.log(
                                    f"Error: Security risk! Absolute path found in delimiter: '{potential_relative_path}' near line {line_num+1}. Skipping block.")
                                is_safe = False
                            # elif normalized_path_check.startswith("../") or "/../" in normalized_path_check:
                            #     self.log(f"Warning: Potential path traversal detected in delimiter: '{potential_relative_path}' near line {line_num+1}. Final check during write.")
                            #     # Let _write_file handle final check

                            if not is_safe:
                                continue

                            current_file_path_relative = potential_relative_path
                            current_file_content = []
                            in_file_block = True
                            just_started_block = skip_line_after_start
                            # self.log(f"Found block start: '{current_file_path_relative}' (Line {line_num+1})") # Verbose
                            continue  # Move to next line

                    else:  # in_file_block is True
                        if just_started_block:
                            just_started_block = False
                            continue  # Skip this line

                        expected_end_delimiter = get_end_delimiter_func(
                            current_file_path_relative)

                        if line_stripped == expected_end_delimiter:
                            # self.log(f"Found block end for: '{current_file_path_relative}' (Line {line_num+1})") # Verbose
                            if self._write_file(current_file_path_relative, "".join(current_file_content)):
                                file_count += 1
                                created_file_paths.add(
                                    self.output_dir.joinpath(current_file_path_relative))
                            in_file_block = False
                            current_file_path_relative = None
                            current_file_content = []
                            just_started_block = False
                            continue  # Don't include end delimiter line
                        else:
                            # Append original line with newline
                            current_file_content.append(line)

                # --- Loop finished ---
                if not self.is_running:
                    self.log("Split cancelled during file processing.")
                    self.signals.finished.emit(False, "Split cancelled.")
                    # TODO: Consider removing partially created files if desired
                    return

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
                final_message = f"Split successful! {file_count} files created in '{self.output_dir.name}' (Format: {self.format_details['name']})."
                self.log(final_message)
                self.signals.finished.emit(True, final_message)
            elif not self.is_running:
                pass  # Finished signal already emitted during cancel
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
        target_path_resolved = None  # Define outside try for logging in except
        try:
            target_path = self.output_dir / pathlib.Path(relative_path_str)

            # --- Final Safety Check ---
            # Resolve the target path *without* creating it (strict=False)
            # Resolve the output directory *requiring* it exists (strict=True)
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
                return False  # Cannot proceed if output base doesn't exist

            # Check if the resolved target path is truly inside the resolved output directory
            # This prevents "../.." tricks etc. that might bypass simple string checks
            is_within_output_dir = False
            try:
                # Path.is_relative_to() is ideal but requires Python 3.9+
                # Fallback: check if output_dir_resolved is one of the parents of target_path_resolved
                is_within_output_dir = output_dir_resolved == target_path_resolved or \
                    output_dir_resolved in target_path_resolved.parents
            except Exception as path_comp_err:
                self.log(
                    f"Warning: Could not perform robust path comparison: {path_comp_err}")
                # Less robust fallback (might fail on symlinks etc.)
                if str(target_path_resolved).startswith(str(output_dir_resolved)):
                    is_within_output_dir = True

            if not is_within_output_dir:
                self.log(f"Error: Security risk! Path '{relative_path_str}' resolved to '{target_path_resolved}', "
                         f"which is outside the designated output directory '{output_dir_resolved}'. Skipping write.")
                return False

            # If safe, create parent directories and write
            # self.log(f"Attempting to create file: {target_path_resolved}") # Verbose
            target_path_resolved.parent.mkdir(parents=True, exist_ok=True)

            with open(target_path_resolved, "w", encoding="utf-8") as outfile:
                outfile.write(content)
            # self.log(f"Successfully wrote: {target_path_resolved}") # Verbose
            return True

        except OSError as e:
            log_path_str = str(
                target_path_resolved) if target_path_resolved else f"(Failed resolving {relative_path_str})"
            self.log(f"Error writing file '{log_path_str}' (OS Error): {e}")
            return False
        except Exception as e:
            log_path_str = str(
                target_path_resolved) if target_path_resolved else f"(Failed resolving {relative_path_str})"
            self.log(
                f"Error writing file for relative path '{relative_path_str}' (Resolved: {log_path_str}) (General Error): {e}\n{traceback.format_exc()}")
            return False
