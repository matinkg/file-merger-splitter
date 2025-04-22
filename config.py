import re
from PyQt6.QtCore import Qt

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
        "start_regex_pattern": r"^```(?!``)(.*)$",
        "get_end_delimiter": lambda fp: "```",
        "skip_start_line_in_content": False,  # The fence line isn't content
        "skip_line_after_start": False,    # Content starts on the next line
    },
    # Add more formats here if needed
}


# --- Data Roles for Tree Items ---
PATH_DATA_ROLE = Qt.ItemDataRole.UserRole + 1  # Stores the full absolute path (str)
# Stores "file", "folder", or "folder-root" (str)
TYPE_DATA_ROLE = Qt.ItemDataRole.UserRole + 2
# Stores the base path for relative calculation (str)
BASE_PATH_DATA_ROLE = Qt.ItemDataRole.UserRole + 3
