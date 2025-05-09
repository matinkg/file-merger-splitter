from ui_main_window import MergerSplitterApp
import sys
import os
from PyQt6.QtWidgets import QApplication

# Set environment variables for HiDPI scaling if needed
os.environ["QT_ENABLE_HIGHDPI_SCALING"] = "1"
# Helps with non-integer scale factors (e.g., 150% on Windows)
os.environ["QT_SCALE_FACTOR_ROUNDING_POLICY"] = "PassThrough"
# Optional Qt Attributes (usually covered by env vars above)
# QApplication.setAttribute(Qt.ApplicationAttribute.AA_EnableHighDpiScaling, True)
# QApplication.setAttribute(Qt.ApplicationAttribute.AA_UseHighDpiPixmaps, True)

# --- Main Execution ---
if __name__ == '__main__':
    try:
        # Create the Qt Application
        app = QApplication(sys.argv)

        # Set application details (optional but good practice)
        app.setApplicationName("FileMergerSplitter")
        app.setOrganizationName("UtilityApps")
        app.setApplicationVersion("1.2")  # Keep version updated

        # Create and show the main window
        main_window = MergerSplitterApp()
        main_window.show()

        # Start the Qt event loop
        sys.exit(app.exec())
    except Exception as e:
        # Handle any unexpected errors during startup
        from PyQt6.QtWidgets import QMessageBox
        error_msg = f"Error starting application: {str(e)}"
        print(error_msg)  # Print to console
        app = QApplication.instance() or QApplication(sys.argv)
        QMessageBox.critical(None, "Application Error", error_msg)
        sys.exit(1)
