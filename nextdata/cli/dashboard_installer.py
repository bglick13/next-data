import subprocess
import shutil
from pathlib import Path
import platform


class DashboardInstaller:
    def __init__(self):
        self.dashboard_dir = Path(__file__).parent.parent / "dashboard"
        self.installation_marker = self.dashboard_dir / ".installed"

    def ensure_pnpm(self):
        """Ensure pnpm is installed"""
        try:
            subprocess.run(["pnpm", "--version"], check=True, capture_output=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("Installing pnpm...")
            if platform.system() == "Windows":
                subprocess.run(["npm", "install", "-g", "pnpm"], check=True)
            else:
                subprocess.run(
                    ["curl", "-fsSL", "https://get.pnpm.io/install.sh | sh -"],
                    check=True,
                    shell=True,
                )

    def is_installed(self):
        """Check if dashboard is already installed"""
        return self.installation_marker.exists()

    def install(self, force=False):
        """Install dashboard dependencies"""
        print(f"Installing dashboard dependencies in {self.dashboard_dir}")
        if self.is_installed() and not force:
            return

        print("Installing dashboard dependencies...")
        self.ensure_pnpm()

        subprocess.run(["pnpm", "install"], cwd=self.dashboard_dir, check=True)

        # Create marker file
        self.installation_marker.touch()

    def clean(self):
        """Clean dashboard installation"""
        node_modules = self.dashboard_dir / "node_modules"
        if node_modules.exists():
            shutil.rmtree(node_modules)
        if self.installation_marker.exists():
            self.installation_marker.unlink()
