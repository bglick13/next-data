from nextdata.core.db.db_manager import DatabaseManager
from nextdata.core.project_config import NextDataConfig


def get_db_dependency() -> DatabaseManager:
    config = NextDataConfig.from_env()
    db_path = config.project_dir / "nextdata.db"
    return DatabaseManager(db_path)
