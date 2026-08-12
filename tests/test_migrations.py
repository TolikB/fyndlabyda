from alembic.config import Config
from alembic.script import ScriptDirectory


def test_alembic_revision_ids_fit_version_table() -> None:
    scripts = ScriptDirectory.from_config(Config("alembic.ini"))

    assert all(len(revision.revision) <= 32 for revision in scripts.walk_revisions())
    assert scripts.get_current_head() == "0009_live_execution"
