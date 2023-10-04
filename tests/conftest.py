import asyncio
from os import environ
from pathlib import Path

import pytest
from devtools import debug


environ["PYTHONASYNCIODEBUG"] = "1"


def check_basetemp(tpath: Path):
    """
    Check that root temporary directory contains `pytest` in its name.

    This is necessary because some tests use the directory name as a
    discriminant to set and test permissions.
    """
    if "pytest" not in tpath.as_posix():
        raise ValueError(
            f"`basetemp` must contain `pytest` in its name. Got {tpath.parent}"
        )


def pytest_configure(config):
    """
    See https://docs.pytest.org/en/stable/how-to/mark.html#registering-marks
    """
    config.addinivalue_line("markers", "slow: marks tests as slow")


@pytest.fixture(scope="session")
def event_loop():
    debug("event_loop")
    _event_loop = asyncio.new_event_loop()
    _event_loop.set_debug(True)

    yield _event_loop
    debug("FINE event_loop")


@pytest.fixture(scope="session")
async def testdata_path() -> Path:
    debug("testdata_path")
    TEST_DIR = Path(__file__).parent
    return TEST_DIR / "data/"
    debug("testdata_path")


@pytest.fixture(scope="session")
def tmp777_session_path(tmp_path_factory):
    """
    Makes a subdir of the tmp_path with 777 access rights
    """
    debug("tmp777_session_path")
    def _tmp_path_factory(relative_path: str):
        tmp = tmp_path_factory.mktemp(relative_path)
        tmp.chmod(0o777)
        check_basetemp(tmp.parent)
        return tmp

    yield _tmp_path_factory
    debug("FINE tmp777_session_path")

    

@pytest.fixture(scope="session")
def monkeysession(tmp777_session_path):
    debug("monkeysession")
    import fractal_server
    tmp_path = tmp777_session_path("server_folder")    
    patched_settings = get_patched_settings(tmp_path)
    
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("fractal_server.get_settings.get_settings", lambda: patched_settings)
        mp.setattr("fractal_server.app.db.get_settings", lambda: patched_settings)
        mp.setattr("fractal_server.app.db.set_logger", lambda: "DIOBASTARDO")
        yield mp
    debug("FINE monkeysession")



def check_python_has_venv(python_path: str, temp_path: Path):
    """
    This function checks that we can safely use a certain python interpreter,
    namely
    1. It exists;
    2. It has the venv module installed.
    """

    import subprocess
    import shlex

    temp_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path.parent.chmod(0o755)
    temp_path.mkdir(parents=True, exist_ok=True)
    temp_path.chmod(0o755)

    cmd = f"{python_path} -m venv {temp_path.as_posix()}"
    p = subprocess.run(
        shlex.split(cmd),
        capture_output=True,
    )
    if p.returncode != 0:
        debug(cmd)
        debug(p.stdout.decode("UTF-8"))
        debug(p.stderr.decode("UTF-8"))
        logging.warning(
            "check_python_has_venv({python_path=}, {temp_path=}) failed."
        )
        raise RuntimeError(
            p.stderr.decode("UTF-8"),
            f"Hint: is the venv module installed for {python_path}? "
            f'Try running "{cmd}".',
        )


def get_patched_settings(tmp_path: Path):
    settings = Settings()
    settings.JWT_SECRET_KEY = "secret_key"

    settings.FRACTAL_DEFAULT_ADMIN_USERNAME = "admin"

    settings.DB_ENGINE = DB_ENGINE
    if DB_ENGINE == "sqlite":
        settings.SQLITE_PATH = f"{tmp_path.as_posix()}/_test.db"
    elif DB_ENGINE == "postgres":
        settings.DB_ENGINE = "postgres"
        settings.POSTGRES_USER = "postgres"
        settings.POSTGRES_PASSWORD = "postgres"
        settings.POSTGRES_DB = "fractal_test"
    else:
        raise ValueError

    settings.FRACTAL_TASKS_DIR = tmp_path / "fractal_tasks_dir"
    settings.FRACTAL_TASKS_DIR.mkdir(parents=True, exist_ok=True)
    settings.FRACTAL_TASKS_DIR.chmod(0o755)
    settings.FRACTAL_RUNNER_WORKING_BASE_DIR = tmp_path / "artifacts"
    settings.FRACTAL_RUNNER_WORKING_BASE_DIR.mkdir(parents=True, exist_ok=True)
    settings.FRACTAL_RUNNER_WORKING_BASE_DIR.chmod(0o755)

    # NOTE:
    # This variable is set to work with the system interpreter within a docker
    # container. If left unset it defaults to `sys.executable`
    if not HAS_LOCAL_SBATCH:
        settings.FRACTAL_SLURM_WORKER_PYTHON = "/usr/bin/python3"
        check_python_has_venv(
            "/usr/bin/python3", tmp_path / "check_python_has_venv"
        )

    settings.FRACTAL_SLURM_CONFIG_FILE = tmp_path / "slurm_config.json"

    settings.FRACTAL_SLURM_POLL_INTERVAL = 4
    settings.FRACTAL_SLURM_KILLWAIT_INTERVAL = 4
    settings.FRACTAL_SLURM_OUTPUT_FILE_GRACE_TIME = 1

    settings.FRACTAL_LOGGING_LEVEL = logging.DEBUG
    
    return settings
    

@pytest.fixture
def tmp777_path(tmp_path):
    debug("tmp777_path")
    check_basetemp(tmp_path)
    tmp_path.chmod(0o777)
    for parent in tmp_path.parents:
        if "pytest" in parent.as_posix():
            parent.chmod(0o777)
    yield tmp_path
    debug("FINE tmp777_path")


from .fixtures_server import *  # noqa F403
# from .fixtures_tasks import *  # noqa F403
# from .fixtures_slurm import *  # noqa F403
