"""
Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
University of Zurich

Original authors:
Jacopo Nespolo <jacopo.nespolo@exact-lab.it>

This file is part of Fractal and was originally developed by eXact lab S.r.l.
<exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
Institute for Biomedical Research and Pelkmans Lab from the University of
Zurich.
"""
import logging
import random
import shutil
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Any
from typing import AsyncGenerator
from typing import Dict
from typing import List
from typing import Optional

import pytest
from asgi_lifespan import LifespanManager
from devtools import debug
from fastapi import FastAPI
from httpx import AsyncClient
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from fractal_server.app.db import get_db
from fractal_server.main import _create_first_user
from fractal_server.config import Settings

try:
    import asyncpg  # noqa: F401

    DB_ENGINE = "postgres"
except ModuleNotFoundError:
    DB_ENGINE = "sqlite"

HAS_LOCAL_SBATCH = bool(shutil.which("sbatch"))


@pytest.fixture
async def db_create_tables(monkeysession):
    debug("db_create_tables")
    from fractal_server.app.db import DB
    from fractal_server.app.models import SQLModel

    engine = DB.engine_sync()
    metadata = SQLModel.metadata
    metadata.create_all(engine)

    yield

    metadata.drop_all(engine)

    debug("FINE db_create_tables")


@pytest.fixture
async def db(db_create_tables, monkeysession):
    debug("db")
    async for session in get_db():
        yield session
    debug("FINE db")


@pytest.fixture
async def db_sync(db_create_tables, monkeysession):
    debug("db_sync")
    from fractal_server.app.db import get_sync_db

    for session in get_sync_db():
        yield session
    
    debug("FINE db_sync")


@pytest.fixture
async def app(monkeysession, db) -> AsyncGenerator[FastAPI, Any]:
    debug("app")
    app = FastAPI()
    from fractal_server.main import collect_routers
    collect_routers(app)

    yield app
    debug("FINE app")


@pytest.fixture
async def client(app: FastAPI) -> AsyncGenerator[AsyncClient, Any]:
    debug("client")
    async with AsyncClient(
        app=app, base_url="http://test"
    ) as client, LifespanManager(app):
        yield client
    debug("FINE client")


# @pytest.fixture
# async def registered_client(
#     app: FastAPI, register_routers, db
# ) -> AsyncGenerator[AsyncClient, Any]:

#     EMAIL = "test@test.com"
#     PWD = "123"
#     await _create_first_user(email=EMAIL, password=PWD, is_superuser=False)

#     async with AsyncClient(
#         app=app, base_url="http://test"
#     ) as client, LifespanManager(app):
#         data_login = dict(
#             username=EMAIL,
#             password=PWD,
#         )
#         res = await client.post("auth/token/login", data=data_login)
#         token = res.json()["access_token"]
#         client.headers["Authorization"] = f"Bearer {token}"
#         yield client


# @pytest.fixture
# async def registered_superuser_client(
#     app: FastAPI, register_routers, db
# ) -> AsyncGenerator[AsyncClient, Any]:
#     EMAIL = "some-admin@fractal.xy"
#     PWD = "some-admin-password"
#     await _create_first_user(email=EMAIL, password=PWD, is_superuser=True)
#     async with AsyncClient(
#         app=app, base_url="http://test"
#     ) as client, LifespanManager(app):
#         data_login = dict(username=EMAIL, password=PWD)
#         res = await client.post("auth/token/login", data=data_login)
#         token = res.json()["access_token"]
#         client.headers["Authorization"] = f"Bearer {token}"
#         yield client


@pytest.fixture
async def MockCurrentUser(app, db, monkeysession):
    debug("MockCurrentUser")
    from fractal_server.app.security import current_active_user
    from fractal_server.app.security import User

    def _random_email():
        return f"{random.randint(0, 100000000)}@exact-lab.it"

    @dataclass
    class _MockCurrentUser:
        """
        Context managed user override
        """

        name: str = "User Name"
        user_kwargs: Optional[Dict[str, Any]] = None
        scopes: Optional[List[str]] = field(
            default_factory=lambda: ["project"]
        )
        email: Optional[str] = field(default_factory=_random_email)
        persist: Optional[bool] = True

        def _create_user(self):
            defaults = dict(
                email=self.email,
                hashed_password="fake_hashed_password",
                slurm_user="test01",
            )
            if self.user_kwargs:
                defaults.update(self.user_kwargs)
            self.user = User(name=self.name, **defaults)

        def current_active_user_override(self):
            def __current_active_user_override():
                return self.user

            return __current_active_user_override

        async def __aenter__(self):
            self._create_user()

            if self.persist:
                try:
                    db.add(self.user)
                    await db.commit()
                    await db.refresh(self.user)
                except IntegrityError:
                    # Safety net, in case of non-unique email addresses
                    await db.rollback()
                    self.user.email = _random_email()
                    db.add(self.user)
                    await db.commit()
                    await db.refresh(self.user)
                # Removing object from test db session, so that we can operate
                # on user from other sessions
                db.expunge(self.user)
            self.previous_user = app.dependency_overrides.get(
                current_active_user, None
            )
            app.dependency_overrides[
                current_active_user
            ] = self.current_active_user_override()
            return self.user

        async def __aexit__(self, *args, **kwargs):
            if self.previous_user:
                app.dependency_overrides[
                    current_active_user
                ] = self.previous_user
    debug("FINE MockCurrentUser")
    return _MockCurrentUser


# @pytest.fixture
# async def project_factory(db):
#     """
#     Factory that adds a project to the database
#     """
#     from fractal_server.app.models import Project

#     async def __project_factory(user, **kwargs):
#         defaults = dict(name="project")
#         defaults.update(kwargs)
#         project = Project(**defaults)
#         project.user_list.append(user)
#         db.add(project)
#         await db.commit()
#         await db.refresh(project)
#         return project

#     return __project_factory


# @pytest.fixture
# async def dataset_factory(db: AsyncSession):
#     """
#     Insert dataset in db
#     """
#     from fractal_server.app.models import Dataset

#     async def __dataset_factory(db: AsyncSession = db, **kwargs):
#         defaults = dict(
#             name="My Dataset",
#             project_id=1,
#         )
#         args = dict(**defaults)
#         args.update(kwargs)
#         _dataset = Dataset(**args)
#         db.add(_dataset)
#         await db.commit()
#         await db.refresh(_dataset)
#         return _dataset

#     return __dataset_factory


# @pytest.fixture
# async def resource_factory(db, testdata_path):
#     from fractal_server.app.models import Dataset, Resource

#     async def __resource_factory(dataset: Dataset, **kwargs):
#         """
#         Add a new resorce to dataset
#         """
#         defaults = dict(path=(testdata_path / "png").as_posix())
#         defaults.update(kwargs)
#         resource = Resource(dataset_id=dataset.id, **defaults)
#         db.add(resource)
#         await db.commit()
#         await db.refresh(dataset)
#         return dataset.resource_list[-1]

#     return __resource_factory


# @pytest.fixture
# async def task_factory(db: AsyncSession):
#     """
#     Insert task in db
#     """
#     from fractal_server.app.models import Task

#     async def __task_factory(db: AsyncSession = db, index: int = 0, **kwargs):
#         defaults = dict(
#             name=f"task{index}",
#             input_type="zarr",
#             output_type="zarr",
#             command="cmd",
#             source="source",
#         )
#         args = dict(**defaults)
#         args.update(kwargs)
#         t = Task(**args)
#         db.add(t)
#         await db.commit()
#         await db.refresh(t)
#         return t

#     return __task_factory


# @pytest.fixture
# async def job_factory(db: AsyncSession):
#     """
#     Insert job in db
#     """
#     from fractal_server.app.models import ApplyWorkflow
#     from fractal_server.app.models import Workflow
#     from fractal_server.app.runner.common import set_start_and_last_task_index

#     async def __job_factory(
#         working_dir: Path, db: AsyncSession = db, **kwargs
#     ):
#         defaults = dict(
#             project_id=1,
#             input_dataset_id=1,
#             output_dataset_id=2,
#             workflow_id=1,
#             worker_init="WORKER_INIT string",
#             working_dir=working_dir,
#         )
#         args = dict(**defaults)
#         args.update(kwargs)

#         wf = await db.get(Workflow, args["workflow_id"])

#         num_tasks = len(wf.task_list)
#         first_task_index, last_task_index = set_start_and_last_task_index(
#             num_tasks,
#             args.get("first_task_index", None),
#             args.get("last_task_index", None),
#         )
#         args["first_task_index"] = first_task_index
#         args["last_task_index"] = last_task_index

#         if "workflow_dump" not in args:
#             args["workflow_dump"] = dict(
#                 wf.dict(exclude={"task_list"}),
#                 task_list=[
#                     dict(
#                         wf_task.task.dict(exclude={"task"}),
#                         task=wf_task.dict(),
#                     )
#                     for wf_task in wf.task_list
#                 ],
#             )

#         j = ApplyWorkflow(**args)
#         db.add(j)
#         await db.commit()
#         await db.refresh(j)
#         return j

#     return __job_factory


# @pytest.fixture
# async def workflow_factory(db: AsyncSession):
#     """
#     Insert workflow in db
#     """
#     from fractal_server.app.models import Workflow

#     async def __workflow_factory(db: AsyncSession = db, **kwargs):
#         defaults = dict(
#             name="my workflow",
#             project_id=1,
#         )
#         args = dict(**defaults)
#         args.update(kwargs)
#         w = Workflow(**args)
#         db.add(w)
#         await db.commit()
#         await db.refresh(w)
#         return w

#     return __workflow_factory


# @pytest.fixture
# async def workflowtask_factory(db: AsyncSession):
#     """
#     Insert workflowtask in db
#     """
#     from fractal_server.app.models import WorkflowTask

#     async def __workflowtask_factory(
#         workflow_id: int, task_id: int, db: AsyncSession = db, **kwargs
#     ):
#         defaults = dict(
#             workflow_id=workflow_id,
#             task_id=task_id,
#         )
#         args = dict(**defaults)
#         args.update(kwargs)
#         wft = WorkflowTask(**args)
#         db.add(wft)
#         await db.commit()
#         await db.refresh(wft)
#         return wft

#     return __workflowtask_factory
