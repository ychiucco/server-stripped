from typing import Optional

from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlalchemy.exc import IntegrityError
from sqlmodel import select

from ....config import get_settings
from ....syringe import Inject


from ....logger import close_logger
from ....logger import set_logger
from ...db import AsyncSession
from ...db import get_db
from ...models import Dataset
from ...models import LinkUserProject
from ...models import Project
from ...schemas import ProjectCreate
from ...schemas import ProjectRead
from ...schemas import ProjectUpdate
from ...security import current_active_user
from ...security import User
from ._aux_functions import _check_project_exists
from ._aux_functions import _get_project_check_owner


router = APIRouter()


@router.get("/", response_model=list[ProjectRead])
async def get_list_project(
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> list[Project]:
    """
    Return list of projects user is member of
    """

    from devtools import debug
    settings = get_settings()
    debug(id(settings))
    debug(settings)

    stm = (
        select(Project)
        .join(LinkUserProject)
        .where(LinkUserProject.user_id == user.id)
    )
    res = await db.execute(stm)
    project_list = res.scalars().all()
    await db.close()
    return project_list


@router.post("/", response_model=ProjectRead, status_code=201)
async def create_project(
    project: ProjectCreate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[ProjectRead]:
    """
    Create new poject
    """

    # Check that there is no project with the same user and name
    await _check_project_exists(
        project_name=project.name, user_id=user.id, db=db
    )

    db_project = Project.from_orm(project)
    db_project.dataset_list.append(Dataset(name=project.default_dataset_name))
    db_project.user_list.append(user)
    try:
        db.add(db_project)
        await db.commit()
        await db.refresh(db_project)
        await db.close()
    except IntegrityError as e:
        await db.rollback()
        logger = set_logger("create_project")
        logger.error(str(e))
        close_logger(logger)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(e),
        )

    return db_project


@router.get("/{project_id}", response_model=ProjectRead)
async def read_project(
    project_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[ProjectRead]:
    """
    Return info on an existing project
    """
    project = await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )
    await db.close()
    return project


@router.patch("/{project_id}", response_model=ProjectRead)
async def update_project(
    project_id: int,
    project_update: ProjectUpdate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
):
    project = await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )

    # Check that there is no project with the same user and name
    if project_update.name is not None:
        await _check_project_exists(
            project_name=project_update.name, user_id=user.id, db=db
        )

    for key, value in project_update.dict(exclude_unset=True).items():
        setattr(project, key, value)

    await db.commit()
    await db.refresh(project)
    await db.close()
    return project


@router.delete("/{project_id}", status_code=204)
async def delete_project(
    project_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Response:
    """
    Delete project
    """
    project = await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )
    await db.delete(project)
    await db.commit()
    await db.close()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
