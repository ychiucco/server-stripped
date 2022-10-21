import importlib
import logging
from concurrent.futures import Future
from copy import deepcopy
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Literal
from typing import Optional
from typing import Union

from parsl.app.app import join_app
from parsl.app.python import PythonApp
from parsl.dataflow.dflow import DataFlowKernel
from parsl.dataflow.futures import AppFuture

from ...models.task import PreprocessedTask
from ...models.task import Task
from .._common import async_wrap
from .runner_utils import get_unique_executor
from .runner_utils import load_parsl_config


def _task_fun(
    *,
    task: Task,
    input_paths: List[Path],
    output_path: Path,
    metadata: Optional[Dict[str, Any]],
    task_args: Optional[Dict[str, Any]],
    inputs: Iterable,
):

    # NOTE 1: logging takes place here (in the function, not in the app), so
    # that it is only triggered when the function executes, rather than when
    # the app is defined. The executors argument is only needed for logging, in
    # this function.
    # NOTE 2: for functions that become parsl app, we always need to re-import
    # relevant modules (e.g. logging)

    import logging

    logger = logging.getLogger(__name__)
    formatter = logging.Formatter("%(asctime)s; %(levelname)s; %(message)s")
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    logger.info(f'Start execution of task "{task.name}".')
    task_module = importlib.import_module(task.import_path)
    _callable = getattr(task_module, task.callable)
    metadata_update = _callable(
        input_paths=input_paths,
        output_path=output_path,
        metadata=metadata,
        **task_args,
    )
    logger.info(f'End execution of task "{task.name}".')
    console_handler.flush()

    metadata.update(metadata_update)
    try:
        metadata["history"].append(task.name)
    except KeyError:
        metadata["history"] = [task.name]
    return metadata


def _task_app_future(
    *,
    task: Task,
    input_paths: List[Path],
    output_path: Path,
    metadata: Optional[Dict[str, Any]],
    task_args: Optional[Dict[str, Any]],
    workflow_id: int,
    inputs,
    executors: Union[List[str], Literal["all"]] = "all",
    data_flow_kernel=None,
) -> AppFuture:

    logger = logging.getLogger(f"WF{workflow_id}")
    logger.info(
        f'Defining app future for task "{task.name}", '
        f"to be executed on {executors=}"
    )
    app = PythonApp(
        _task_fun,
        executors=executors,
        data_flow_kernel=data_flow_kernel,
    )
    # TODO: can we reassign app.__name__, for clarity in monitoring?
    return app(
        task=task,
        input_paths=input_paths,
        output_path=output_path,
        metadata=metadata,
        task_args=task_args,
        inputs=inputs,
    )


def _task_component_fun(
    *,
    task: Task,
    component: str,
    input_paths: List[Path],
    output_path: Path,
    metadata: Optional[Dict[str, Any]],
    task_args: Optional[Dict[str, Any]],
):

    import logging

    logger = logging.getLogger(__name__)
    formatter = logging.Formatter("%(asctime)s; %(levelname)s; %(message)s")
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    logger.info(f'Start execution of task "{task.name}" for {component=}.')
    task_module = importlib.import_module(task.import_path)
    _callable = getattr(task_module, task.callable)
    _callable(
        input_paths=input_paths,
        output_path=output_path,
        metadata=metadata,
        component=component,
        **task_args,
    )
    logger.info(f'End execution of task "{task.name}" for {component=}.')
    console_handler.flush()

    return task.name, component


def _task_parallel_collect(
    metadata,
    task_name: str = None,
    component_list: Iterable[str] = None,
    inputs=None,
):
    history = f"{task_name}: {component_list}"
    try:
        metadata["history"].append(history)
    except KeyError:
        metadata["history"] = [history]

    return metadata


def _atomic_task_factory(
    *,
    task: Union[Task, PreprocessedTask],
    input_paths: List[Path],
    output_path: Path,
    data_flow_kernel: DataFlowKernel,
    metadata: Optional[Union[Future, Dict[str, Any]]] = None,
    depends_on: Optional[List[AppFuture]] = None,
    workflow_id: int = None,
) -> AppFuture:
    """
    Single task processing

    Create a single PARSL app that encapsulates the task at hand and
    its parallelizazion.
    """
    if depends_on is None:
        depends_on = []

    task_args = task._arguments

    logger = logging.getLogger(f"WF{workflow_id}")

    try:
        task_executor = get_unique_executor(
            workflow_id=workflow_id,
            task_executor=task.executor,
            data_flow_kernel=data_flow_kernel,
        )
        executors = [task_executor]
    except ValueError as e:
        # When assigning a task to an unknown executor, make sure to cleanup
        # DFK before raising an error
        data_flow_kernel.cleanup()
        logger.error("END of workflow due to ValueError (unknown executors).")
        raise ValueError(str(e))

    parall_level = task.parallelization_level
    if metadata and parall_level:

        # Define a single app
        logger.info(
            f'Defining app future for task "{task.name}", '
            f"to be executed on {executors=}"
        )
        task_component_app = PythonApp(
            _task_component_fun,
            executors=executors,
            data_flow_kernel=data_flow_kernel,
        )

        # Define an app that takes all the other as input
        parallel_collection_app = PythonApp(
            _task_parallel_collect,
            executors="all",
            data_flow_kernel=data_flow_kernel,
        )

        @join_app(data_flow_kernel=data_flow_kernel)
        def _parallel_task_app_future(
            *,
            task: Task,
            parall_level: str,
            input_paths: List[Path],
            output_path: Path,
            metadata: AppFuture,
            task_args: Optional[Dict[str, Any]],
            workflow_id: int,
            executors: Union[List[str], Literal["all"]] = "all",
        ) -> AppFuture:

            import logging

            logger = logging.getLogger(__name__)
            formatter = logging.Formatter(
                "%(asctime)s; %(levelname)s; %(message)s"
            )
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
            logger.setLevel(logging.INFO)
            logger.propagate = False

            num_parallel_items = len(metadata[parall_level])
            logger.info(
                f"Defining {num_parallel_items} app futures "
                f' for parallel execution of task "{task.name}" '
                f"on {executors=}."
            )
            console_handler.flush()

            # Define a list of futures, to be used as inputs (AKA dependencies)
            # for parallel_collection_app. This must happen within a join_app,
            # because metadata has not yet been computed.
            app_futures = []
            for item in metadata[parall_level]:
                component_app_future = task_component_app(
                    task=task,
                    component=item,
                    input_paths=input_paths,
                    output_path=output_path,
                    metadata=metadata,
                    task_args=task_args,
                )
                app_futures.append(component_app_future)

            # Return the future for the app collecting all parallel tasks
            parallel_collection_app_future = parallel_collection_app(
                metadata,
                task_name=task.name,
                component_list=metadata[parall_level],
                inputs=app_futures,
            )

            return parallel_collection_app_future

        res = _parallel_task_app_future(
            parall_level=parall_level,
            task=task,
            input_paths=input_paths,
            output_path=output_path,
            metadata=metadata,
            task_args=task_args,
            executors=executors,
            workflow_id=workflow_id,
        )
        return res
    else:
        res = _task_app_future(
            task=task,
            input_paths=input_paths,
            output_path=output_path,
            metadata=metadata,
            task_args=task_args,
            inputs=depends_on,
            executors=executors,
            data_flow_kernel=data_flow_kernel,
            workflow_id=workflow_id,
        )
        return res


def _process_workflow(
    task: Task,
    input_paths: List[Path],
    output_path: Path,
    metadata: Dict[str, Any],
    username: str = None,
    worker_init: str = None,
) -> AppFuture:
    """
    Creates the PARSL app that will execute the full workflow, taking care of
    dependencies

    Arguments
    ---------
    output_path (Path):
        directory or file where the final output, i.e., the output of the last
        task, will be written
    TBD

    Return
    ------
    TBD
    """
    preprocessed = task.preprocess()

    this_input = input_paths
    this_output = output_path
    this_metadata = deepcopy(metadata)

    workflow_id = task.id
    workflow_name = task.name

    dfk = load_parsl_config(
        workflow_id=workflow_id,
        workflow_name=workflow_name,
        worker_init=worker_init,
        username=username,
    )

    app_futures: List[PythonApp] = []
    for i, task in enumerate(preprocessed):
        this_task_app_future = _atomic_task_factory(
            task=task,
            input_paths=this_input,
            output_path=this_output,
            metadata=app_futures[i - 1] if i > 0 else this_metadata,
            workflow_id=workflow_id,
            data_flow_kernel=dfk,
        )

        app_futures.append(this_task_app_future)
        this_input = [this_output]

    return app_futures[-1], dfk


def get_app_future_result(app_future: AppFuture):
    """
    See issue #140 and https://stackoverflow.com/q/43241221/19085332

    By replacing
        .. = final_metadata.result()
    with
        .. = await async_wrap(get_app_future_result)(app_future=final_metadata)
    we avoid a (long) blocking statement.
    """
    return app_future.result()


async def process_workflow(
    *,
    workflow: Task,
    input_paths: List[Path],
    output_path: Path,
    input_metadata: Dict[str, Any],
    logger: logging.Logger,
    username: str = None,
    worker_init: str = None,
) -> Dict[str, Any]:
    """
    Public interface to the runner backend

    Return
    ------
    final_metadata: Dict[str, Any]
        mapping representing the final state of the output dataset metadata
    """
    logger.info(f"{input_paths=}")
    logger.info(f"{output_path=}")

    final_metadata_future, dfk = _process_workflow(
        task=workflow,
        input_paths=input_paths,
        output_path=output_path,
        metadata=input_metadata,
        username=username,
        worker_init=worker_init,
    )
    logger.info("Definition of app futures complete, now start execution. ")
    final_metadata = await async_wrap(get_app_future_result)(
        app_future=final_metadata_future
    )

    dfk.cleanup()
    return final_metadata