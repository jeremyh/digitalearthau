import collections
import getpass
import logging
import multiprocessing
import re
import signal
import socket
import uuid
from time import sleep, time

import celery
import celery.events.state as celery_state
import celery.states
import click
import datetime
from celery.events import EventReceiver
from dateutil import tz
from typing import Optional, List, Iterable

from datacube import _celery_runner as cr
from digitalearthau import serialise, pbs

from digitalearthau.events import Status, TaskEvent, NodeMessage
from digitalearthau.runners.model import TaskDescription

_LOG = logging.getLogger(__name__)

# The strigified args that celery gives us back within task messages
_EXAMPLE_TASK_ARGS = "'(functools.partial(<function do_fc_task at 0x7f47e7aad598>, {" \
                     "\'source_type\': \'ls8_nbar_albers\', \'output_type\': \'ls8_fc_albers\', " \
                     "\'version\': \'${version}\', \'description\': \'Landsat 8 Fractional Cover 25 metre, " \
                     "100km tile, Australian Albers Equal Area projection (EPSG:3577)\', \'product_type\': " \
                     "\'fractional_cover\', \'location\': \'/g/data/fk4/datacube/002/\', \'file_path_template\': " \
                     "\'LS8_OLI_FC/{tile_index[0]}_{tile_index[1]}/LS8_OLI_FC_3577_{tile_index[0]}_{tile_index[1]}_" \
                     "{start_time}_v{version}.nc\', \'partial_ncml_path_template\': \'LS8_OLI_FC/{tile_index[0]}_" \
                     "{tile_index[1]}/LS8_OLI_FC_3577_{tile_index[0]}_{tile_index[1]}_{start_time}.ncml\', \'ncml_" \
                     "path_template\': \'LS8_OLI_FC/LS8_OLI_FC_3577_{tile_index[0]}_{tile_index[1]}.ncml\', \'sensor" \
                     "_regression_coefficients\': {\'blue\': [0.00041, 0.9747], \'green\': [0.00289, 0.99779], " \
                     "\'red\': [0.00274, 1.00446], \'nir\': [4e-05, 0.98906], \'swir1\': [0.00256, 0.99467], " \
                     "\'swir2\': [-0.00327, 1.02551]}, \'global_attributes\': {\'title\': \'Fractional Cover 25 " \
                     "v2\', \'summary\': \"The Fractional Cover (FC)...,)'"
_EXAMPLE_TASK_KWARGS = "{'task': {'nbar': Tile<sources=<xarray.DataArray (time: 1)>\narray([ (Dataset <id=" \
                       "d514c26a-d98f-47f1-b0de-15f7fe78c209 type=ls8_nbar_albers location=/g/data/rs0/datacube/002/" \
                       "LS8_OLI_NBAR/-11_-28/LS8_OLI_NBAR_3577_-11_-28_2015_v1496400956.nc>,)], dtype=object)\n" \
                       "Coordinates:\n  * time     (time) datetime64[ns] 2015-01-31T01:51:03,\n\tgeobox=GeoBox(4000, " \
                       "4000, Affine(25.0, 0.0, -1100000.0,\n       0.0, -25.0, -2700000.0), EPSG:3577)>, " \
                       "'tile_index': (-11, -28, numpy.datetime64('2015-01-31T01:51:03.000000000')), " \
                       "'filename': '/g/data/fk4/datacube/002/LS8_OLI_FC/-11_-28/LS8_OLI_FC_3577_-11_-28_" \
                       "20150131015103000000_v1507076205.nc'}}"

TASK_ID_RE_EXTRACT = re.compile('Dataset <id=([a-z0-9-]{36}) ')


def _extract_task_args_dataset_id(kwargs: str) -> Optional[uuid.UUID]:
    """
    >>> _extract_task_args_dataset_id(_EXAMPLE_TASK_KWARGS)
    UUID('d514c26a-d98f-47f1-b0de-15f7fe78c209')
    >>> _extract_task_args_dataset_id("no match")
    """
    m = TASK_ID_RE_EXTRACT.search(kwargs)
    if not m:
        return None

    return uuid.UUID(m.group(1))


def get_task_input_dataset_id(task: celery_state.Task):
    return _extract_task_args_dataset_id(task.kwargs)


def celery_event_to_task(name: TaskDescription,
                         task: celery_state.Task,
                         user=getpass.getuser()) -> Optional[TaskEvent]:
    # root_id uuid ?
    # uuid    uuid
    # hostname, pid
    # retries ?
    # timestamp ?
    # state ("RECEIVED")

    celery_statemap = {
        celery.states.PENDING: Status.PENDING,
        # Task was received by a worker.
        celery.states.RECEIVED: Status.PENDING,
        celery.states.STARTED: Status.ACTIVE,
        celery.states.SUCCESS: Status.COMPLETE,
        celery.states.FAILURE: Status.FAILED,
        celery.states.REVOKED: Status.CANCELLED,
        # Task was rejected (by a worker?)
        celery.states.REJECTED: Status.CANCELLED,
        # Waiting for retry
        celery.states.RETRY: Status.PENDING,
        celery.states.IGNORED: Status.PENDING,
    }
    if not task.state:
        _LOG.warning("No state known for task %r", task)
        return None
    status = celery_statemap.get(task.state)
    if not status:
        raise RuntimeError("Unknown celery state %r" % task.state)

    message = None
    if status.FAILED:
        message = task.traceback

    celery_worker: celery_state.Worker = task.worker
    dataset_id = get_task_input_dataset_id(task)
    return TaskEvent(
        timestamp=_utc_datetime(task.timestamp) if task.timestamp else datetime.datetime.utcnow(),
        event=f"task.{status.name.lower()}",
        name=name,
        user=user,
        status=status,
        id=task.id,
        parent_id=pbs.current_job_task_id(),
        message=message,
        input_datasets=(dataset_id,) if dataset_id else None,
        output_datasets=None,
        node=NodeMessage(
            hostname=_just_hostname(celery_worker.hostname),
            pid=celery_worker.pid
        ),
    )


def log_celery_tasks(should_shutdown: multiprocessing.Value, app: celery.Celery, task_description: 'TaskDescription'):
    # Open log file.
    # Connect to celery
    # Stream events to file.

    click.secho("Starting logger", bold=True)
    state: celery_state.State = app.events.State()

    # TODO: handling immature shutdown cleanly? The celery runner itself might need better support for it...

    # For now we ignore these "gentle" shutdown signals as we don't want to quit until all logs have been received.
    # The main process will receive sigints/terms and will tell us ("should_shutdown" var) when it's safe...
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)

    def handle_task(event):

        state.event(event)

        event_type: str = event['type']

        if not event_type.startswith('task-'):
            _LOG.debug("Skipping event %r", event_type)
            return

        # task name is sent only with -received event, and state
        # will keep track of this for us.
        task: celery_state.Task = state.tasks.get(event['uuid'])

        if not task:
            _LOG.warning(f"No task found {event_type}")
            return
        output.write_item(celery_event_to_task(task_description, task))
        _log_task_states(state)

    events_path = task_description.events_path.joinpath(f'{socket.gethostname()}-collected-events.jsonl')

    with serialise.JsonLinesWriter(events_path.open('a')) as output:
        with app.connection() as connection:

            recv: EventReceiver = app.events.Receiver(connection, handlers={
                '*': handle_task,
            })

            shutdown_received = None

            while True:
                # If idle for 5 seconds, it will recheck whether to shutdown
                try:
                    for _ in recv.consume(limit=None, timeout=5, wakeup=True):
                        pass
                except socket.timeout:
                    pass

                # We get a signal from the main process when it has terminated all workers, but there may still be
                # events to consume.
                if should_shutdown.value:
                    # If all workers have sent a shutdown event to us, we know we have recorded everything.
                    if sum(1 for w in state.workers.values() if w.active) == 0:
                        _LOG.info("All workers finished, exiting.")
                        break

                    # Otherwise we wait up to 60 seconds for the last events to filter through redis...
                    shutdown_received = shutdown_received or time()
                    ellapsed_shutdown_seconds = time() - shutdown_received
                    if ellapsed_shutdown_seconds > 60:
                        _LOG.warning("Some workers are marked as active but the time-limit was reached.")
                        break

            _LOG.info("logger finished")

    _log_task_states(state)

    # According to our recorded state we should have seen all workers stop.
    workers: List[celery_state.Worker] = list(state.workers.values())
    active_workers = [w.hostname for w in workers if w.active]
    _LOG.info("%s/%s recorded workers are active", len(active_workers), len(workers))
    # Based on recency of heartbeat, not an offline event.
    _LOG.info("%s/%s recorded workers seem to be alive", len(list(state.alive_workers())), len(workers))

    if active_workers:
        _LOG.warning(
            "Some workers had not finished executing; their logs will be missed:n\n\t%s",
            "\n\t".join(active_workers)
        )


def _log_task_states(state):
    # Print count of tasks in each state.
    tasks: Iterable[celery_state.Task] = state.tasks.values()
    task_states = collections.Counter(t.state for t in tasks)
    _LOG.info("Task states: %s", ", ".join(f"{v} {k}" for (k, v) in task_states.items()))


def _utc_datetime(timestamp: float):
    """
    >>> _utc_datetime(1507241505.7179525)
    datetime.datetime(2017, 10, 5, 22, 11, 45, 717952, tzinfo=tzutc())
    """
    return datetime.datetime.utcfromtimestamp(timestamp).replace(tzinfo=tz.tzutc())


def _just_hostname(hostname: str):
    """
    >>> _just_hostname("kveikur.local")
    'kveikur.local'
    >>> _just_hostname("someone@kveikur.local")
    'kveikur.local'
    >>> _just_hostname("someone@kveikur@local")
    Traceback (most recent call last):
    ...
    ValueError: ...
    """
    if '@' not in hostname:
        return hostname

    parts = hostname.split('@')
    if len(parts) != 2:
        raise ValueError("Strange-looking, unsupported hostname %r" % (hostname,))

    return parts[-1]


# TODO: Refactor before pull request (Hopefully this comment doesn't enter the pull request, that would be embarrassing)
# pylint: disable=too-many-locals
def launch_celery_worker_environment(task_description: TaskDescription, redis_params: dict):
    redis_port = redis_params['port']
    redis_host = pbs.hostname()
    redis_password = cr.get_redis_password(generate_if_missing=True)

    redis_shutdown = cr.launch_redis(password=redis_password, **redis_params)
    if not redis_shutdown:
        raise RuntimeError('Failed to launch Redis')

    _LOG.info('Launched Redis at %s:%d', redis_host, redis_port)

    for i in range(5):
        if cr.check_redis(redis_host, redis_port, redis_password) is False:
            sleep(0.5)

    executor = cr.CeleryExecutor(
        redis_host,
        redis_port,
        password=redis_password,
    )
    logger_shutdown = multiprocessing.Value('b', False, lock=False)

    log_proc = multiprocessing.Process(target=log_celery_tasks, args=(logger_shutdown, cr.app, task_description))
    log_proc.start()

    worker_env = pbs.get_env()
    worker_procs = []

    for node in pbs.nodes():
        nprocs = node.num_cores
        if node.is_main:
            nprocs = max(1, nprocs - 2)

        celery_worker_script = 'exec datacube-worker --executor celery {}:{} --nprocs {}'.format(
            redis_host, redis_port, nprocs)
        proc = pbs.pbsdsh(node.offset, celery_worker_script, env=worker_env)
        _LOG.info(f"Started {proc.pid}")
        worker_procs.append(proc)

    def start_shutdown():
        cr.app.control.shutdown()

    def shutdown():
        start_shutdown()
        _LOG.info('Waiting for workers to quit')

        # TODO: time limit followed by kill
        for p in worker_procs:
            p.wait()

        # We deliberately don't want to stop the logger until all worker have stopped completely.
        _LOG.info('Stopping log process')
        logger_shutdown.value = True
        log_proc.join()

        _LOG.info('Shutting down redis-server')
        redis_shutdown()

    return executor, shutdown