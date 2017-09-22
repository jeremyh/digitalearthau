import datetime
import json
import pathlib
import uuid
from pathlib import Path

import os
import structlog
import sys
import logging

_LOG = structlog.get_logger()


class CleanConsoleRenderer(structlog.dev.ConsoleRenderer):
    def __init__(self, pad_event=25):
        super().__init__(pad_event)
        # Dim debug messages
        self._level_to_color['debug'] = structlog.dev.DIM


class StructLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord):
        msg = record.msg
        if record.args is not None:
            msg = record.msg % record.args

        _LOG.info(
            'log.' + record.levelname.lower(),
            message=msg,
            logger=record.name
        )


def get_log_destination() -> Path:
    # Simplest option: stdout for structured.
    # if inside job? Local JOBFS?

    # In job, override all logging?
    if 'PBS_JOBFS' in os.environ:
        # If stageout defined, use it.
        # Eg. /jobfs/local/8765259.r-man2
        return Path(os.environ['PBS_JOBFS']) / 'dea-events.jsonl'

        # directly to lustre? If pbs job
        # current directory?
        # stdout? Simple


def init_log_storage():
    # Push std logging through structlog
    # This will only include whatever log levels have been configured (WARN by default)
    legacy_log = logging.getLogger()
    legacy_log.setLevel(logging.INFO)
    handler = StructLogHandler()
    handler.setLevel(logging.INFO)
    legacy_log.addHandler(handler)

    # Output structlog to our work location
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="ISO"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(serializer=_to_json),
        ],
        context_class=dict,
        cache_logger_on_first_use=True,
        logger_factory=structlog.PrintLoggerFactory(open('events.jsonl', 'a')),
    )


def _to_json(o, *args, **kwargs):
    """
    structlog by default writes the repr() of unknown objects.

    Let's make the output slightly more useful for common types.

    >>> _to_json([1, 2])
    '[1, 2]'
    >>> # Sets and paths
    >>> _to_json({Path('/tmp')})
    '["/tmp"]'
    >>> _to_json(uuid.UUID('b6bf8ff5-99e6-4562-87b4-cbe6549335e9'))
    '"b6bf8ff5-99e6-4562-87b4-cbe6549335e9"'
    """

    return json.dumps(
        o,
        default=_json_fallback,
        separators=(', ', ':'),
        sort_keys=True
    )


def _json_fallback(obj):
    """Fallback for non-serialisable json types."""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()

    if isinstance(obj, (pathlib.Path, uuid.UUID)):
        return str(obj)

    if isinstance(obj, set):
        return list(obj)

    try:
        # Allow class to define their own.
        return obj.to_dict()
    except AttributeError:
        # Same behaviour to structlog default: we always want to log the event
        return repr(obj)
