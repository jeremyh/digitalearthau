"""
Report on NCI PBS DEA jobs based on output into a set of work_dirs.
"""

import json
import locale
import math
import os
import re
import subprocess
from datetime import datetime
from functools import partial
from pathlib import Path

import click
import numpy as np
import pandas as pd
import sqlalchemy
from babel import Locale, numbers

WORK_DIR = Path('/g/data/v10/work/')

LOCALE, ENCODING = locale.getlocale()
LOCALE_OBJ = Locale(LOCALE or "en_AU")

as_percent = partial(numbers.format_percent,
                     locale=LOCALE_OBJ)
"""Format number as percentage."""


def as_currency(num):
    """Format number as currency."""
    return numbers.format_currency(num,
                                   currency='AUD',
                                   locale=LOCALE_OBJ) if not math.isnan(num) else 'NaN'


@click.command(help=__doc__)
@click.option('--glob', required=True, help="For example: 'ls?_fc*/create/*/*'. Be careful of shell escaping.")
@click.option('--html-file', required=True, type=click.Path(dir_okay=False, writable=True))
@click.option('--work-dir', default=WORK_DIR)
@click.option('--pickle-file', type=click.Path(dir_okay=False, writable=True))
@click.option('--sqlite-file', type=click.Path(dir_okay=False))
def main(glob, html_file, work_dir, pickle_file, sqlite_file):
    fc_dirs = work_dir.glob(glob)

    fc_create_tasks = [find_task_info(name) for name in fc_dirs]

    df = pd.DataFrame(fc_create_tasks)
    df = df.set_index('tag')
    df = df.sort_index(ascending=False)

    for job_part in ('run', 'generate'):
        if f'{job_part}_cputime' not in df:
            continue

        duration_cols = [f'{job_part}_cputime', f'{job_part}_walltime']
        numeric_cols = [f'{job_part}_service_units', f'{job_part}_ncpus']

        df[duration_cols] = df[duration_cols].apply(pd.to_timedelta)
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)

        df[f'{job_part}_cpu_efficiency'] = df[f'{job_part}_cputime'] / df[f'{job_part}_walltime'] / df[
            f'{job_part}_ncpus']

        df[f'{job_part}_cost'] = df[f'{job_part}_service_units'] * 0.03

    df['percent_complete'] = (df['num_completed'] / df['num_tasks'])

    if pickle_file:
        df.to_pickle(pickle_file)

    if sqlite_file:
        engine = sqlalchemy.create_engine('sqlite:///' + sqlite_file)
        df.to_sql('stacking_progress', engine, if_exists='replace')

    # Select and order columns
    # Available columns
    # cputime exit_status  generate_is_queued   memused  ncpus  num_completed  num_tasks  output_product
    # run_completed  run_queued  run_running  service_units time_finish # ed walltime  year  cpu_efficiency  percent_complete
    # tag

    df = df['year output_product run_queued run_running run_completed num_tasks num_completed '
            'percent_complete run_service_units run_cpu_efficiency run_cost'.split()]
    formatters = {'cost': as_currency,
                  'percent_complete': as_percent,
                  'cpu_efficiency': as_percent}
    # Output!
    Path(html_file).write_text(df.to_html(formatters=formatters), encoding='utf8')

    #    pd.set_option('display.max_colwidth', 250)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)
    print(df.to_string(formatters=formatters))


def find_task_info(task_dir):
    task_info = {'tag': str(task_dir)[-17:]}

    os.chdir(task_dir)
    td = json.loads((task_dir / 'task-description.json').read_text())
    task_info['output_product'] = td['parameters']['output_products'][0]
    task_info['year'] = td['parameters']['query']['time'][0][:4]

    task_info['generate_is_queued'] = any(Path().glob('jobs/*-generate-*'))
    task_info['run_queued'] = any(Path().glob('jobs/*-run-*'))
    task_info['run_running'] = any(Path().glob('events/*events.jsonl'))
    task_info['run_completed'] = any(Path().glob('logs/*-run-head*'))
    if task_info['run_queued']:
        try:
            task_info['num_tasks'] = int(
                find_in_file('Found ([0-9]*) tasks', task_dir.glob('logs/*generate-head.err.log')))
        except ValueError:
            try:
                task_info['num_tasks'] = int(
                    find_in_file('saved ([0-9]*) tasks', task_dir.glob('logs/*generate-head.err.log')))
            except ValueError:
                print(f'Error finding num_tasks for {task_dir}')
                task_info['num_tasks'] = np.nan
        generate_job_info = next(task_dir.glob('logs/*generate-head.out.log'))
        try:
            generate_job_info = {f'generate_{key}': val for key, val in parse_outfile(generate_job_info).items()}
            task_info.update(generate_job_info)
        except ValueError as e:
            print("Can't parse 'generate' stdout job information")
            print(e)

    if task_info['run_running']:
        task_info['num_completed'] = int(count_in_file('task.complete', task_dir.glob('events/*events.jsonl')))

    if task_info['run_completed']:
        pbs_stdout = next(task_dir.glob('logs/*run-head.out.log'))
        try:
            pbs_stdout_info = parse_outfile(pbs_stdout)
            pbs_stdout_info = {f'run_{key}': val for key, val in pbs_stdout_info.items()}
            task_info.update(pbs_stdout_info)
        except ValueError as e:
            print("Can't parse 'run' stdout job information")
            print(e)
    return task_info


def parse_outfile(path):
    content = path.read_text()
    su = re.compile('^\s*Service Units:\s*(\d+\.\d*)', flags=re.MULTILINE)

    exit_status = re.compile('^\s*Exit Status:\s*(\d.*)$', re.MULTILINE)

    cputime = re.compile('^.*CPU Time Used:\s*([\d\:]+)\s*$', re.M)

    memused = re.compile('^.*Memory Used:\s*([0-9\.]+\w\w?)', re.M)

    ncpus = re.compile('^.*NCPUs Used:\s*(\d+)', re.M)

    walltime = re.compile('^.*Walltime Used:\s*([\d\:]+)\s*$', re.M)

    job_finished = datetime.fromtimestamp(path.stat().st_ctime)

    try:
        return {'service_units': su.findall(content)[0],
                'exit_status': exit_status.findall(content)[0],
                'cputime': cputime.findall(content)[0],
                'walltime': walltime.findall(content)[0],
                'ncpus': ncpus.findall(content)[0],
                'memused': memused.findall(content)[0],
                'time_finished': str(job_finished)}
    except IndexError:
        raise ValueError('Unable to parse outfile: ', path)


def find_in_file(regexp, paths):
    return execute_command(['sed', '-rn', fr's/.*{regexp}.*/\1/p'] + [str(p) for p in paths])


def count_in_file(regexp, paths):
    return execute_command(f'grep {regexp} {" ".join([str(p) for p in paths])} | wc -l', shell=True)


def execute_command(cmd, **extra_args):
    completed = subprocess.run(cmd, stdout=subprocess.PIPE, encoding='ascii', check=True, **extra_args)
    return completed.stdout


if __name__ == '__main__':
    main()
