"""
Methods for replaying a :class:`combtest.walk.Walk`. Used via function call, or
main. This includes the ability to replay the ``Walk`` from a trace file
produced during a run.
"""

from __future__ import print_function

import argparse

import combtest.encode as encode
import combtest.runner as runner
import combtest.utils as utils
import combtest.walk as walk


ACCEPTED_COMMANDS = ('step', 'replay')

DEFAULT_REPLAY_FUNC = runner.replay_walk
DEFAULT_REPLAY_FUNC_NAME = utils.get_class_qualname(
        DEFAULT_REPLAY_FUNC)

_COMMAND_HELP = {'step': 'Replay, one step at a time',
                 'replay': 'Replay the walk'}
COMMAND_HELP = "\n".join(["%s: %s" % (k, v) for k, v in
                          _COMMAND_HELP.items()])


def load_from_trace(trace_file, walk_id):
    """
    Load a ``Walk`` from a trace file.

    :param str trace_file: a path to the trace file
    :param int walk_id: a ``walk_id`` which appears in the trace file
    """
    found = False
    walk_out = walk.Walk()
    with open(trace_file, 'r') as f:
        for line in f:
            decoded = encode.decode(line)
            walk_id = decoded['walk_id']
            if walk_id != walk_id:
                continue

            found = True
            serial_action = decoded.get('serial_action', None)
            walk_segment = decoded['walk']

            if serial_action is not None:
                walk_out.append(serial_action)

            walk_out += walk_segment

    if found:
        return walk_out
    raise ValueError("Cannot find walk with id %d" % walk_id)

def load_from_master(log_file, walk_id):
    """
    Load a ``Walk`` from a master log file. A master log file provides paths to
    trace files which were produced by ``Walk`` running services. This presumes
    the trace files are available locally, so if they were produced on a remote
    node, you'll need to make them available by whatever method you prefer.

    :param str log_file: a path to the master log file
    :param int walk_id: a ``walk_id`` which appears in the trace file
    """
    with open(log_file, 'r') as f:
        # Burn the first line, which identifies the file as a master log
        f.readline()
        for line in f:
            decoded = encode.decode(line)

            logs = decoded['logs']
            trace_file = logs[1]
            if trace_file:
                try:
                    return load_from_trace(trace_file, walk_id)
                except (ValueError, OSError):
                    pass

        raise ValueError("Cannot find walk with id %d", walk_id)

def _load_walk(log_file, walk_id):
    with open(log_file, 'r') as f:
        first_line = f.readline()
        try:
            first_line = encode.decode(first_line)
        except ValueError:
            raise ValueError("I couldn't interpret this as a log file: %s" %
                             log_file)

    if 'id' in first_line:
        return load_from_master(log_file, walk_id)
    return load_from_trace(log_file, walk_id)

def replay_walk(walk_to_run, step=False, replay_func_qualname=None, state=None):
    """
    Run a single :class:`combtest.walk.Walk`.

    :param Walk walk_to_run: self evident
    :param bool step: if True, step Action-by-Action through the Walk; the user
                      hits a key to proceed to the next Action.
    :param str replay_func_qualname: qualname of a replay function to use.
                                     Typically this will be
                                     :func:`combtest.runner.replay_walk`,
                                     but the user is free to provide their own.
    :param object state: ``state`` passed to the Walk for execution.
    """
    if replay_func_qualname is None:
        replay_func = runner.replay_walk
    else:
        replay_func = utils.get_class_from_qualname(replay_func_qualname)

    replay_func(walk_to_run, step=step, state=state)

    return state

def replay_walk_by_id(log_file, walk_id, step=False, replay_func_qualname=None,
                      state=None):
    """
    Run a single :class:`combtest.walk.Walk`. Load it by deserializing it from
    a trace file.

    :param object log_file: 'logs' object returned from :func:`run_tests`
    :param int walk_id: a walk_id that appears in the trace file, or one of the
                        trace files referenced by the master log file.
    :param bool step: if True, step Action-by-Action through the Walk; the user
                      hits a key to proceed to the next Action.
    :param str replay_func_qualname: qualname of a replay function to use.
                                     Typically this will be
                                     :func:`combtest.runner.replay_walk`,
                                     but the user is free to provide their own.
    :param object state: ``state`` passed to the Walk for execution.
    """

    walk_to_run = _load_walk(log_file['master'], walk_id)

    return replay_walk(walk_to_run, step=step,
                       replay_func_qualname=replay_func_qualname, state=state)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Replay combtests")
    parser.add_argument('command', type=str, choices=ACCEPTED_COMMANDS,
            help=COMMAND_HELP)
    parser.add_argument('log_file',
                        type=str,
                        help="Master log file created by the running test, "
                             "or a trace file created by a WalkOpTracer "
                             "(e.g. via run_tests)."
                       )
    parser.add_argument('--replay_func',
                        type=str,
                        help="Qualname for function used for replay; see %s" %
                              DEFAULT_REPLAY_FUNC_NAME,
                        default=DEFAULT_REPLAY_FUNC_NAME)
    parser.add_argument('--state',
                        type=str,
                        help="state provided as a "
                             "JSON string, decodable by %s" %
                             utils.get_class_qualname(encode.decode))
    parser.add_argument('walk_id', type=int)
    parser.add_argument('--print_state', action='store_true')
    args = parser.parse_args()

    command = args.command

    step = command == 'step'

    if args.state:
        state = encode.decode(args.state)
    else:
        state = None

    state = replay_walk_by_id(args.log_file,
                            args.walk_id,
                            step=step,
                            replay_func_qualname=args.replay_func,
                            state=state)

    if args.print_state:
        print(encode.encode(state))
