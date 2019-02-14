# TODO: PORT/WRAP

from __future__ import print_function

import argparse

import combtest.encode as encode
import combtest.runner as runner
import combtest.utils as utils
import combtest.walk as walk


ACCEPTED_COMMANDS = ('step', 'replay')

DEFAULT_REPLAY_FUNC = runner.replay_multistage_walk
DEFAULT_REPLAY_FUNC_NAME = utils.get_class_qualname(
        runner.replay_multistage_walk)

COMMAND_HELP = {'step': 'Replay, one step at a time',
                'replay': 'Replay the walk'}
COMMAND_HELP = "\n".join(["%s: %s" % (k, v) for k, v in
        COMMAND_HELP.iteritems()])


def load_from_trace(trace_file, walk_id):
    found = False
    walk_out = walk.Walk()
    with open(trace_file, 'r') as f:
        for line in f:
            decoded = encode.decode(line)
            walk_id = decoded['walk_id']
            if walk_id != walk_id:
                continue

            found = True
            sync_point = decoded.get('sync_point', None)
            walk_segment = decoded['walk']

            if sync_point is not None:
                walk_out.append(sync_point)

            walk_out += walk_segment

    if found:
        return walk_out
    raise ValueError("Cannot find walk with id %d" % walk_id)

def load_from_master(log_file, walk_id):
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

def load_walk(log_file, walk_id):
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

def replay_walk(walk_to_run, step=False, replay_func_qualname=None, ctx=None):
    if replay_func_qualname is None:
        replay_func = runner.replay_multistage_walk
    else:
        replay_func = utils.get_class_from_qualname(replay_func_qualname)

    replay_func(walk_to_run, step=step, ctx=ctx)

    return ctx

def replay_walk_by_id(log_file, walk_id, step=False, replay_func_qualname=None,
        ctx=None):

    walk_to_run = load_walk(log_file, walk_id)

    return replay_walk(walk_to_run, step=step,
            replay_func_qualname=replay_func_qualname, ctx=ctx)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Replay combtests")
    parser.add_argument('command', type=str, choices=ACCEPTED_COMMANDS,
            help=COMMAND_HELP)
    parser.add_argument('log_file',
                        type=str,
                        help="Master log file created by the running test, "
                             "or a trace file created by a WalkOpTracer "
                             "(e.g. via run_multistage_walks)."
                       )
    parser.add_argument('--replay_func',
                        type=str,
                        help="Qualname for function used for replay; see %s" %
                              DEFAULT_REPLAY_FUNC_NAME,
                        default=DEFAULT_REPLAY_FUNC_NAME)
    parser.add_argument('--ctx',
                        type=str,
                        help="ctx provided as a "
                             "JSON string, decodable by %s" %
                             utils.get_class_qualname(encode.decode))
    parser.add_argument('walk_id', type=int)
    parser.add_argument('--print_ctx', action='store_true')
    args = parser.parse_args()

    command = args.command

    step = command == 'step'

    if args.ctx:
        ctx = encode.decode(args.ctx)
    else:
        ctx = None

    ctx = replay_walk_by_id(args.log_file,
                            args.walk_id,
                            step=step,
                            replay_func_qualname=args.replay_func,
                            ctx=ctx)

    if args.print_ctx:
        print(encode.encode(ctx))
