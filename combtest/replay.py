from __future__ import print_function

import argparse
import copy
import json
import paramiko.sftp_client as sftp
import sys
import tempfile

import ssh_session

import filesystem.layout.actions_misc as am
import filesystem.layout.file_configurator as file_configurator
import filesystem.layout.test_file as test_file
import filesystem.layout.walk.runner as runner
import filesystem.layout.walk.walk as walk


FTP_USER = "root"
FTP_PASS = "a"

ACCEPTED_FILTERS = ('lin', 'walk_id', 'path')
ACCEPTED_COMMANDS = ('step', 'replay')

COMMAND_HELP = {'step': 'Replay, one step at a time',
                'replay': 'Replay the walk'}
COMMAND_HELP = "\n".join(["%s: %s" % (k, v) for k, v in
        COMMAND_HELP.iteritems()])

def load_walk_by_path(master_log_path, test_path):
    return _load_walks_from_master(master_log_path, path=test_path)

def load_walk_by_lin(master_log_path, lin):
    return _load_walks_from_master(master_log_path, lin=lin)

def load_walk_by_walk_id(master_log_path, walk_id):
    return _load_walks_from_master(master_log_path, walk_id=walk_id)

def load_walk(log_path, **filter_kwargs):
    with open(log_path, 'r') as f:
        for line in f:
            try:
                first_line = json.loads(line)
            except ValueError:
                pass

            if 'id' in first_line and first_line['id'] == 'master':
                return _load_walks_from_master(f, **filter_kwargs)

            first_line_valid = True
            break

        if not first_line_valid:
            raise ValueError("Could not understand the log at %s" % log_path)

        if filter_kwargs:
            sys.stderr.write("Warning: filters ignored when a specific "
                    "walk or set of ops is given")

        if 'id' in first_line and first_line['id'] == 'op_replay':
            return _load_walk_from_op_replay(first_line, f)

        f.seek(0)
        return _load_walk_from_replay(f)

def _load_walk_from_op_replay(header, f):
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.writelines(f)

    file_config = header['config']
    action = am.Action_Replay(temp_file.name)
    walk_out = walk.Walk()
    walk_out.append(action)
    return walk_out, file_config

def _load_walk_from_replay(handle):
    record = None
    for line in handle:
        record = line
        break

    if record is None:
        raise ValueError("No walk in the replay file?")

    record = json.loads(record)
    walk_out, file_config_out = _unpack_walk(record)
    return walk_out, file_config_out

def _load_walks_from_master(handle, **filter_kwargs):
    # Maps ip->log location
    log_map = {}
    for line in handle:
        record = json.loads(line)
        log_map[record['ip']] = record['logs'][1]

    # Map ip->local log file
    files = _get_log_files(log_map)

    ip, found_walk, file_config = _unpack_files(files, **filter_kwargs)

    # close/remove the temp files
    for temp_log_file in files.values():
        temp_log_file.close()

    return found_walk, file_config

def _unpack_files(files, **filter_kwargs):
    # TODO: allow returning multiple walks + configs based on filter.
    ip_out = None
    walk_out = None
    file_config_out = None
    found = False
    for ip, temp_log_file in files.iteritems():
        if found:
            break

        for line in temp_log_file:
            record = json.loads(line)
            for key, value in filter_kwargs.iteritems():
                if key not in ACCEPTED_FILTERS:
                    raise ValueError("Filter key must be one of: %s" %
                            str(ACCEPTED_FILTERS))

                if key in record and record[key] == value:
                    # We need the last matching record in the file,
                    # since we will log multiple times for a multistage walk.
                    # The last entry will have the extent of the walk actually
                    # executed at the point the worker stopped. So e.g. on
                    # success or failure in the last stage we will see the full
                    # walk here.
                    found = True
                    ip_out = ip
                    walk_out, file_config_out = _unpack_walk(record)

    return ip_out, walk_out, file_config_out

def _get_log_files(log_map):
    # Maps ip->local log file copy
    files = {}
    array = ssh_session.SSHArray(log_map.keys())
    for ip, log_file_path in log_map.iteritems():
        shell = array.instances[ip]
        sftp_client = sftp.SFTPClient.from_transport(
                shell.ssh_client.get_transport())
        tf = tempfile.NamedTemporaryFile()
        sftp_client.get(log_file_path, tf.name)
        # XXX: implies closing the ssh_client too,
        # according to the docs. So don't try to reuse it.
        sftp_client.close()
        files[ip] = tf
    return files

def _unpack_walk(record):
    found_walk = walk.Walk.from_json(record['walk'])
    return found_walk, record['config']

def print_replay(found_walk):
    print(found_walk.as_json())

def replay_walk(found_walk, replay_dir, file_config=None, step=False):
    tf = test_file.TestFile.get_unique_in_dir(replay_dir)
    if file_config:
        file_configurator.apply_config(tf, file_config)

    _file_config = {'id': 'op_replay', 'config': file_config}
    print(json.dumps(_file_config))
    runner.replay_multistage_walk(found_walk, tf, step=step)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Replay walks")
    parser.add_argument('command', type=str, choices=ACCEPTED_COMMANDS,
            help=COMMAND_HELP)
    parser.add_argument('master_log_file', type=str,
            help="Master log file created by the running test")

    filter_group = parser.add_mutually_exclusive_group(required=False)
    filter_group.add_argument('--lin', type=str)
    filter_group.add_argument('--walk_id', type=int)
    filter_group.add_argument('--path', type=str)

    parser.add_argument('--replay_dir', type=str,
            help="A directory where the replay script will make a new "
                 "file to perform its ops on")

    args = parser.parse_args()

    command = args.command

    if command in ('replay', 'step') and not args.replay_dir:
        raise ValueError("Must supply a replay dir if you want a walk replay")

    filter_kwargs = {}
    if args.lin is not None:
        try:
            lin = int(args.lin)
        except ValueError:
            lin = args.lin
            if lin.startswith("0x"):
                lin = lin[2:]
            else:
                lin = lin.replace(':', '')

            try:
                lin = int(lin, base=16)
            except ValueError:
                raise ValueError("Provided lin must be a hex or "
                        "int literal. Do not include snapid.")
        filter_kwargs['lin'] = lin
    elif args.walk_id is not None:
        filter_kwargs['walk_id'] = args.walk_id
    elif args.path is not None:
        filter_kwargs['path'] = args.path

    found_walk, file_config = load_walk(args.master_log_file, **filter_kwargs)

    if command == 'step':
        replay_walk(found_walk, args.replay_dir, file_config=file_config,
                step=True)
    elif command == 'replay':
        replay_walk(found_walk, args.replay_dir, file_config=file_config,
                step=False)
