"""
Easy serialization/deserialization mechanism using JSON. It uses a pair of
methods: an instance level ``to_json()`` method and a classmethod
``from_json()`` which are used, when available, to convert b/w an instance and
JSON.

We use this mechanism to serialize :class:`combtest.action.Action` and
:class:`combtest.walk.Walk` instances for printing to a trace file, or other
logs where human readability is handy.  The reason we don't use a custom
encoder/decoder pair for those two classes is that the user can attach a
static_ctx which has no default JSON behavior.  We let them write ``to_json()``
etc. if they want ``static_ctx`` to be a member of a custom class.
"""

import json
import threading

from six import string_types

from combtest.action import Action
from combtest.utils import get_class_qualname, get_class_from_qualname

#### Encode
_DEFAULT_MAGIC = "CombtestEncoded."
class _Encoder(json.JSONEncoder):
    """
    JSONEncoder that leverages an instance-level to_json() func where
    available. Records take the form:
        {'magic.class.qualname': obj.to_json()}

    Simplifying: add a to_json() method that returns something jsonifiable
    (which can itself have a to_json() method), then pass it to this Encoder.
    The rest will be taken care of.
    """

    def default(self, obj):
        if hasattr(obj, 'to_json'):
            qualname = get_class_qualname(obj.__class__)
            qualname = _DEFAULT_MAGIC + qualname
            return {qualname: obj.to_json()}

        raise TypeError("Cannot figure out how to JSONify: %s" % repr(obj))

#### Decode
# Simple cache, since we are likely to decode the same Action multiple times,
# to the point it could threaten mem usage.
# Maps (action_class, jsonified static_ctx) -> instance
_ACTION_CACHE = {}
_ACTION_CACHE_LOCK = threading.RLock()

def _get_cached_action(action_class, static_ctx):
    """
    For Actions specifically, we are likely to repeatedly decode strings that
    result in a functionally equivalent action. This is because the 'comb' in
    'combtest' refers to running combinations of test options. To prevent mem
    explosion, we want all those decode attempts to return refs to the same
    underlying set of Actions.
    action_class(static_ctx) would return the Action in question. We pass them
    in separately so we have something to hash. (classes are hashable it seems)
    """
    key = (action_class, encode(static_ctx))
    if key not in _ACTION_CACHE:
        with _ACTION_CACHE_LOCK:
            if key not in _ACTION_CACHE:
                instance = action_class(static_ctx)
                _ACTION_CACHE[key] = instance
                return instance
    return _ACTION_CACHE[key]


def _jd_object_pair_hook(pair_list):
    """
    This func is passed to obj_pair_hook during decode. It is the counterpart
    to packaging {'class.qualname': inner_state} that is done during encode.
    For simplicity we leverage the Action cache right here. If somebody wants
    to leverage this file for some other lib at some point, they can just
    remove all the references to the Action cache below and above. That is -
    remove the issubclass(obj_class, Action) branch below.
    """
    if len(pair_list) > 1:
        return dict(pair_list)

    k, v = pair_list[0]
    if not isinstance(k, string_types):
        return {k: v}

    if k.startswith(_DEFAULT_MAGIC):
        qualname = k[len(_DEFAULT_MAGIC):]
    else:
        return {k: v}

    try:
        obj_class = get_class_from_qualname(qualname)
    except ValueError:
        return {k: v}

    if hasattr(obj_class, 'from_json'):
        if issubclass(obj_class, Action):
            return _get_cached_action(obj_class, v)
        return obj_class.from_json(v)
    else:
        return {k: v}

# These are the top level functions that are used by client code.
def encode(obj):
    """
    Return JSON equivalent to the provided obj, if possible.
    Will raise TypeError if it isn't possible.
    """
    return json.dumps(obj, cls=_Encoder)

def decode(json_str):
    """
    Decode a JSON string which was provided by :func:`encode`. Will leverage
    a cache for single-instancing ``Actions``. An ``Action`` is defined by a
    single, immutable ``static_ctx``, so we are safe to use interning.
    """
    return json.loads(json_str, object_pairs_hook=_jd_object_pair_hook)
