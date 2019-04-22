import unittest

from combtest.action import Action, OptionSet
import combtest.encode as encode
import combtest.walk as walk


class MyClass(object):
    def __init__(self):
        self.a = 1

    def to_json(self):
        return self.a

    @classmethod
    def from_json(cls, s):
        out = cls()
        out.a = s
        return out

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self.a == other.a

class MyClassInner(object):
    def __init__(self):
        self.a = 1

    def to_json(self):
        return str(self.a)

    @classmethod
    def from_json(cls, s):
        out = cls()
        out.a = int(s)
        return out

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self.a == other.a

class MyAction(Action):
    def __eq__(self, other):
        if not isinstance(other, MyAction):
            return False
        return self._param == other._param

    @classmethod
    def get_option_set(cls):
        mc = MyClass()
        mc.a = MyClassInner()
        options = [1, "b", mc]
        return OptionSet(options, action_class=cls)

class TestGeneralEncodeDecode(unittest.TestCase):
    def do_test(self, obj):
        s = encode.encode(obj)
        dec = encode.decode(s)
        s2 = encode.encode(dec)

        self.assertEqual(obj, dec)
        self.assertEqual(s, s2)

        return s, dec

    def test_simple_nested(self):
        o1 = MyClass()
        o1.a = MyClassInner()
        o1.a.a = 1

        o2 = MyClass()
        o2.a = MyClassInner()
        o2.a.a = 2

        o3 = MyClass()
        o3.a = MyClassInner()
        o3.a.a = 3

        l = [o1, o2, o3]
        l2 = [l, o1, o3]

        self.do_test(l2)

    def test_cannot_jsonify(self):
        o = object()
        try:
            self.do_test(o)
        except TypeError:
            return
        self.fail("Should have received a TypeError")

    def test_dict_pair(self):
        o1 = MyClass()
        o1.a = 10

        blah = {'asdf': o1}
        self.do_test(blah)

        # Shhhhh: I'm using a protected member for test purposes.
        blah2 = {encode._DEFAULT_MAGIC + "asdf": o1}
        self.do_test(blah2)

    def test_walk(self):
        my_walk = walk.Walk(MyAction.get_option_set())
        self.do_test(my_walk)

    def test_action_decode_cache(self):
        state = MyClass()
        state.a = 3
        ac = MyAction(state)
        json_str, decoded = self.do_test(ac)
        decoded_from_cache = encode.decode(json_str)
        self.assertIs(decoded, decoded_from_cache)
        self.assertEqual(ac, decoded)

    def test_magic_collision(self):
        class Inner(object):
            def to_json(self):
                return 1

        Inner.__module__ = encode._DEFAULT_MAGIC
        instance = Inner()
        encoded = encode.encode(instance)
        # (Cannot test decode, due to Inner having no sane __module__ against
        #  which we can resolve the qualname).

if __name__ == "__main__":
    unittest.main()
