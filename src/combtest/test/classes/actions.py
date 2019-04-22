from combtest.action import Action, SerialAction, OptionSet

class ActionAppend1(Action):
    def run(self, param, state):
        if 'inner' not in state:
            state['inner'] = []

        my_state = state['inner']

        if 'sp_value' in state:
            my_state.append(state['sp_value'])
            del state['sp_value']

        my_state.append(self.param)

    OPTION_SET = range(5)
    @classmethod
    def get_option_set(cls):
        return OptionSet(cls.OPTION_SET, action_class=cls)

class ActionAppend2(ActionAppend1):
    OPTION_SET = range(10)

class ActionAppend3(ActionAppend1):
    OPTION_SET = range(3)


class SerialActionAppend1(SerialAction):
    def run(self, param, state):
        state['sp_value'] = param
        return state

    OPTION_SET = range(5)
    @classmethod
    def get_option_set(cls):
        return OptionSet(cls.OPTION_SET, action_class=cls)

class SerialActionAppend2(SerialActionAppend1):
    OPTION_SET = range(2)


class ActionSingleton1(ActionAppend1):
    OPTION_SET = (1,)

class ActionSingleton2(ActionAppend1):
    OPTION_SET = (2,)

class SerialActionSingleton1(SerialActionAppend1):
    OPTION_SET = (0,)

class ActionSingleton3(ActionAppend1):
    OPTION_SET = (3,)


class ActionAppendList1(Action):
    OPTIONS = (1, 2, 3)
    def run(self, param, state):
        state.append(param)

class ActionFail(Action):
    OPTIONS = (True,)

    def run(self, param, state):
        assert not param, "Uh Oh"

class SerialActionTryUpdate(SerialAction):
    OPTIONS = (True, True)
    def run(self, param, state):
        if param:
            return ('s',)

class SerialActionDontTryUpdate(SerialActionTryUpdate):
    OPTIONS = (False, False)

class ActionAppendList2(ActionAppendList1):
    OPTIONS = ('a', 'b', 'c', 'd')

class ActionExpectNoState(Action):
    OPTIONS = (True,)

    def run(self, param, state=None):
        assert state is None
