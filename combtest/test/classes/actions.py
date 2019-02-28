from combtest.action import Action, SyncPoint, OptionSet

class ActionAppend1(Action):
    def run(self, static_ctx, dynamic_ctx):
        if 'inner' not in dynamic_ctx:
            dynamic_ctx['inner'] = []

        my_ctx = dynamic_ctx['inner']

        if 'sp_value' in dynamic_ctx:
            my_ctx.append(dynamic_ctx['sp_value'])
            del dynamic_ctx['sp_value']

        my_ctx.append(self.static_ctx)

    OPTION_SET = range(5)
    @classmethod
    def get_option_set(cls):
        return OptionSet(cls.OPTION_SET, action_class=cls)

class ActionAppend2(ActionAppend1):
    OPTION_SET = range(10)

class ActionAppend3(ActionAppend1):
    OPTION_SET = range(3)


class SyncPointAppend1(SyncPoint):
    def run(self, static_ctx, dynamic_ctx):
        self.update_remote_contexts(dynamic_ctx, sp_value=static_ctx)

    def run_as_replay(self, static_ctx, dynamic_ctx):
        dynamic_ctx['sp_value'] = static_ctx

    OPTION_SET = range(5)
    @classmethod
    def get_option_set(cls):
        return OptionSet(cls.OPTION_SET, action_class=cls)

class SyncPointAppend2(SyncPointAppend1):
    OPTION_SET = range(2)


class ActionSingleton1(ActionAppend1):
    OPTION_SET = (1,)

class ActionSingleton2(ActionAppend1):
    OPTION_SET = (2,)

class SyncPointSingleton1(SyncPointAppend1):
    OPTION_SET = (0,)

class ActionSingleton3(ActionAppend1):
    OPTION_SET = (3,)
