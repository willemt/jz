
class OpRegister(object):
    ops = {}

    def add(self, cls):
        self.ops[cls.name] = cls
        return cls

ops = OpRegister()



#@ops.add
#class LoadCmd(object):
#    name = "load"
#    """ Load a into b """
#    def execute(self, a, b):
#        pass


#@ops.add
#class IndexCmd(object):
#    def execute(self):
#        pass


@ops.add
class CursorSetCmd(object):
    """ Set cursor at position

    @param 0 cursor pointer
    @param 1 table pointer
    @param 2 value to set cursor at

    """

    name = "curset"

    def execute(self, cursor, table, value):
        pass


@ops.add
class CursorNextCmd(object):
    name = "curnxt"
    """ Get next item and put in A 
    
    @param 0 cursor pointer
    @param 1 register to place the item
    @param 2 intruction to goto if cursor exhausted
    
    """
    def execute(self):
        pass


@ops.add
class CursorNewCmd(object):
    name = "curnew"
    """ New cursor   
    @param 0 cursor pointer
    """
    def execute(self):
        pass




@ops.add
class TableCmd(object):
    """ Register table into register """

    name = "table"

    def execute(self):
        pass


@ops.add
class EmitCmd(object):
    """ Output this item

    target_ptr = 0 is the final result
    """

    name = "emit"

    def execute(self, row, target_ptr):
        pass


@ops.add
class EqCmd(object):
    name = "eq"

    def execute(self, var1, var2, goto):
        pass
#        if var1 == var2:





"""
00 table  1, "index_distancekm"
01 curnew 2
02 curset 1, 2
03 curnxt 1, 3, 6
04 eq     1, 3, 6
05 emit   3, 0
06 exit
"""




class VM(object):
    def get_next():
        pass

    def run(self):
        for cmd in self.get_next():
            cmd.execute()
