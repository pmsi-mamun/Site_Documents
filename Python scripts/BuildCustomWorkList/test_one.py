__author__ = 'mrashid'

global my_name
my_name = 'some name'
p = []
class class_one:
    def __init__(self, name):
        self.my_name = name

    def test(self):
        p
        global my_name
        print(my_name)
        my_name = 'Florian'
        print(my_name)
        p = 3