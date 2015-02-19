__author__ = 'mrashid'

from BuildCustomWorkList.test_one import class_one
class_one_test = class_one('test name')

def test_to_read():
    class_one_test.test()
    print(class_one_test.my_name)

test_to_read()