from common import gearsTest
from common import toDictionary

'''
todo:
1. tests for rdb save and load
2. tests for block and go to background
'''

@gearsTest()
def testBasicJSInvocation(env):
    """#!js name=foo
redis.register_function("test", function(){
    return 1
})
    """
    env.expect('RG.FUNCTION', 'CALL', 'foo', 'test').equal(1)

@gearsTest()
def testCommandInvocation(env):
    """#!js name=foo
redis.register_function("test", function(client){
    return client.call('ping')
})  
    """
    env.expect('RG.FUNCTION', 'CALL', 'foo', 'test').equal('PONG')

@gearsTest()
def testLibraryUpgrade(env):
    """#!js name=foo
redis.register_function("test", function(client){
    return 1
})  
    """
    script = '''#!js name=foo
redis.register_function("test", function(client){
    return 2
})  
    '''
    env.expect('RG.FUNCTION', 'CALL', 'foo', 'test').equal(1)
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', script).equal('OK')
    env.expect('RG.FUNCTION', 'CALL', 'foo', 'test').equal(2)

    # make sure isolate was released
    isolate_stats = toDictionary(env.cmd('RG.FUNCTION', 'DEBUG', 'js', 'isolates_stats'))
    env.assertEqual(isolate_stats['active'], 1)
    env.assertEqual(isolate_stats['not_active'], 1)

@gearsTest()
def testLibraryUpgradeFailure(env):
    """#!js name=foo
redis.register_function("test", function(client){
    return 1
})  
    """
    script = '''#!js name=foo
redis.register_function("test", function(client){
    return 2
})
redis.register_function("test", "bar"); // this will fail
    '''
    env.expect('RG.FUNCTION', 'CALL', 'foo', 'test').equal(1)
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', script).error().contains('must be a function')
    env.expect('RG.FUNCTION', 'CALL', 'foo', 'test').equal(1)

    # make sure isolate was released
    isolate_stats = toDictionary(env.cmd('RG.FUNCTION', 'DEBUG', 'js', 'isolates_stats'))
    env.assertEqual(isolate_stats['active'], 1)
    env.assertEqual(isolate_stats['not_active'], 1)

