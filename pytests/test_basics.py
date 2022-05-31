from common import gearsTest

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
