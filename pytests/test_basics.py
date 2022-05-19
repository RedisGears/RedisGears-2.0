from common import gearsTest

@gearsTest()
def testBasicJSInvocation(env):
    script = '''#!js name=foo
redis.register_function("test", function(){
    return 1
})
    '''
    env.expect('RG.FUNCTION', 'LOAD', script).equal('OK')
    env.expect('RG.FUNCTION', 'CALL', 'foo', 'test').equal(1)
