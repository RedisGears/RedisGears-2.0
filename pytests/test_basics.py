from common import gearsTest

@gearsTest()
def testBasicJSInvocation(env):
    env.expect('RG.FUNCTION', 'LOAD', '#!js name=foo\nredis.register_function("test", function(){return 1})').equal('OK')
    env.expect('RG.FUNCTION', 'CALL', 'foo', 'test').equal(1)
