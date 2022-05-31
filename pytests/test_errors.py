from common import gearsTest

@gearsTest()
def testWrongEngine(env):
    script = '''#!js1 name=foo
redis.register_function("test", function(client){
    return 2
})  
    '''
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', script).error().contains('Unknown backend')

@gearsTest()
def testNoName(env):
    script = '''#!js
redis.register_function("test", function(client){
    return 2
})  
    '''
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', script).error().contains("Failed find 'name' property")

@gearsTest()
def testSameFunctionName(env):
    script = '''#!js name=foo
redis.register_function("test", function(client){
    return 2
})
redis.register_function("test", function(client){
    return 2
})
    '''
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', script).error().contains("Function test already exists")

@gearsTest()
def testWrongArguments1(env):
    script = '''#!js name=foo
redis.register_function(1, function(client){
    return 2
})
    '''
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', script).error().contains("must be a string")

@gearsTest()
def testWrongArguments2(env):
    script = '''#!js name=foo
redis.register_function("test", "foo")
    '''
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', script).error().contains("must be a function")

@gearsTest()
def testNoRegistrations(env):
    script = '''#!js name=foo

    '''
    env.expect('RG.FUNCTION', 'LOAD', 'UPGRADE', script).error().contains("No function nor registrations was registered")
