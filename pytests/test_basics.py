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

@gearsTest()
def testRedisCallNullReply(env):
    """#!js name=foo
redis.register_function("test", function(client){
    return client.call('get', 'x');
})  
    """
    env.expect('RG.FUNCTION', 'CALL', 'foo', 'test').equal("undefined")

@gearsTest()
def testOOM(env):
    """#!js name=lib
redis.register_function("set", function(client, key, val){
    return client.call('set', key, val);
})  
    """
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'set', 'x', '1').equal('OK')
    env.expect('CONFIG', 'SET', 'maxmemory', '1')
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'set', 'x', '1').error().contains('OOM can not run the function when out of memory')

@gearsTest()
def testOOMOnAsyncFunction(env):
    """#!js name=lib
var continue_set = null;
var set_done = null;
var set_failed = null;

redis.register_function("async_set_continue",
    async function(client) {
        if (continue_set == null) {
            throw "no async set was triggered"
        }
        continue_set("continue");
        return await new Promise((resolve, reject) => {
            set_done = resolve;
            set_failed = reject
        })
    },
    ["allow-oom"]
)

redis.register_function("async_set_trigger", function(client, key, val){
    client.run_on_background(async function(client){
        await new Promise((resolve, reject) => {
            continue_set = resolve;
        })
        try {
            client.block(function(c){
                c.call('set', key, val);
            });
        } catch (error) {
            set_failed(error);
            return;
        }
        set_done("OK");
    });
    return "OK";
});
    """
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'async_set_trigger', 'x', '1').equal('OK')
    env.expect('CONFIG', 'SET', 'maxmemory', '1')
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'async_set_continue').error().contains('OOM Can not lock redis for write')

@gearsTest(envArgs={'useSlaves': True})
def testRunOnReplica(env):
    """#!js name=lib
redis.register_function("test1", function(client){
    return 1;
});

redis.register_function("test2", function(client){
    return 1;
},
['no-writes']);
    """
    replica = env.getSlaveConnection()
    env.expect('WAIT', '1', '7000').equal(1)

    try:
        replica.execute_command('RG.FUNCTION', 'CALL', 'lib', 'test1')
        env.assertTrue(False, message='Command succeed though should failed')
    except Exception as e:
        env.assertContains('can not run a function that might perform writes on a replica', str(e))

    env.assertEqual(1, replica.execute_command('RG.FUNCTION', 'CALL', 'lib', 'test2'))

@gearsTest()
def testNoWritesFlag(env):
    """#!js name=lib
redis.register_function("my_set", function(client, key, val){
    return client.call('set', key, val);
},
['no-writes']);
    """
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'my_set', 'foo', 'bar').error().contains('was called while write is not allowed')

@gearsTest()
def testBecomeReplicaWhenFunctionRunning(env):
    """#!js name=lib
var continue_set = null;
var set_done = null;
var set_failed = null;

redis.register_function("async_set_continue",
    async function(client) {
        if (continue_set == null) {
            throw "no async set was triggered"
        }
        continue_set("continue");
        return await new Promise((resolve, reject) => {
            set_done = resolve;
            set_failed = reject
        })
    },
    ["no-writes"]
)

redis.register_function("async_set_trigger", function(client, key, val){
    client.run_on_background(async function(client){
        await new Promise((resolve, reject) => {
            continue_set = resolve;
        })
        try {
            client.block(function(c){
                c.call('set', key, val);
            });
        } catch (error) {
            set_failed(error);
            return;
        }
        set_done("OK");
    });
    return "OK";
});
    """
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'async_set_trigger', 'x', '1').equal('OK')
    env.expect('replicaof', '127.0.0.1', '33333')
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'async_set_continue').error().contains('Can not lock redis for write on replica')
    env.expect('replicaof', 'no', 'one')