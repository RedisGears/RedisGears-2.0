from common import gearsTest
from common import toDictionary

@gearsTest()
def testAclOnSyncFunction(env):
    """#!js name=lib
redis.register_function("get", function(client, key){
    return client.call('get', key);
})
    """
    env.expect('ACL', 'SETUSER', 'alice', 'on', '>pass', '~cached:*', '+get', '+rg.function').equal('OK')
    env.expect('set', 'x', '1').equal(True)
    env.expect('set', 'cached:x', '1').equal(True)
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'get', 'x').equal('1')
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'get', 'cached:x').equal('1')
    env.expect('AUTH', 'alice', 'pass').equal(True)
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'get', 'x').error().contains('acl verification failed')
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'get', 'cached:x').equal('1')

@gearsTest()
def testAclOnAsyncFunction(env):
    """#!js name=lib
redis.register_function("get", async function(client, key){
    return client.block(function(client){
        return client.call('get', key);
    });
})
    """
    env.expect('ACL', 'SETUSER', 'alice', 'on', '>pass', '~cached:*', '+get', '+rg.function').equal('OK')
    env.expect('set', 'x', '1').equal(True)
    env.expect('set', 'cached:x', '1').equal(True)
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'get', 'x').equal('1')
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'get', 'cached:x').equal('1')
    env.expect('AUTH', 'alice', 'pass').equal(True)
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'get', 'x').error().contains('acl verification failed')
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'get', 'cached:x').equal('1')

@gearsTest()
def testAclOnAsyncComplex(env):
    """#!js name=lib
redis.register_function("get", async function(client, key){
    return client.block(function(client){
        return client.run_on_background(async function(client) {
            return client.block(function(client) {
                return client.call('get', key);
            });
        });
    });
});
    """
    env.expect('ACL', 'SETUSER', 'alice', 'on', '>pass', '~cached:*', '+get', '+rg.function').equal('OK')
    env.expect('set', 'x', '1').equal(True)
    env.expect('set', 'cached:x', '1').equal(True)
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'get', 'x').equal('1')
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'get', 'cached:x').equal('1')
    env.expect('AUTH', 'alice', 'pass').equal(True)
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'get', 'x').error().contains('acl verification failed')
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'get', 'cached:x').equal('1')

@gearsTest()
def testAclUserDeletedWhileFunctionIsRunning(env):
    """#!js name=lib
var async_get_continue = null;
var async_get_resolve = null;
var async_get_reject = null;

redis.register_function("async_get_continue", async function(client){
    async_get_continue("continue");
    return await new Promise((resolve, reject) => {
        async_get_resolve = resolve;
        async_get_reject = reject;
    })
});

redis.register_function("async_get_start", function(client, key){
    client.run_on_background(async function(client) {
        await new Promise((resolve, reject) => {
            async_get_continue = resolve;
        });
        client.block(function(client){
            try {
                async_get_resolve(client.call('get', key));
            } catch (e) {
                async_get_reject(e);
            }
        });
    });
    return "OK";
});
    """
    env.expect('ACL', 'SETUSER', 'alice', 'on', '>pass', '~cached:*', '+get', '+rg.function').equal('OK')
    env.expect('set', 'x', '1').equal(True)
    env.expect('set', 'cached:x', '1').equal(True)
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'async_get_start', 'x').equal('OK')
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'async_get_continue').equal('1')
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'async_get_start', 'cached:x').equal('OK')
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'async_get_continue').equal('1')
    c = env.getConnection()
    c.execute_command('AUTH', 'alice', 'pass')

    env.assertEqual(c.execute_command('RG.FUNCTION', 'CALL', 'lib', 'async_get_start', 'x'), "OK")
    try:
        c.execute_command('RG.FUNCTION', 'CALL', 'lib', 'async_get_continue')
        env.assertTrue(False, message='Command succeed though should failed')
    except Exception as e:
        env.assertContains("acl verification failed", str(e))

    env.assertEqual(c.execute_command('RG.FUNCTION', 'CALL', 'lib', 'async_get_start', 'cached:x'), "OK")
    try:
        env.assertEqual(c.execute_command('RG.FUNCTION', 'CALL', 'lib', 'async_get_continue'), '1')
    except Exception as e:
        env.assertTrue(False, message='Command failed though should success, %s' % str(e))

    env.assertEqual(c.execute_command('RG.FUNCTION', 'CALL', 'lib', 'async_get_start', 'cached:x'), "OK")
    env.expect('ACL', 'DELUSER', 'alice').equal(1) # delete alice user while function is running
    try:
        c.execute_command('RG.FUNCTION', 'CALL', 'lib', 'async_get_continue')
        env.assertTrue(False, message='Command succeed though should failed')
    except Exception as e:
        env.assertContains("Failed authenticating client", str(e))
