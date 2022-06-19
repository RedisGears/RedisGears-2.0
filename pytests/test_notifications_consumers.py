from common import gearsTest
from common import TimeLimit
from common import toDictionary
from common import runUntil
from common import runFor
import time

@gearsTest()
def testBasicNotifcations(env):
    """#!js name=lib
var n_notifications = 0;
redis.register_notifications_consumer("consumer", "", function(client, data) {
    n_notifications += 1;
});

redis.register_function("n_notifications", function(){
    return n_notifications
})
    """

    env.expect('RG.FUNCTION', 'CALL', 'lib', 'n_notifications').equal(0)
    env.expect('SET', 'X', '1').equal(True)
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'n_notifications').equal(1)
    env.expect('SET', 'X', '2').equal(True)
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'n_notifications').equal(2)
    env.expect('SET', 'X', '1').equal(True)
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'n_notifications').equal(3)

@gearsTest()
def testAsyncNotification(env):
    """#!js name=lib
var n_notifications = 0;
redis.register_notifications_consumer("consumer", "", async function(client, data) {
    n_notifications += 1;
});

redis.register_function("n_notifications", async function(){
    return n_notifications
})
    """

    env.expect('RG.FUNCTION', 'CALL', 'lib', 'n_notifications').equal(0)
    env.expect('SET', 'X', '1').equal(True)
    runUntil(env, 1, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'n_notifications'))
    env.expect('SET', 'X', '2').equal(True)
    runUntil(env, 2, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'n_notifications'))
    env.expect('SET', 'X', '1').equal(True)
    runUntil(env, 3, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'n_notifications'))

@gearsTest()
def testCallRedisOnNotification(env):
    """#!js name=lib
redis.register_notifications_consumer("consumer", "key", async function(client, data) {
    client.block(function(client){
        client.call('incr', 'count')
    });
});
    """

    env.expect('GET', 'count').equal(None)
    env.expect('SET', 'key1', '1').equal(True)
    runUntil(env, '1', lambda: env.cmd('GET', 'count'))
    env.expect('SET', 'key2', '2').equal(True)
    runUntil(env, '2', lambda: env.cmd('GET', 'count'))
    env.expect('SET', 'key3', '1').equal(True)
    runUntil(env, '3', lambda: env.cmd('GET', 'count'))
