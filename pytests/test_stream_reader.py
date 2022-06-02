from common import gearsTest
from common import TimeLimit
from common import toDictionary
from common import runUntil
from common import runFor
import time

'''
todo:
1. tests for upgrade/delete library (continue from the same id and finishes the pending ids)
2. tests for delete the stream while processes
3. tests for flushall while processes
4. tests for multiple consumers on the same stream
   a. each one gets all the data
   b. data is trimmed only when everyone finished processing it
5. multiple streams for consumer
6. RDB save and load continues from the same id
'''

@gearsTest()
def testBasicStreamReader(env):
    """#!js name=lib
var num_events = 0;
redis.register_function("num_events", function(){
    return num_events;
})
redis.register_stream_consumer("consumer", "stream", 1, false, function(){
    num_events++;
})
    """
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'num_events').equal(0)
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'num_events').equal(1)
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'num_events').equal(2)

@gearsTest()
def testAsyncStreamReader(env):
    """#!js name=lib
var num_events = 0;
redis.register_function("num_events", function(){
    return num_events;
})
redis.register_stream_consumer("consumer", "stream", 1, false, async function(){
    num_events++;
})
    """
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'num_events').equal(0)
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 1, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_events'))
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 2, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_events'))

@gearsTest()
def testStreamTrim(env):
    """#!js name=lib
var num_events = 0;
redis.register_function("num_events", function(){
    return num_events;
})
redis.register_stream_consumer("consumer", "stream", 1, true, function(){
    num_events++;
})
    """
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'num_events').equal(0)
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.expect('xlen', 'stream:1').equal(0)
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'num_events').equal(1)
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    env.expect('xlen', 'stream:1').equal(0)
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'num_events').equal(2)

@gearsTest()
def testStreamProccessError(env):
    """#!js name=lib
redis.register_stream_consumer("consumer", "stream", 1, false, function(){
    throw 'Error';
})
    """
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    res = toDictionary(env.cmd('RG.FUNCTION', 'LIST', 'vv'), 6)
    env.assertEqual('Error', res[0]['stream_registrations'][0]['streams'][0]['last_error'])

@gearsTest()
def testStreamWindow(env):
    """#!js name=lib
var promises = [];
redis.register_function("num_pending", function(){
    return promises.length;
})

redis.register_function("continue", function(){
    if (promises.length == 0) {
        throw "No pending records"
    }
    promises[0]('continue');
    promises.shift()
    return "OK"
})

redis.register_stream_consumer("consumer", "stream", 3, true, async function(){
    return await new Promise((resolve, reject) => {
        promises.push(resolve);
    });
})
    """
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'num_pending').equal(0)
    env.expect('RG.FUNCTION', 'CALL', 'lib', 'continue').error().contains('No pending records')

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 1, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 2, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))
    
    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 3, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))

    res = toDictionary(env.cmd('RG.FUNCTION', 'LIST', 'vvv'), 6)
    env.assertEqual(3, len(res[0]['stream_registrations'][0]['streams'][0]['pending_ids']))

    env.expect('RG.FUNCTION', 'CALL', 'lib', 'continue').equal('OK')
    runUntil(env, 2, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))
    
    runUntil(env, 2, lambda: len(toDictionary(env.cmd('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['stream_registrations'][0]['streams'][0]['pending_ids']))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 3, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))

    res = toDictionary(env.cmd('RG.FUNCTION', 'LIST', 'vvv'), 6)
    env.assertEqual(3, len(res[0]['stream_registrations'][0]['streams'][0]['pending_ids']))

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runFor(3, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))

    env.expect('RG.FUNCTION', 'CALL', 'lib', 'continue').equal('OK')
    runUntil(env, 3, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))

    res = toDictionary(env.cmd('RG.FUNCTION', 'LIST', 'vvv'), 6)
    env.assertEqual(2, res[0]['stream_registrations'][0]['streams'][0]['total_record_processed'])

@gearsTest(envArgs={'useSlaves': True})
def testStreamWithReplication(env):
    """#!js name=lib
var promises = [];
redis.register_function("num_pending", function(){
    return promises.length;
})

redis.register_function("continue", function(){
    if (promises.length == 0) {
        throw "No pending records"
    }
    p = promises[0]
    promises.shift()
    p[1]('continue')
    id = p[0].id;
    return id[0].toString() + "-" + id[1].toString()
})

redis.register_stream_consumer("consumer", "stream", 3, true, async function(client, data){
    return await new Promise((resolve, reject) => {
        promises.push([data,resolve]);
    });
})
    """
    slave_conn = env.getSlaveConnection()

    env.expect('WAIT', '1', '5000').equal(1)

    env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    runUntil(env, 1, lambda: env.cmd('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))

    res = toDictionary(env.cmd('RG.FUNCTION', 'LIST', 'vvv'), 6)
    id_to_read_from = res[0]['stream_registrations'][0]['streams'][0]['id_to_read_from']

    env.expect('RG.FUNCTION', 'CALL', 'lib', 'continue').equal(id_to_read_from)
    runUntil(env, 1, lambda: len(toDictionary(slave_conn.execute_command('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['stream_registrations'][0]['streams']))
    env.assertEqual(id_to_read_from, toDictionary(slave_conn.execute_command('RG.FUNCTION', 'LIST', 'vvv'), 6)[0]['stream_registrations'][0]['streams'][0]['id_to_read_from'])

    # add 2 more record to the stream
    id1 = env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')
    id2 = env.cmd('xadd', 'stream:1', '*', 'foo', 'bar')

    runUntil(env, 2, lambda: slave_conn.execute_command('xlen', 'stream:1'))

    # Turn slave to master
    slave_conn.execute_command('slaveof', 'no', 'one')
    runUntil(env, 2, lambda: slave_conn.execute_command('RG.FUNCTION', 'CALL', 'lib', 'num_pending'))
    def continue_function():
        try:
            return slave_conn.execute_command('RG.FUNCTION', 'CALL', 'lib', 'continue')
        except Exception as e:
            return str(e)
    runUntil(env, id1, continue_function)
    runUntil(env, id2, continue_function)
