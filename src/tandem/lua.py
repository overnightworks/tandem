"""The Lua kit a host's script runs beside tandem's: one implementation of each transition."""

# `keys` of the deadline functions is a table with the fields due, claimed and claim, the key names
# of one deadline kind (`Deadlines.keys`); that of `lock_state` has the fields epoch, expiry and
# generation (`Locks.keys`). A script passes them from its own KEYS, so the server sees every key
# it uses.
LUA_KIT = """
local tandem = {}

-- Redis TIME in milliseconds; no pod clock is read.
function tandem.now_ms()
  local time = redis.call('TIME')
  return tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
end

-- Creates the epoch, the Redis time of the first command after Redis lost its data, when it is
-- absent. True once `lasts_ms` have passed since the epoch: an absent entry is then gone, not lost.
function tandem.epoch_settled(epoch_key, now_ms, lasts_ms)
  redis.call('SET', epoch_key, now_ms, 'NX')
  return now_ms - tonumber(redis.call('GET', epoch_key)) >= tonumber(lasts_ms)
end

-- Arms `key` to come due `due_in_ms` from now; a claim on it is invalidated. Returns the due ms.
function tandem.deadline_set(keys, key, due_in_ms)
  local due_ms = tandem.now_ms() + tonumber(due_in_ms)
  redis.call('ZREM', keys.claimed, key)
  redis.call('HDEL', keys.claim, key)
  redis.call('ZADD', keys.due, due_ms, key)
  return due_ms
end

-- Removes `key` whether it waits or is claimed; a claim on it is invalidated. Returns 1 when
-- there was something to remove, else 0.
function tandem.deadline_cancel(keys, key)
  local removed = redis.call('ZREM', keys.due, key) + redis.call('ZREM', keys.claimed, key)
  redis.call('HDEL', keys.claim, key)
  if removed > 0 then
    return 1
  end
  return 0
end

-- {'waiting', due_ms} while `key` waits, {'claimed', claim_until_ms} while a claim holds it, else
-- nil. A claim past its expiry that nobody took over reads as claimed.
function tandem.deadline_state(keys, key)
  local due_ms = redis.call('ZSCORE', keys.due, key)
  if due_ms then
    return {'waiting', tonumber(due_ms)}
  end
  local claim_until_ms = redis.call('ZSCORE', keys.claimed, key)
  if claim_until_ms then
    return {'claimed', tonumber(claim_until_ms)}
  end
  return nil
end

-- {'held', generation, expires_ms} while a live lock holds `name`; {'unknown'} while it is absent
-- and `lasts_ms` have not passed since the epoch, which this call creates when absent; else nil.
-- `if tandem.lock_state(...) then` therefore refuses unless the lock is free and settled.
function tandem.lock_state(keys, name, lasts_ms)
  local now_ms = tandem.now_ms()
  local settled = tandem.epoch_settled(keys.epoch, now_ms, lasts_ms)
  local expires_ms = redis.call('ZSCORE', keys.expiry, name)
  if expires_ms and tonumber(expires_ms) > now_ms then
    return {'held', redis.call('HGET', keys.generation, name), tonumber(expires_ms)}
  end
  if settled then
    return nil
  end
  return {'unknown'}
end

-- Publishes `address` on the wake channel `channel` (`wake_channel(key_prefix)`); returns the
-- number of subscribers it reached.
function tandem.wake(channel, address)
  return redis.call('PUBLISH', channel, address)
end
"""
