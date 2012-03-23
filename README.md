# labs-redis-clojure

This is Clojure client lib for [Redis](https://github.com/antirez/redis)

*Here be dragons!* .. or rather, Java. The client is a small core of java that implements the Redis protocol
with a clojure API for Redis commands around it.

## Status
*WORK IN PROGRESS!* I expect to have a stable release in a couple of weeks, but things seems to work ok.

## Features
- Pretty fast. The core Java part is in the same ballpark as redis-bench
- All commands are pipelined by default and returns futures.
- Syntax
- Return values are processed as little as possible, eg. GET xxx returns byte[].
includes some helper fns for converting to String `(->str @(get r "xxx"))` and String[] (->strs)
- Futures return can be deref:ed with `(deref xx)` and `@` (returns Reply)
- Replies can alse be deref:ed to their underlying values `@@(ping db) => "PONG"`
- Commands are generated from [redis-doc](https://github.com/antirez/redis-doc). This means it's easy to
keep the client up-to-date with Redis development. Also, useful documention for command fns.
- Sane pub/sub support, including correct behaviour for UNSUBSCRIBE returning connection to normal state.
- Support for MULTI/EXEC and return values (see example below).
- labs-redis does not use global `*bindings*` for connection ref. In my targeted code for this library
talks to alot of different Redis instances and `(with-connection (client) (set key val))` add alot of
uneccesary boilerplate for us.
- Idiomatic support for EVAL. `defeval` handles caching of lua source per connection and EVALSHA use etc. `(defeval my-echo [] [x] "return redis.call('ECHO',ARGV[1])")`
- Small connection pool impl (`pool` and `with-pool` macro.) Work in progress..

## Basic Usage

```clojure
(require '[labs-redis2.core :as redis])

(def db (redis/client))

(redis/ping db)
=> <ReplyFuture <StatusReply "PONG">>

@(redis/ping db)
=> <StatusReply "PONG">

(redis/value @(redis/ping db))
=> "PONG"

@@(redis/ping db)
=> "PONG"

@(redis/set db "foo" "bar")
=> <StatusReply "OK">

@(redis/get db "foo")
=> <BulkReply@xxxx: #<byte[] [B@xxxx]>>

@@(redis/get db "foo")
=> #<byte[] [B@xxxx]>

(String. @@(redis/get db "foo"))
=> "bar"

(redis/->str @(redis/get db "foo"))
=> "bar"
```

Arguments to commands are converted to do-what-I-mean so we can write code like
```clojure
(zunionstore r "out" 2 ["set1" "set2"] {:weights [10 20] :aggregate :sum})
```

## PUB/SUB
```clojure
(defn example-subscribe-with [db]
  (subscribe-with  db
   ["msgs" "msgs2" "msgs3"]
   (fn [db channel message]
     (let [message (->str message)]
       (println "R " channel message)
       (when (= "u" message)
         (cmd** db "UNSUBSCRIBE" [channel]))
       (not (= "quit" message))))))
```

## MULTI/EXEC
```clojure
  ;; multi/exec with sane handling of return values
  (try
    (multi r)
    (let [_ (mset r "k1" "v1" "k2" "v2")
          a (mget r "k1" "k2")
          b (set r "k1" "xx")]
      (exec! r)
      (println @a @b)) ;; xx v2
    (catch Throwable t
      (try @(discard r)
           (finally (throw t)))))
```

There is also a macro `(atomically db  &body)` that does multi/exec/discard and return the MultiBulkReply from EXEC.
See source for details.

## What's missing

Tests..

## Installation / Leiningen

*Not deployed on clojars yet. Install locally or use leiningen checkouts*

Add `[labs-redis "0.1.0"]` in your `project.clj`.
then run lein deps.

Author
------

Andreas Bielk :: andreas@preemtive.se :: @wallrat


## License

Copyright © 2012 Andreas Bielk

Distributed under the MIT/X11 license; see the file LICENSE.
