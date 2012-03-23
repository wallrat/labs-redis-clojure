# labs-redis2

This is Clojure client lib for [Redis](https://github.com/antirez/redis)

- fast (frugal)
- pipeline by default
- @
- @@
- idiomatic
- more than one redis server
- pub/sub INC unsub
- multi/exec
- eval
- up-to-date, commands.json

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

## Installation / Leiningen

Add `[labs-redis "0.1.1"] in your `project.clj`.
then run lein deps.


## License

Copyright © 2012 Andreas Bielk

Distributed under the MIT/X11 license; see the file LICENSE.