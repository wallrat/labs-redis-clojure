(ns ^{:doc "A Redis client library for Clojure."
      :author "Andreas Bielk (@wallrat)"}
  labs.redis.core
  (:refer-clojure :exclude [get set keys type eval sort sync])
  (:use [clojure.repl]
        [clojure.pprint :only (pprint)]
        [clojure.java.io :only (resource)])
  (:require [clojure.data.json :as json])
  (:import [labs.redis Client ClientPool Reply BulkReply MultiBulkReply]))

(set! *warn-on-reflection* true)

(defn parse-url [url]
  (let [u (java.net.URI. url)
        ui (.getUserInfo u)
        [user password] (when ui (.split ui ":"))]
    {:host (.getHost u)
     :port (.getPort u)
     :user user
     :password password}))

(defn client
  "Creates and returns an Redis client"
  ([] (Client.))
  ([{:keys [host port timeout test-on-borrow] :as opts}]
     (Client. ^String host ^int port)))

(defn pool
  "Creates and returns a pool of Redis clients"
  ([] (pool (parse-url "redis://localhost:6379")))
  ([{:keys [host port timeout test-on-borrow] :as opts}]
     (ClientPool. host port test-on-borrow)))

;; (defmacro with-pool [name pool & body]
;;   `(let [~name (.borrow ~pool)]
;;      (try
;;        (let [result# (do ~@body)]
;;          (.release ~pool ~name)
;;          result#)
;;        (catch Exception e#
;;          (.release ~pool ~name)
;;          (throw e#)))))

(defmacro with-pool [name pool & body]
  `(let [~name (.borrow ~pool)]
     (try
       (let [result# (do ~@body)]
         result#)
       (finally
         (.release ~pool ~name)))))

;; helper fns for futures and values

(defn value [^Reply reply]
  (.getValue reply))

(let [byte-array-class (Class/forName "[B")]
  (defn ->str
    "Coerces reply into a String."
    [reply]
    (condp instance? reply
      byte-array-class (String. ^bytes reply)
      BulkReply (String. (.bytes ^BulkReply reply))
      java.lang.Object (.toString ^java.lang.Object reply))))

(defn ->strs [reply]
  (map ->str (value reply)))

(defn ->>str [r]
  (condp instance? r
    MultiBulkReply (map ->>str (value r))
    Reply (->>str (value r))
    java.lang.Object (->str r)))

;; low level redis protocol fns

(defn- cmd-arg-convert
  "Converts arguments to cmd* to acceptable java interop values"
  [v]
  (cond
   (string? v) v
   (instance? java.lang.Number v) v
   (keyword? v) (name v)
   (map? v) (map cmd-arg-convert v)
   (vector? v) (map cmd-arg-convert v)
   ;; string
   ;; byte[]
   :default (.toString ^Object v)))


(defn cmd*
  "Low-level fn for sending commands to redis. Returns a LinkedReplyFuture
  Example (cmd* db \"SET\" [\"mykey\" \"myval\"])"
  ([^Client R cmd ks1 ks2] (cmd* R cmd (concat ks1 ks2)))
  ([^Client R cmd ks]
     (let [cv (flatten (map cmd-arg-convert ks))
           args (into-array java.lang.Object (cons cmd cv))]
       (.pipeline R args))))

(defn cmd**
  ([^Client R cmd ks1 ks2] (cmd* R cmd (concat ks1 ks2)))
  ([^Client R cmd ks]
     (let [cv (flatten (map cmd-arg-convert ks))
           args (into-array java.lang.Object (cons cmd cv))]
       (.send R args))))

;; high level redis commands

(def REDIS-COMMANDS
  (dissoc (json/read-json (slurp (resource "commands.json")))
          :SUBSCRIBE :UNSUBSCRIBE :PSUBSCRIBE :PUNSUBSCRIBE :MONITOR))

(defn- redis-doc-str
  "Creates a doc string matching http://redis.io"
  ;; ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
  [n {args :arguments}]
  (let [fmt (fn [a]
              (let [s (cond
                       (:command a) (str (:command a) " "
                                         (if (= (:type a) "enum")
                                           (apply str (interpose "|" (:enum a)))
                                           (if (string? (:name a))
                                             (:name a)
                                             (apply str (interpose " " (:name a))))))
                       (= (:type a) "enum") (apply str (interpose "|" (:enum a)))
                       :default (:name a))
                    s (if (:multiple a) (str s " [" s " ..]") s)
                    s (if (:optional a) (str "[" s "]") s)]
                s))
        ]
    (str (.toUpperCase (name n)) " "
         (apply str (interpose " " (map fmt args))))))

(defn- fn-docs [n m]
  (str (redis-doc-str n m) "\n"
       "  " (:summary m) "\n"
       "  " "Since Redis version " (:since m)))

(defn- fn-args [m]
  (if (:arguments m)
    ['db '& 'args]
    ['db]))

(defn- create-cmd* [^String n m]
  (let [cmd-parts (seq (.split (.toUpperCase n) " ")) ;; handle 'DEBUG OBJECT'
        cmd-name (first cmd-parts)
        static-args (apply vector (rest cmd-parts))
        fn-name (.replaceAll (.toLowerCase n) " " "-")
        args (fn-args m)
        params (if (second args) ['args])
        dox (fn-docs n m)
        ]
    `(let [name# (.getBytes ~cmd-name)]
       (defn ~(symbol fn-name) ~dox ~args
         (cmd* ~'db name# ~static-args ~@params)))
    ))

(defn- create-cmd
  "Create a new fn from a redis.io documentation map"
  [n m]
  (clojure.core/eval (create-cmd* n m)))

(defn create-cmds []
  (doseq [[cmd-name cmd-def] REDIS-COMMANDS]
    ;; (println "adding " cmd-name)
    (create-cmd (name cmd-name) cmd-def)))

(defn spit-cmds [f]
  (spit f (pr-str
           (for [[cmd-name cmd-def] REDIS-COMMANDS]
             (create-cmd* (name cmd-name) cmd-def)))))

(create-cmds)


;; EVAL
;; eval "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}" 2 key1 key2 first second
(defmacro defeval-naive
  "Creates a custom EVAL command"
  [name keys args lua]
  `(defn ~name [~'db ~@keys ~@args]
     (cmd* ~'db "EVAL" [~lua ~(count keys)] [~@keys ~@args])))

(defmacro defeval
  "Creates a custom EVAL command. Scripts are sent once (SCRIPT LOAD) per connection, then ran with EVALSHA."
  ([name keys args lua]
  `(defn ~name [~'db ~@keys ~@args]
     (.eval ~'db ~lua
            (into-array java.lang.Object ~keys)
            (into-array java.lang.Object ~args))))
  ([name doc-string keys args lua]
  `(defn ~name ~doc-string [~'db ~@keys ~@args]
     (.eval ~'db ~lua
            (into-array java.lang.Object ~keys)
            (into-array java.lang.Object ~args)))))


;; PUB/SUB
(let [MESSAGE (.getBytes "message")
      SUBSCRIBE (.getBytes "subscribe")
      UNSUBSCRIBE (.getBytes "unsubscribe")
      acmp (fn [^bytes a ^bytes b] (java.util.Arrays/equals a b))]
  (defn subscribe-with
    "Listen for messages published to given channels.
  Calls handler with (handler db channel message) on messages. Handler can return false to unsubscribe all channels and make the connection available again. Runs in current thread, returns when all channels are unsubscribed (connection no longer in pub/sub special state).
  Since Redis version 1.3.8"
    [^Client db channels handler]
    (cmd** db SUBSCRIBE channels)

    (loop [subscribed-channels (count channels)]
      ;; (print subscribed-channels " SW ")
      (when (> subscribed-channels 0)
        (let [r @(.pull db)
              ^objects v (value r)
              cmd (.getValue ^Reply (aget v 0))
              channel (->str (aget v 1))
              message (aget v 2)]
          ;; (println (->str cmd) channel (->str message))
          (cond
           (acmp cmd SUBSCRIBE) (recur (int (value (aget v 2))))
           (acmp cmd UNSUBSCRIBE) (recur (int (value (aget v 2))))
           (acmp cmd MESSAGE) (do
                                (when-not (handler db channel message)
                                  (cmd** db UNSUBSCRIBE channels))
                                (recur subscribed-channels))))))))

(let [MESSAGE (.getBytes "pmessage")
      SUBSCRIBE (.getBytes "psubscribe")
      UNSUBSCRIBE (.getBytes "punsubscribe")
      acmp (fn [^bytes a ^bytes b] (java.util.Arrays/equals a b))]
  (defn psubscribe-with
    "Listen for messages published to given channels matching given patterns.
  Calls handler with (handler db channel message) on messages. Handler can return false to unsubscribe all channels and make the connection available again. Runs in current thread, returns when all channels are unsubscribed (connection no longer in pub/sub special state).
  Since Redis version 1.3.8"
    [^Client db channels handler]
    (cmd** db SUBSCRIBE channels)

    (loop [subscribed-channels (count channels)]
      ;; (print subscribed-channels " SW ")
      (when (> subscribed-channels 0)
        (let [r @(.pull db)
              ^objects v (value r)
              cmd (.getValue ^Reply (aget v 0))]
          ;; (println (->str cmd) (alength v) (->str (aget v 1)))
          (cond
           (acmp cmd SUBSCRIBE) (recur (int (value (aget v 2))))
           (acmp cmd UNSUBSCRIBE) (recur (int (value (aget v 2))))
           (acmp cmd MESSAGE) (do
                                (when-not (handler db (->str (aget v 1)) (->str (aget v 2)) (aget v 3))
                                  (cmd** db UNSUBSCRIBE channels))
                                (recur subscribed-channels))))))))

(defn example-subscribe-with [db]
  (subscribe-with
   db
   ["msgs" "msgs2" "msgs3"]
   (fn [db channel message]
     (let [message (->str message)]
       (println "R " channel message)
       (when (= "u" message)
         (cmd** db "UNSUBSCRIBE" [channel]))
       (not (= "quit" message))))))

(defn example-psubscribe-with [db]
  (psubscribe-with
   db
   ["msgs.*"]
   (fn [db pattern channel message]
     (let [message (->str message)]
       (println "R " pattern channel message)
       (not (= "quit" message))))))

;; Transactions
;; Like atomically in redis-clojure
(defmacro atomically
  "Execute all redis commands in body in a MULTI/EXEC. If an exception is thrown the
  the transaction will be closed by an DISCARD, and the exception will be rethrown.
Any exceptions thrown by DISCARD will be ignored."
  [db & body]
  `(do
     (multi ~db)
     (try
      (do
        ~@body
        (exec ~db))
      (catch Throwable e#
        ;; on DISCARD we .ensure to flush the pipeline
        (try @(discard ~db)
          (finally (throw e#)))))))

(defn exec!
  "EXEC
Execute all commands issued after MULTI.
Completes QUEUED futures with results.
Since Redis version 1.1.95"
  [^Client db]
  (.execWithResults db))

(comment
  (try
    (multi r)
    (let [_ (mset r "k1" "v1" "k2" "v2")
          a (mget r "k1" "k2")
          b (set r "k1" "xx")]
      (println @a @b) ;; QUEUED QUEUED
      (exec! r)
      (println @a @b)) ;; xx v2
    (catch Throwable t
      (try @(discard r)
           (finally (throw t)))))
  )

;; info helper
(defn info!
  "Blocking version of INFO that parses value into a map.
  For more info, see (doc info)"
  [db]
  (let [s ^String (->str @(cmd* db "INFO" []))
        i (map #(seq (.split ^String % ":")) (seq (.split s "\r\n")))
        f (filter second i)]
    (zipmap (map first f) (map second f))))


(defn -main []
  (pprint
   (let [r (client)]
    (->>str @(ping r)))))
