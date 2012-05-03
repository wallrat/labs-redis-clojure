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

(def ^:private byte-array-class (Class/forName "[B"))

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
  ([] (client {}))
  ([{:keys [host port timeout]
     :or {host "localhost" port 6379 timeout 10000}
     :as opts}]
     (Client. ^String host ^int port ^int timeout)))

(defn pool
  "Creates and returns a pool of Redis clients"
  ([] (pool {}))
  ([{:keys [host port test-on-borrow]
     :or {host "localhost" port 6379 test-on-borrow false}
     :as opts}]
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


(defn ->str
  "Coerces reply into a String."
  [reply]
  (when reply
    (condp instance? reply
      byte-array-class (String. ^bytes reply)
      BulkReply (let [bs (.bytes ^BulkReply reply)] (when bs (String. bs)))
      java.lang.Object (.toString ^java.lang.Object reply))))

(defn ->strs [reply]
  (when reply
    (map ->str (value reply))))

(defn ->>str [r]
  (when r
    (condp instance? r
      MultiBulkReply (map ->>str (value r))
      Reply (->>str (value r))
      java.lang.Object (->str r))))

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
   (instance? byte-array-class v) v
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
  Calls handler with (handler db pattern channel message) on messages. Handler can return false to unsubscribe all channels and make the connection available again. Runs in current thread, returns when all channels are unsubscribed (connection no longer in pub/sub special state).
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
