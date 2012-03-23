(defproject labs.redis "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojure/data.json "0.1.1"]
		 [redis.clients/jedis "2.0.0"]
                 [criterium "0.2.0"]
                 ]
  :dev-dependencies [[clojure-source "1.3.0"]]
  :java-source-path "java"
  :main labs.redis.core)