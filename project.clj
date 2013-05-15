(defproject labs.redis "0.1.1"
  :description "Redis client library"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojure/data.json "0.1.1"]]
  :dev-dependencies [[clojure-source "1.3.0"]
                     [criterium "0.2.0"]]
  :java-source-paths ["java"]
  :main labs.redis.core)
