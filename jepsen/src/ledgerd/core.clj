(ns ledgerd.core
  "Jepsen test entry point for LEDGERD.

  Workers credit a single account through the real cluster while a nemesis may
  inject faults, and a fault-tolerant conservation checker confirms exactly-once
  value conservation. J1 ran with no nemesis; J2 added network partitions; J3
  adds process crashes (kill then recover) and pauses. Clock and disk nemeses
  arrive in J4."
  (:gen-class)
  (:require [jepsen [checker :as checker]
                    [cli :as cli]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.os.debian :as debian]
            [ledgerd.checker :as lc]
            [ledgerd.client :as client]
            [ledgerd.db :as db]
            [ledgerd.nemesis :as nem]))

(defn- credit-op [_test _ctx]
  {:f :credit :value (inc (rand-int 100))})

(defn ledgerd-test
  "Builds the Jepsen test map from parsed CLI options."
  [opts]
  (let [db  (db/db)
        pkg (nem/package (assoc opts :db db))]
    (merge tests/noop-test
           opts
           {:name      (str "ledgerd-" (:nemesis opts "none"))
            :os        debian/os
            :db        db
            :client    (client/client)
            :nemesis   (:nemesis pkg)
            :checker   (checker/compose
                         {:conservation (lc/conservation-checker)
                          :stats        (checker/stats)
                          :perf         (checker/perf)})
            :generator (gen/phases
                         ;; Interleave client credits with nemesis fault ops for
                         ;; the run duration.
                         (gen/time-limit
                           (:time-limit opts 60)
                           (gen/nemesis
                             (:generator pkg)
                             (gen/stagger 1/50 credit-op)))
                         ;; Heal any outstanding fault so the cluster settles and
                         ;; a final credit can observe the converged balance.
                         (gen/nemesis (:final-generator pkg))
                         (gen/clients (gen/once credit-op)))})))

(def cli-opts
  "Extra CLI options for the LEDGERD test."
  [[nil "--nemesis NAME" "Fault package: none, partition, kill, pause, or all."
    :default "none"]
   [nil "--nemesis-interval SECONDS" "Seconds between nemesis fault operations."
    :default 15
    :parse-fn #(Long/parseLong %)]])

(defn -main [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  ledgerd-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
