(ns ledgerd.core
  "Jepsen test entry point for LEDGERD.

  Sprint 5 (J1) runs a no-nemesis smoke test: Jepsen workers credit a single
  account through the real cluster and a conservation checker confirms the
  history is well-formed and value is conserved. Later sprints add partition,
  crash, pause, clock, and disk nemeses on top of this harness."
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
  (let [pkg (nem/package opts)]
    (merge tests/noop-test
           opts
           {:name      "ledgerd"
            :os        debian/os
            :db        (db/db)
            :client    (client/client)
            :nemesis   (:nemesis pkg)
            :checker   (checker/compose
                         {:conservation (lc/conservation-checker)
                          :stats        (checker/stats)
                          :perf         (checker/perf)})
            :generator (gen/clients
                         (gen/time-limit
                           (:time-limit opts 60)
                           (gen/stagger 1/50 credit-op)))})))

(def cli-opts
  "Extra CLI options for the LEDGERD test."
  [[nil "--nemesis NAME" "Fault package to run; J1 supports only 'none'."
    :default "none"]])

(defn -main [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  ledgerd-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
