(ns ledgerd.nemesis
  "Fault-injection packages for LEDGERD. Sprint 5 (J1) ships only the no-op
  package so the harness, history, and checker can be validated without faults;
  partition, crash, pause, clock, and disk nemeses arrive in later sprints."
  (:require [jepsen.nemesis :as nemesis]))

(defn package
  "Returns {:nemesis <Nemesis> :generator <gen-or-nil>} for the named faults."
  [opts]
  (let [name (:nemesis opts "none")]
    (case name
      "none" {:nemesis (nemesis/noop)
              :generator nil}
      (throw (IllegalArgumentException.
               (str "unknown nemesis '" name "'; J1 supports only 'none'"))))))
