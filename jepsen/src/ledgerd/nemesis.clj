(ns ledgerd.nemesis
  "Fault-injection packages for LEDGERD.

  Sprint 5 (J1) ships only the no-op package. Sprint 6 (J2) adds network
  partitions, the classic Raft test: a partition that splits the cluster must
  force a leader change without losing an acknowledged command or applying one
  twice, which the conservation checker verifies. Crash, pause, clock, and disk
  nemeses arrive in later sprints."
  (:require [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]))

;; Alternating start/stop ops, staggered so each partition and heal lasts long
;; enough for the cluster to react. The election window is lowered via the
;; launcher M6 timeouts, so a partition longer than that triggers a real leader
;; change.
(defn- flip-flop-gen [period-seconds]
  (->> (cycle [{:type :info, :f :start}
               {:type :info, :f :stop}])
       (gen/stagger period-seconds)))

(defn package
  "Returns {:nemesis N :generator G-or-nil :final-generator G-or-nil} for the
  named faults."
  [opts]
  (let [name (:nemesis opts "none")]
    (case name
      "none"
      {:nemesis         (nemesis/noop)
       :generator       nil
       :final-generator nil}

      "partition-halves"
      {:nemesis         (nemesis/partition-random-halves)
       :generator       (flip-flop-gen 15)
       :final-generator {:type :info, :f :stop}}

      "partition-ring"
      {:nemesis         (nemesis/partition-majorities-ring)
       :generator       (flip-flop-gen 15)
       :final-generator {:type :info, :f :stop}}

      (throw (IllegalArgumentException.
               (str "unknown nemesis '" name "'; expected one of: "
                    "none, partition-halves, partition-ring"))))))
