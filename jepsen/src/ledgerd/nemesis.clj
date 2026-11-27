(ns ledgerd.nemesis
  "Fault-injection packages for LEDGERD, built on jepsen.nemesis.combined.

  J2 added network partitions, the classic Raft test. J3 adds process crashes
  (SIGKILL then restart, recovering from the archive and snapshot) and pauses
  (SIGSTOP/SIGCONT, simulating a long GC). Under every fault the conservation
  checker must still hold: no acknowledged credit is lost and none is applied
  twice. Clock and disk faults arrive in J4."
  (:require [clojure.string :as str]
            [jepsen.nemesis.combined :as nc]))

(def ^:private fault-sets
  {"none"      #{}
   "partition" #{:partition}
   "kill"      #{:kill}
   "pause"     #{:pause}
   "all"       #{:partition :kill :pause}})

(defn package
  "Builds a combined nemesis package for the named faults. Requires :db and
  :nodes in opts. Returns {:nemesis :generator :final-generator ...}."
  [opts]
  (let [name   (:nemesis opts "none")
        faults (or (get fault-sets name)
                   (throw (IllegalArgumentException.
                            (str "unknown nemesis '" name "'; expected one of: "
                                 (str/join ", " (keys fault-sets))))))]
    (nc/nemesis-package
      {:db        (:db opts)
       :nodes     (:nodes opts)
       :faults    faults
       :partition {:targets [:majorities-ring :majority]}
       :interval  (:nemesis-interval opts 15)})))
