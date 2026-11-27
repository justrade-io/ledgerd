(ns ledgerd.nemesis
  "Fault-injection packages for LEDGERD, built on jepsen.nemesis.combined.

  J2 added network partitions, the classic Raft test. J3 adds process crashes
  (SIGKILL then restart, recovering from the archive and snapshot) and pauses
  (SIGSTOP/SIGCONT, simulating a long GC). J4 adds clock skew and snapshot
  corruption. Under every fault the conservation checker must still hold: no
  acknowledged credit is lost and none is applied twice. Snapshot corruption is
  verified differently: a corrupted snapshot must be rejected by its checksum on
  recovery, never loaded as wrong state."
  (:require [clojure.string :as str]
            [jepsen.control :as c]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [jepsen.nemesis.combined :as nc]
            [ledgerd.db :as db]))

(def ^:private fault-sets
  {"none"      #{}
   "partition" #{:partition}
   "kill"      #{:kill}
   "pause"     #{:pause}
   "clock"     #{:clock}
   "all"       #{:partition :kill :pause :clock}})

(defn- corrupt-newest-recording!
  "Kills a node, flips bytes in its newest archive recording segment (which holds
  the service snapshot), and restarts it. On recovery LEDGERD's SnapshotManager
  must reject the corrupted snapshot by its checksum rather than load wrong
  state. Runs in the target node's SSH context."
  [test node]
  (c/on-nodes test [node]
    (fn [test node]
      (c/su
        (c/exec :bash :-c
                (str "kill -9 $(cat " db/pid-file " 2>/dev/null) 2>/dev/null || true; "
                     "rm -f " db/pid-file))
        (c/exec :bash :-c
                (str "f=$(ls -t " db/data-dir "/archive/*.rec 2>/dev/null | head -1); "
                     "if [ -n \"$f\" ]; then "
                     "dd if=/dev/urandom of=\"$f\" bs=1 count=64 seek=1024 conv=notrunc 2>/dev/null; "
                     "fi; true")))
      (db/start-recover! test node))))

(defn- disk-corruption-package
  "A standalone nemesis that periodically corrupts one node's newest snapshot
  recording and forces a recovery. Not part of the combined package, because its
  outcome (checksum rejection) is verified operationally rather than through the
  conservation checker."
  [opts]
  {:nemesis (reify nemesis/Nemesis
              (setup! [this _test] this)
              (invoke! [_this test op]
                (if (= :corrupt (:f op))
                  (let [node (rand-nth (vec (:nodes test)))]
                    (corrupt-newest-recording! test node)
                    (assoc op :value node))
                  op))
              (teardown! [this _test] this))
   :generator (->> (repeat {:type :info, :f :corrupt})
                   (gen/stagger (:nemesis-interval opts 30)))
   :final-generator nil})

(defn package
  "Builds a nemesis package for the named faults. Requires :db and :nodes in
  opts. Returns {:nemesis :generator :final-generator ...}."
  [opts]
  (let [name (:nemesis opts "none")]
    (if (= name "corrupt")
      (disk-corruption-package opts)
      (let [faults (or (get fault-sets name)
                       (throw (IllegalArgumentException.
                                (str "unknown nemesis '" name "'; expected one of: "
                                     (str/join ", " (conj (keys fault-sets) "corrupt"))))))]
        (nc/nemesis-package
          {:db        (:db opts)
           :nodes     (:nodes opts)
           :faults    faults
           :partition {:targets [:majorities-ring :majority]}
           :interval  (:nemesis-interval opts 15)})))))
