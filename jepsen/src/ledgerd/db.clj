(ns ledgerd.db
  "Jepsen DB lifecycle for a LEDGERD cluster node.

  Implements the Jepsen DB, Process, and Pause protocols so the combined nemesis
  can start, kill, pause, resume, and restart nodes. The initial start wipes
  prior state (cleanStart=true); a restart after a kill preserves it
  (cleanStart=false) so the node recovers from its archive and snapshot and
  catches up, which is exactly the crash-recovery path under test.

  Assumes each DB node already has a JDK on PATH and that the launcher
  distribution tar was built locally before the run with:
    ./gradlew :launcher:distTar"
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen.control :as c]
            [jepsen.control.util :as cu]
            [jepsen.db :as db]))

(def install-dir "/opt/ledgerd")
(def data-dir (str install-dir "/data"))
(def lib-dir (str install-dir "/lib"))
(def props-file (str install-dir "/node.properties"))
(def pid-file (str install-dir "/ledgerd.pid"))
(def log-file (str install-dir "/ledgerd.log"))

(def port-base 20100)
(def port-stride 100)
(def metrics-base 9100)

(defn- member-entry [i node]
  (let [p (+ port-base (* i port-stride))]
    (str i "," node ":" p
         "," node ":" (+ p 1)
         "," node ":" (+ p 2)
         "," node ":" (+ p 3)
         "," node ":" (+ p 4))))

(defn- cluster-members [nodes]
  (->> nodes (map-indexed member-entry) (str/join "|")))

(defn node-id [test node]
  (.indexOf (vec (:nodes test)) node))

(defn- metrics-port [test node]
  (+ metrics-base (node-id test node)))

(defn- start-node!
  "Launches the node daemon. When clean-start? is false the node preserves and
  recovers its prior archive and cluster state. Election and heartbeat timeouts
  are lowered via the launcher M6 knobs so failover is fast enough for a nemesis
  cycle while still letting the cluster make progress."
  [test node clean-start?]
  (let [id (node-id test node)
        mport (metrics-port test node)]
    (c/su
      (cu/start-daemon!
        {:chdir install-dir
         :logfile log-file
         :pidfile pid-file}
        "java"
        "--add-opens" "java.base/jdk.internal.misc=ALL-UNNAMED"
        "--add-opens" "java.base/sun.nio.ch=ALL-UNNAMED"
        (str "-Dledgerd.nodeId=" id)
        (str "-Dledgerd.cleanStart=" clean-start?)
        (str "-Dledgerd.metricsPort=" mport)
        "-Dledgerd.leaderHeartbeatTimeoutNs=2000000000"
        "-Dledgerd.leaderHeartbeatIntervalNs=500000000"
        "-Dledgerd.electionTimeoutNs=1500000000"
        "-cp" (str lib-dir "/*")
        "io.justrade.ledgerd.launcher.ClusterLauncher"
        (str "--config=" props-file)))))

(defn- pid-expr [] (str "$(cat " pid-file " 2>/dev/null)"))

(defn- signal!
  "Best-effort signal to the node PID; tolerates an already-dead process."
  [sig]
  (c/su (c/exec :bash :-c (str "kill -" sig " " (pid-expr) " 2>/dev/null || true"))))

(defn- await-healthz [test node]
  (let [port (metrics-port test node)]
    (c/exec :bash :-c
            (str "for i in $(seq 1 90); do "
                 "curl -sf http://localhost:" port "/healthz >/dev/null 2>&1 && exit 0; "
                 "sleep 1; done; exit 1"))))

(defn db
  "Returns a Jepsen DB that installs and runs one LEDGERD node per test node, and
  supports kill/restart and pause/resume for the combined nemesis."
  []
  (reify db/DB
    (setup! [_ test node]
      (info node "installing LEDGERD")
      (c/su
        (c/exec :mkdir :-p install-dir data-dir)
        ;; Ship the locally-built launcher distribution (bin + lib) to the node.
        (c/upload "launcher/build/distributions/launcher.tar" "/tmp/launcher.tar")
        (c/exec :tar :xf "/tmp/launcher.tar" :-C install-dir :--strip-components "1")
        (c/exec :bash :-c
                (str "printf '%s\\n%s\\n%s\\n' "
                     "'ledgerd.clusterMembers=" (cluster-members (:nodes test)) "' "
                     "'ledgerd.baseDir=" data-dir "' "
                     "'ledgerd.host=" node "' > " props-file)))
      (start-node! test node true)
      (await-healthz test node))

    (teardown! [_ test node]
      (info node "tearing down LEDGERD")
      (c/su
        (cu/stop-daemon! pid-file)
        (c/exec :rm :-rf install-dir)))

    db/LogFiles
    (log-files [_ _test _node]
      [log-file])

    db/Process
    (start! [_ test node]
      ;; Restart after a kill: recover from the node's own archive and snapshot.
      (start-node! test node false)
      (await-healthz test node))

    (kill! [_ _test _node]
      (c/su
        (c/exec :bash :-c (str "kill -9 " (pid-expr) " 2>/dev/null || true"))
        (c/exec :rm :-f pid-file)))

    db/Pause
    (pause! [_ _test _node]
      (signal! "STOP"))

    (resume! [_ _test _node]
      (signal! "CONT"))))
