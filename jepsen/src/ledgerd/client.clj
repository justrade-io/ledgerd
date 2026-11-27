(ns ledgerd.client
  "Jepsen client for LEDGERD, driving the real write-client over Java interop.

  Each Jepsen worker owns its own WriteClient with a unique clientId, so the
  per-client dedup keys (clientId, clientSeq) never collide across workers. The
  client runs in raw single-shot mode: a command is sent exactly once and an
  unacknowledged command surfaces as an indeterminate (:info) result rather than
  being masked by an idempotent retry, which is what a linearizability-style
  checker needs to observe."
  (:require [clojure.string :as str]
            [jepsen.client :as client])
  (:import (io.justrade.ledgerd.write.client WriteClient ResultHandler)
           (io.justrade.ledgerd.write.client.config ClientConfig)
           (io.justrade.ledgerd.protocol CommandType StatusCode)
           (java.util.concurrent TimeUnit)))

;; The single account whose balance is the counter under test for J1.
(def ^:private account 100)

;; Assigns a unique clientId to each opened worker client.
(def ^:private client-id-seq (atom 0))

(defn- ingress-endpoints
  "Builds the Aeron cluster ingress endpoint list id=host:port from the test's
  node order, matching ClusterConfig's PORT_BASE 20100 and PORT_STRIDE 100."
  [test]
  (->> (:nodes test)
       (map-indexed (fn [i node] (str i "=" node ":" (+ 20100 (* i 100)))))
       (str/join ",")))

(defn- make-handler [results expired]
  (reify ResultHandler
    (onResult [_ _id-hi id-lo status balance has-balance _allowance _has-allowance]
      (swap! results assoc id-lo {:status status
                                  :balance (when has-balance balance)}))
    (onExpired [_ _id-hi id-lo]
      (swap! expired conj id-lo))))

(defn- await-result
  "Drives the client's poll loop until the command resolves (result or expiry)
  or the deadline passes, returning :ok / :fail / :info enriched op data. Every
  outcome carries {:amount ...} so the checker sees a uniform value shape."
  [^WriteClient wc results expired id-lo amount op timeout-ms]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (.poll wc)
      (cond
        (contains? @results id-lo)
        (let [{:keys [status balance]} (get @results id-lo)]
          (if (= StatusCode/SUCCESS status)
            (assoc op :type :ok :value {:amount amount :balance balance})
            (assoc op :type :fail :value {:amount amount} :error (str status))))

        (contains? @expired id-lo)
        (assoc op :type :info :value {:amount amount} :error :timeout)

        (< (System/currentTimeMillis) deadline)
        (recur)

        :else
        (assoc op :type :info :value {:amount amount} :error :timeout)))))

(defrecord LedgerdClient [^WriteClient wc results expired op-timeout-ms]
  client/Client
  (open! [this test _node]
    (let [results (atom {})
          expired (atom #{})
          client-id (swap! client-id-seq inc)
          config (-> (ClientConfig/builder client-id (ingress-endpoints test))
                     (.rawSingleShot true)
                     (.messageTimeoutNs (.toNanos TimeUnit/SECONDS 5))
                     (.build))
          wc (WriteClient. config (make-handler results expired))]
      (assoc this :wc wc :results results :expired expired)))

  (setup! [_this _test])

  (invoke! [_this _test op]
    (case (:f op)
      :credit
      (let [amount (long (:value op))
            id-lo (.submit wc CommandType/CREDIT
                           (long account) (long 0) (long 0) amount)]
        (await-result wc results expired id-lo amount op op-timeout-ms))))

  (teardown! [_this _test])

  (close! [_this _test]
    (when wc
      (.close wc))))

(defn client []
  (map->LedgerdClient {:op-timeout-ms 10000}))
