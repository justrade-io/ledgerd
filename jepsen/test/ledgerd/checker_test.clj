(ns ledgerd.checker-test
  "Unit tests for the exactly-once conservation checker. These run without a
  cluster, so the nightly workflow can validate the checker logic alongside the
  harness compile."
  (:require [clojure.test :refer [deftest is]]
            [jepsen.checker :as checker]
            [ledgerd.checker :as lc]))

(defn- valid? [history]
  (:valid? (checker/check (lc/conservation-checker) nil history nil)))

(defn- credit-ok [amount balance]
  {:type :ok, :f :credit, :value {:amount amount, :balance balance}})

(defn- credit-info [amount]
  {:type :info, :f :credit, :value {:amount amount}})

(deftest accepts-conserved-history
  (is (valid? [(credit-ok 10 10) (credit-ok 5 15)])))

(deftest rejects-lost-acknowledged-credit
  ;; Final balance 10 is below sum of acknowledged credits 15: a committed credit
  ;; was lost.
  (is (not (valid? [(credit-ok 10 10) (credit-ok 5 10)]))))

(deftest rejects-double-applied-credit
  ;; Final balance 20 exceeds the only acknowledged credit 10: it applied twice.
  (is (not (valid? [(credit-ok 10 20)]))))

(deftest accepts-applied-indeterminate-within-bounds
  ;; An :info credit of 5 applied, so the balance 15 sits within
  ;; [sum(ok)=10, sum(ok)+sum(info)=15]; that is allowed.
  (is (valid? [(credit-ok 10 15) (credit-info 5)])))

(deftest rejects-balance-above-indeterminate-upper-bound
  ;; Balance 16 exceeds everything submitted (10 ok + 5 info): impossible under
  ;; exactly-once.
  (is (not (valid? [(credit-ok 10 16) (credit-info 5)]))))
