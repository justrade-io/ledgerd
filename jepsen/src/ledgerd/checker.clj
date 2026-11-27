(ns ledgerd.checker
  "Checkers for LEDGERD histories."
  (:require [jepsen.checker :as checker]))

(defn conservation-checker
  "Value-conservation check for the single-account credit counter.

  The account starts empty and every successful credit adds its amount exactly
  once, so with no faults the total of all :ok credit amounts must equal the
  highest balance any credit observed (the final balance), and no credit may
  report a balance below the running total. A lost or double-applied credit
  breaks one of those equalities. The balance a credit returns is LEDGERD's
  post-apply state in log order, which is the linearizable observation point
  (plain reads are served by the eventually-consistent read replica)."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [credits (->> history
                         (filter #(= :ok (:type %)))
                         (filter #(= :credit (:f %)))
                         (map :value))
            total   (reduce + 0 (map :amount credits))
            balances (keep :balance credits)
            max-balance (reduce max 0 balances)
            ok-count (count credits)
            missing-balances (count (filter #(nil? (:balance %)) credits))]
        {:valid?          (and (= total max-balance)
                               (zero? missing-balances))
         :ok-credits      ok-count
         :total-credited  total
         :max-balance     max-balance
         :missing-balances missing-balances}))))
