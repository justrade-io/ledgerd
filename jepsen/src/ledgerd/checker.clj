(ns ledgerd.checker
  "Checkers for LEDGERD histories."
  (:require [jepsen.checker :as checker]))

(defn conservation-checker
  "Exactly-once value conservation for the single-account credit counter, robust
  to faults.

  The account starts empty and every credit adds its amount. A credit that
  returns :ok committed and must appear in the final balance exactly once. A
  credit that returns :info (timeout or leader change) is indeterminate: under
  exactly-once semantics it applied either zero or one time, never more. So the
  final balance must lie within:

    sum(:ok amounts)  <=  final balance  <=  sum(:ok amounts) + sum(:info amounts)

  The balance a credit returns is LEDGERD's post-apply state in log order, which
  is the linearizable observation point (plain reads are served by the
  eventually-consistent read replica). The highest balance any :ok credit
  observed is the final balance, provided at least one :ok credit lands after the
  last fault heals.

  A balance below the lower bound means an acknowledged credit was lost; a
  balance above the upper bound means a credit was applied more than once. Either
  breaks exactly-once."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [credit?     #(= :credit (:f %))
            oks         (->> history (filter #(= :ok (:type %)))   (filter credit?) (map :value))
            infos       (->> history (filter #(= :info (:type %))) (filter credit?) (map :value))
            ok-total    (reduce + 0 (map :amount oks))
            info-total  (reduce + 0 (map :amount infos))
            balances    (keep :balance oks)
            max-balance (reduce max 0 balances)
            missing     (count (filter #(nil? (:balance %)) oks))
            lower       ok-total
            upper       (+ ok-total info-total)]
        {:valid?           (and (<= lower max-balance upper)
                                (zero? missing))
         :ok-credits       (count oks)
         :info-credits     (count infos)
         :ok-total         ok-total
         :info-total       info-total
         :max-balance      max-balance
         :bounds           [lower upper]
         :missing-balances missing}))))
