(ns think.query.refactor
  (:require [clojure.walk :as walk]))

(defn update-operator
  "Update one or more operators in a query q of type op by calling the function
  f on the current operator and replacing it with the result.

  (update-operator [:compute [:realize [:select :*]] :retail-value]
    :select
    (fn [x] [:select :THIS-IS-A-NEW-OP]))

     ; => [:compute [:realize [:select :THIS-IS-A-NEW-OP]] :retail-value]
  "
  [q op f]
  (walk/postwalk
   (fn [node]
     (if (and (vector? node) (= op (first node)))
       (f node)
       node))
   q))

(defn add-selection-for-key
  "Add a vector of tags to the select query operator(s) in a query.

    (add-selection-for-key [:select {:product-intelligence/weighted-tags [:or \"burberry\" \"DVG\"]}]
     :product-intelligence/weighted-tags [\"female\" \"tops\"])

  => [:select {:product-intelligence/weighted-tags [:and [:or \"burberry\" \"DVG\"] \"female\" \"tops\"] }]
  "
  [q skey tags]
  (update-operator q :select
                   (fn [[op selection]]
                     (let [selection (if (= :* selection) {} selection)
                           cur-tags  (skey selection)
                           new-tags (vec (if (nil? cur-tags)
                                           (concat [:and] tags)
                                           (concat [:and cur-tags] tags)))]
                       [:select (assoc selection skey new-tags)]))))
