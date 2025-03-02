(ns think.query.query-test
  (:require [clojure.test :refer :all]
            [clojure.set :as set]
            [think.query :as q :refer [--> <--]]
            [think.query.datomic :as datomic]
            [think.query.sql :as sql]
            [think.query.test-util :as test-util]
            [think.query.test-data :as test-data]))

;; Query system unit tests
; * run migrations to create resources and users
; * generate a set of fake users we can query against
; * run queries against plain datomic user database
; * create an index or two, and then run more queries

(use-fixtures :each test-util/db-fixture)

(defn- index-by-attribute [primary-index attribute]
  "Creates a derived index from the given primary index keyed by the given attribute."
  (reduce (fn [eax entity]
            (assoc eax (attribute entity)
                   (conj (get (attribute entity) eax #{})
                         (:resource/id entity))))
          {}
          (vals primary-index)))

(defn uuid [] (str (java.util.UUID/randomUUID)))

(defn in-memory-primary-index
  [items]
  (reduce (fn [index {:keys [:resource/id] :as item}]
            (assoc index id item))
          {}
          items))

(defn filtered-users
  [ctx filters]
  (let [users (vals (:primary-index (:indexes ctx)))]
    (if filters
      (do
        ;;(println "filters: " filters)
        (filter
          (fn [user]
            (let [res (every? #(= (get user (first %)) (second %))
                              filters)]
              ;;(println res " -> " user)
              res))
          users))
      users)))

(defn users
  [ctx]
  (vals (:primary-index (:indexes ctx))))

(defn query-user
  [data-source q]
  (assert (#{:in-memory :datomic :sql-in-mem :sql-dynamic} data-source))
  (let [primary-user-index
        (condp = data-source
          :in-memory (in-memory-primary-index test-data/test-users)
          :datomic (datomic/datomic-primary-index (test-util/db) :resource.type/user)
          :sql-in-mem (sql/sql-primary-index (test-util/sql-db)
                                      :resource.type/user
                                      test-util/sql-map->user)
          :sql-dynamic nil)
        attribute-index-fn (partial index-by-attribute primary-user-index)
        indexes {:primary-index primary-user-index
                 :user/email (attribute-index-fn :user/email)
                 :user/age (attribute-index-fn :user/age)
                 :user/first-name (attribute-index-fn :user/first-name)}
        ctx (condp = data-source
              :in-memory indexes ;; Test v1 shape
              :datomic {:indexes indexes}
              :sql-in-mem {:indexes indexes}
              :sql-dynamic {:selector (partial sql/sql-selector
                                               (test-util/sql-db)
                                               :resource.type/user
                                               test-util/keyword->column-name)
                            :realizer (partial sql/sql-realizer
                                               (test-util/sql-db)
                                               :resource.type/user
                                               test-util/sql-map->user)})]
    (q/query ctx q)))

(deftest should-return-empty-set-when-no-results-on-select
  (doseq [data-source #{:in-memory :datomic :sql-in-mem :sql-dynamic}]
    (testing (str "Empty result: " data-source)
      (let [result (query-user data-source [:select {:user/email "complete garbage"}])]
        (is (= 0 (count result)))))))

(deftest select-indexed-keys
  (doseq [data-source #{:in-memory :datomic :sql-in-mem :sql-dynamic}]
    (testing (str "indexed-keys: " data-source)
      (let [email "alice@foo.com"
            result (query-user data-source [:select {:user/email email}])]
        (is (= #{(:resource/id test-data/alice)} result))))))

(deftest select-non-indexed-keys
  (doseq [data-source #{:in-memory :datomic :sql-in-mem :sql-dynamic}]
    (testing (str "non-indexed-keys: " data-source)
      (let [result (query-user data-source [:select {:user/first-name "Bob"}])]
        (is (= #{(:resource/id test-data/bob)} result))))))

(deftest select-multiple-indexed-keys
  (doseq [data-source #{:in-memory :datomic :sql-in-mem :sql-dynamic}]
    (testing (str "multiple indexed keys: " data-source)
      (let [result (query-user data-source [:select {:user/first-name "Bob"
                                                     :user/email "bob@foo.com"}])]
        (is (= #{(:resource/id test-data/bob)} result))))
    (testing (str "should use AND semantics: " data-source)
      (let [result (query-user data-source [:select {:user/first-name "Bob"
                                                     :user/email "alice@foo.com"}])]
        (is (= 0 (count result)))))))

(deftest star-query
  (doseq [data-source #{:in-memory :datomic :sql-in-mem :sql-dynamic}]
    (testing (str ":* returns all: " data-source)
      (let [result (query-user data-source [:select :*])]
        (is (= (count test-data/test-users) (count result)))))))

(defmethod q/compute-operator :email-first-letter
  [_ item]
  (first (:user/email item)))

(deftest compute-operator-test
  (doseq [data-source #{:in-memory :datomic :sql-in-mem :sql-dynamic}]
    (testing (str ":compute works: " data-source)
      (let [q (--> [:select {:user/first-name "Bob"}]
                   [:realize]
                   [:compute :email-first-letter])
            result (query-user data-source q)]
        (is (= \b (:email-first-letter (first result))))))))

(defmethod q/query-operator :email-list
  [{:keys [indexes] :as ctx} q]
  (map :user/email (q/query-operator ctx (second q))))

(deftest email-list-query-operator
  (doseq [data-source #{:in-memory :datomic :sql-in-mem :sql-dynamic}]
    (testing (str "custom query opertator: " data-source)
      (let [result (query-user data-source [:email-list [:realize [:select :*]]])]
        (is (= (sort result)
               (sort (map :user/email test-data/test-users))))))))

(defmethod q/transform-operator :email-list
  [_ data & args]
  (map :user/email data))

(deftest email-list-transform
  (doseq [data-source #{:in-memory :datomic :sql-in-mem :sql-dynamic}]
    (testing (str "transform opertator: " data-source)
      (let [result (query-user data-source [:transform [:realize [:select :*]] :email-list])]
        (is (= (sort result)
               (sort (map :user/email test-data/test-users))))))))

(deftest test-let-operator
  (doseq [data-source #{:in-memory :datomic :sql-in-mem :sql-dynamic}]
    (testing (str "let opertator: " data-source)
      (let [q '[:let [a [:realize [:select {:user/email "alice@foo.com"}]]
                      b [:realize [:select {:user/email "bob@foo.com"}]]
                      c [:hydrate a [:user/email]]
                      d [:hydrate b [:user/first-name]]]
                {:foo c :bar d :baz [c d]}]
            result (query-user data-source q)]
        (is (= '{:foo ({:user/email "alice@foo.com"})
                 :bar ({:user/first-name "Bob"})
                 :baz [({:user/email "alice@foo.com"}) ({:user/first-name "Bob"})]}
               result))))))

(defmethod q/transform-operator :count
  [_ data & args]
  (count data))

(deftest transform-let-count
  (doseq [data-source #{:in-memory :datomic :sql-in-mem :sql-dynamic}]
    (testing (str "transform opertator in a let: " data-source)
      (let [result (query-user data-source
                               '[:let [a [:realize [:select :*]]
                                       b [:transform a :count]]
                                 b])]
        (is (= (count test-data/test-users) result))))))

(defmethod q/transform-operator :identity
  [_ data & {:as args}]
  {:data data
   :args args})

(deftest transform-let-identity
  (doseq [data-source #{:in-memory :datomic :sql-in-mem :sql-dynamic}]
    (testing (str "transform opertator in a let with args: " data-source)
      (let [result (query-user data-source
                               '[:let [a [:realize [:select :*]]
                                       b [:transform a :identity :arg1 "arg1"]]
                                 b])]
        (is (= (:args result) {:arg1 "arg1"}))))))

(deftest transform-let-binding-in-arg
  (doseq [data-source #{:in-memory :datomic :sql-in-mem :sql-dynamic}]
    (testing (str "transform opertator in a let with bound args: " data-source)
      (let [result (query-user data-source
                               '[:let [arg-val "arg1"
                                       a [:realize [:select :*]]
                                       b [:transform a :identity :arg1 arg-val]]
                                 b])]
        (is (= (:args result) {:arg1 "arg1"}))))))

(deftest paginate
  (doseq [data-source #{:in-memory :datomic :sql-in-mem :sql-dynamic}]
    (testing (str "paginate in let: " data-source)
      (let [result (query-user data-source
                               '[:let [a [:realize [:select :*]]
                                       b [:paginate a {:offset 0 :limit 1}]]
                                 b])]
        (is (= (count result) 1))))))

(deftest let-data
  (doseq [data-source #{:in-memory :datomic :sql-in-mem :sql-dynamic}]
    (testing (str "return a collection from let: " data-source)
      (let [[a b c] (query-user data-source
                                '[:let [a [1 2 3]
                                        b {:a 1
                                           :b 2
                                           :c 3}
                                        c "X"]
                                  [a b c]])]
        (is (= a [1 2 3]))
        (is (= b {:a 1 :b 2 :c 3}))
        (is (= c "X"))))))

(deftest sort-test
  (doseq [data-source #{:in-memory :datomic :sql-in-mem :sql-dynamic}]
    (testing (str "An example of using sort: " data-source)
      (let [result (query-user data-source
                               (--> [:select :*]
                                    [:realize]
                                    [:sort {:path [:user/age]
                                            :direction :descending}]
                                    [:hydrate [:user/age]]))
            ages (map :user/age result)]
        (is (apply >= ages))))))

(deftest weighted-sort-test
  (doseq [data-source #{:in-memory :datomic :sql-in-mem :sql-dynamic}]
    (testing (str "An example of using weighted-sort on a single attribute: " data-source)
      (->> (query-user data-source
                       (--> [:select :*]
                            [:realize]
                            [:weighted-sort [{:path [:user/age]
                                              :weight 1.0
                                              :direction :descending}]]
                            [:hydrate [:user/age]]))
           (map :user/age)
           (apply >=)
           (is)))))

(deftest date-filter-test
  (doseq [data-source #{:in-memory :datomic :sql-in-mem :sql-dynamic}]
    (testing (str "An example of using filter on dates: " data-source)
      (let [creation-times (->> (query-user data-source
                                            (--> [:select :*]
                                                 [:realize]
                                                 [:sort {:path [:user/created]
                                                         :direction :ascending}]
                                                 [:hydrate [:user/created]]))
                                (map :user/created))
            newer-users (->> (query-user data-source
                                         (--> [:select :*]
                                              [:realize]
                                              [:filter [[:user/created] :> (first creation-times)]]
                                              [:hydrate [:user/created]]))
                             (map :user/created))]
        (is (= (dec (count creation-times)) (count newer-users)))))))

(deftest filter-gt-test
  (doseq [data-source #{:in-memory :datomic :sql-in-mem :sql-dynamic}]
    (testing (str "An example of using filter on a single attribute: " data-source)
      (->> (query-user data-source
                       (--> [:select :*]
                            [:realize]
                            [:filter [[:user/age] :> 25]]
                            [:hydrate [:user/age]]))
           (map :user/age)
           (every? #(> % 25))
           (is)))))

(deftest filter-equal-test
  (doseq [data-source #{:in-memory :datomic :sql-in-mem :sql-dynamic}]
    (testing (str "An example of using filter on a single attribute: " data-source)
      (->> (query-user data-source
                       (--> [:select :*]
                            [:realize]
                            [:filter [[:user/first-name] := "Bob"]]
                            [:hydrate [:user/first-name]]))
           (map :user/first-name)
           (every? #(= % "Bob"))
           (is)))))

(deftest filter-not-equal-test
  (doseq [data-source #{:in-memory :datomic :sql-in-mem :sql-dynamic}]
    (testing (str "An example of using filter on a single attribute: " data-source)
      (->> (query-user data-source
                       (--> [:select :*]
                            [:realize]
                            [:filter [[:user/first-name] :not= "Bob"]]
                            [:hydrate [:user/first-name]]))
           (map :user/first-name)
           (every? #(not= % "Bob"))
           (is)))))

(deftest filter-not-test
  (doseq [data-source #{:in-memory :datomic :sql-in-mem :sql-dynamic}]
    (testing (str "An example of using not filter on a single attribute: " data-source)
      (->> (query-user data-source
                       (--> [:select :*]
                            [:realize]
                            [:filter [:not [[:user/first-name] := "Bob"]]]
                            [:hydrate [:user/first-name]]))
           (map :user/first-name)
           (every? #(not= % "Bob"))
           (is)))))

(deftest filter-contains-test
  (doseq [data-source #{:in-memory :datomic :sql-in-mem :sql-dynamic}]
    (testing (str "An example of using filter on a single attribute: " data-source)
      (->> (query-user data-source
                       (--> [:select :*]
                            [:realize]
                            [:filter [[:user/first-name] :contains "Bo"]]
                            [:hydrate [:user/first-name]]))
           (map :user/first-name)
           (every? #(= % "Bob"))
           (is)))))

(deftest filter-set-contains-test
  (doseq [data-source #{:in-memory :datomic :sql-in-mem :sql-dynamic}]
    (testing (str "An example of using filter on a single attribute: " data-source)
      (->> (query-user data-source
                       (--> [:select :*]
                            [:realize]
                            [:filter [[:user/friends] :contains "Alice"]]
                            [:hydrate [:user/first-name]]))
           (map :user/first-name)
           (every? #(= % "Bob"))
           (is)))))

(deftest filter-set-contains-mismatch-test
  (doseq [data-source #{:in-memory :datomic :sql-in-mem :sql-dynamic}]
    (testing (str "An example of using filter on a single attribute: " data-source)
      (->> (query-user data-source
                       (--> [:select :*]
                            [:realize]
                            [:filter [[:user/friends] :contains "Bob"]]
                            [:hydrate [:user/first-name]]))
           (empty?)
           (is)))))

(deftest filter-contains-or-test
  (doseq [data-source #{:in-memory :datomic :sql-in-mem :sql-dynamic}]
    (testing (str "An example of using filter on a single attribute: " data-source)
      (->> (query-user data-source
                       (--> [:select :*]
                            [:realize]
                            [:filter [:or [[:user/first-name] :contains "Bo"]
                                      [[:user/first-name] :contains "Alic"]]]
                            [:hydrate [:user/first-name]]))
           (map :user/first-name)
           (every? #(or (= % "Bob") (= % "Alice")))
           (is)))))

(deftest select-non-indexed
  (doseq [data-source #{:in-memory :datomic :sql-in-mem :sql-dynamic}]
    (testing (str "An example of using select on a non-indexed attribute: " data-source)
      (->> (query-user data-source
                       (--> [:select {:user/last-name "Foo"}]
                            [:realize]
                            [:hydrate [:user/first-name]]))
           (map :user/first-name)
           (every? #(or (= % "Bob") (= % "Alice")))
           (is))
      (->> (query-user data-source
                       (--> [:select {:user/last-name "Foo"}]
                            [:realize]
                            [:hydrate [:user/first-name]]))
           (map :user/first-name)
           (every? #(or (= % "Bob") (= % "Alice")))
           (is)))))

(deftest select-non-indexed-and-indexed
  (doseq [data-source #{:in-memory :datomic :sql-in-mem :sql-dynamic}]
    (testing (str "An example of using select on a non-indexed _and_ an indexed attribute: " data-source)
      (->> (query-user data-source
                       (--> [:select {:user/last-name "Foo"
                                      :user/first-name "Bob"}]
                            [:realize]
                            [:hydrate [:user/first-name]]))
           (map :user/first-name)
           (first)
           (= "Bob")
           (is))
      (->> (query-user data-source
                       (--> [:select {:user/last-name "Foo"
                                      :user/first-name "Bob"}]
                            [:realize]
                            [:hydrate [:user/first-name]]))
           (map :user/first-name)
           (first)
           (= "Bob")
           (is)))))

(deftest select-not
  (let [u1 (java.util.UUID/randomUUID)
        u2 (java.util.UUID/randomUUID)
        indexes {:primary-index {u1 {:x 0
                                     :resource/id u1
                                     :resource/type :resource.type/foo}
                                 u2 {:x 1
                                     :resource/id u2
                                     :resource/type :resource.type/foo}}}
        q1 (q/query {:indexes indexes} [:select {:x 0}])
        q2 (q/query {:indexes indexes} [:select {:x [:not 0]}])]
    (is (= 1 (count q1)))
    (is (= 1 (count q2)))
    (is (= u1 (first q1)))
    (is (= u2 (first q2)))))

(deftest select-not-with-reverse-index
  (let [u1 (java.util.UUID/randomUUID)
        u2 (java.util.UUID/randomUUID)
        indexes {:primary-index {u1 {:x 0
                                     :resource/id u1
                                     :resource/type :resource.type/foo}
                                 u2 {:x 1
                                     :resource/id u2
                                     :resource/type :resource.type/foo}}
                 :x {0 #{u1}
                     1 #{u2}}}
        q1 (q/query {:indexes indexes} [:select {:x 0}])
        q2 (q/query {:indexes indexes} [:select {:x [:not 0]}])]
    (is (= 1 (count q1)))
    (is (= 1 (count q2)))
    (is (= u1 (first q1)))
    (is (= u2 (first q2)))))

(deftest select-not-or
  (let [u1 (java.util.UUID/randomUUID)
        u2 (java.util.UUID/randomUUID)
        u3 (java.util.UUID/randomUUID)
        indexes {:primary-index {u1 {:x 0
                                     :resource/id u1
                                     :resource/type :resource.type/foo}
                                 u2 {:x 1
                                     :resource/id u2
                                     :resource/type :resource.type/foo}
                                 u3 {:x 2
                                     :resource/id u3
                                     :resource/type :resource.type/foo}}}
        result (q/query {:indexes indexes} [:select {:x [:not [:or 0 1]]}])]
    (is (= 1 (count result)))
    (is (= u3 (first result)))))

(deftest select-not-or-with-reverse-index
  (let [u1 (java.util.UUID/randomUUID)
        u2 (java.util.UUID/randomUUID)
        u3 (java.util.UUID/randomUUID)
        indexes {:primary-index {u1 {:x 0
                                     :resource/id u1
                                     :resource/type :resource.type/foo}
                                 u2 {:x 1
                                     :resource/id u2
                                     :resource/type :resource.type/foo}
                                 u3 {:x 2
                                     :resource/id u3
                                     :resource/type :resource.type/foo}}
                 :x {0 #{u1}
                     1 #{u2}
                     2 #{u3}}}
        result (q/query {:indexes indexes} [:select {:x [:not [:or 0 1]]}])]
    (is (= 1 (count result)))
    (is (= u3 (first result)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Unit tests for functions other than `q/query`
(deftest test-hydration
  (let [data {:a 2 :b {:c 23 :d 32} :z 23 :children [{:sku 1} {:sku 2} {:sku 3} {:sku 4}]}]
    (is (= data (q/hydrate data '[*])))
    (is (= {:a 2} (q/hydrate data '[:a])))
    (is (= {:b (:b data)} (q/hydrate data '[:b])))
    (is (= (select-keys data [:a :b]) (q/hydrate data '[:a :b])))
    (is (= {:b {:c 23}} (q/hydrate data '[{:b [:c]}])))
    (is (= {:b (:b data)} (q/hydrate data '[{:b [*]}])))
    (is (= {:children [{:sku 1} {:sku 2} {:sku 3} {:sku 4}]} (q/hydrate data [{:children [:sku]}])))
    (is (= {:b {:d 32} :z 23} (q/hydrate data '[{:b [:d]} :z])))))

(deftest mix-test
  (is (= '(:b :b :b :a :a :a) (q/mix-sampler [[0.1 [:a :a :a]] [0.5 [:b :b :b]]])))
  (is (= '(:b :b :b :a :a :a) (q/mix-sampler [[0.1 [:a :a :a]] [0.5 [:b :b :b]] [99 []]]))))

(deftest arrow-both-ways
  (let [q [[:select :*]
           [:realize]
           [:filter [:or [[:user/first-name] :contains "Bo"]
                     [[:user/first-name] :contains "Al"]]]
           [:hydrate [:user/first-name]]]
        threaded (reduce --> q)
        dethreaded (vec (<-- threaded))]
    (is (= q dethreaded))))
