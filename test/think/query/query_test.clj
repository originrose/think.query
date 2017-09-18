(ns think.query.query-test
  (:require [clojure.test :refer :all]
            [clojure.set :as set]
            [think.query :as q :refer [--> <--]]
            [think.query.datomic :as query.datomic]
            [think.query.test-util :as test-util]
            [think.query.test-data :as test-data]))

;; Query system unit tests
; * run migrations to create resources and users
; * generate a set of fake users we can query against
; * run queries against plain datomic user database
; * create an index or two, and then run more queries

(defn- resource-id-lookup
  [rid]
  #{rid})

(use-fixtures :each test-util/with-db)

(defn- index-by-attribute [primary-index attribute]
  "Creates a derived index from the given primary index keyed by the given
  attribute."
  (reduce (fn [eax entity]
            (assoc eax (attribute entity)
                   (conj (get (attribute entity) eax #{})
                         (:resource/id entity))))
          {}
          (vals primary-index)))

(defn uuid [] (str (java.util.UUID/randomUUID)))

(defn user-primary-index
  [users]
  (reduce (fn [index user]
            (assoc index (uuid) user))
          {}
          users))

(defn filtered-users
  [ctx filters]
  (let [users (vals (:primary-index (:indexes ctx)))]
    (if filters
      (do
        ;(println "filters: " filters)
        (filter
          (fn [user]
            (let [res (every? #(= (get user (first %)) (second %))
                              filters)]
              ;(println res " -> " user)
              res))
          users))
      users)))

(defn users
  [ctx]
  (vals (:primary-index (:indexes ctx))))

(defn query-user
  [q & {:keys [:use-datomic]
        :or {:use-datomic true}}]
  (let [primary-user-index
        (if use-datomic
          (query.datomic/default-datomic-index (test-util/db) :resource.type/user)
          (user-primary-index test-data/test-users))
        attribute-index-fn (partial index-by-attribute primary-user-index)]
    (q/query {:api {:users users
                    :filtered-users filtered-users}
              :indexes (merge {:primary-index primary-user-index
                               :user/email (attribute-index-fn :user/email)
                               :user/age (attribute-index-fn :user/age)
                               :user/first-name (attribute-index-fn :user/first-name)}
                              (if use-datomic
                                {:default-index :datomic
                                 :db (test-util/db)}))} q)))


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


(deftest should-return-empty-set-when-no-results-on-select
  (let [result (query-user [:select {:user/email "complete garbage"}])]
    (is (= 0 (count result)))))

(deftest select-indexed-keys
  (testing "should work for indexed-keys"
    (let [email "alice@foo.com"
          result (query-user [:select {:user/email email}])]
      (is (= #{(test-util/user->resource-id test-data/alice)} result)))))

(deftest select-non-indexed-keys
  (testing "should work for non-indexed-keys"
    (let [result (query-user [:select {:user/first-name "Bob"}])]
      (is (= 1 (count result)))
      (is (= #{(test-util/user->resource-id test-data/bob)} result)))))

(deftest select-multiple-indexed-keys
  (testing "should work for multiple keys"
    (let [result (query-user [:select {:user/first-name "Bob"
                                       :user/email "bob@foo.com"}])]
      (is (= 1 (count result)))
      (is (= #{(test-util/user->resource-id test-data/bob)} result))))
  (testing "should use AND semantics"
    (let [result (query-user [:select {:user/first-name "Bob"
                                       :user/email "alice@foo.com"}])]
      (is (= 0 (count result))))))

(deftest star-query
  (testing "should return all users"
    (let [result (query-user [:select :*])]
      (is (= (count test-data/test-users) (count result))))))

(defmethod q/compute-operator :email-first-letter
  [_ item]
  (first (:user/email item)))

(deftest compute-operator-test
  (let [res (query-user [:compute [:realize [:select {:user/first-name "Bob"}]]
                         :email-first-letter])]
    (is (= \b
           (:email-first-letter (first res))))))

(defmethod q/query-operator :email-list
  [{:keys [indexes] :as ctx} q]
  (map :user/email (q/query-operator ctx q)))

(deftest email-list-transform
  (let [result (query-user [:email-list [:realize [:select :*]]])]
    (is (= (sort result)
           (sort (map :user/email test-data/test-users))))))

(defmethod q/transform-operator :email-list
  [_ data & args]
  (map :user/email data))

(deftest email-list-transform
  (let [result (query-user [:transform [:realize [:select :*]] :email-list])]
    (is (= (sort result)
           (sort (map :user/email test-data/test-users))))))

(deftest test-let-operator
  (let [result (query-user '[:let [a [:realize [:select {:user/email "alice@foo.com"}]]
                                   b [:realize [:select {:user/email "bob@foo.com"}]]
                                   c [:hydrate a [:user/email]]
                                   d [:hydrate b [:user/first-name]]]
                             {:foo c :bar d :baz [c d]}])]
    (is (= '{:foo ({:user/email "alice@foo.com"})
             :bar ({:user/first-name "Bob"})
             :baz [({:user/email "alice@foo.com"}) ({:user/first-name "Bob"})]}
           result))))



(defmethod q/transform-operator :count
  [_ data & args]
  (count data))

(deftest transform-let-count
  (let [result (query-user '[:let [a [:realize [:select :*]]
                                   b [:transform a :count]]
                             b])]
    (is (= 52 result))))

(defmethod q/transform-operator :identity
  [_ data & {:as args}]
  {:data data
   :args args})

(deftest transform-let-identity
  (let [result (query-user '[:let [a [:realize [:select :*]]
                                   b [:transform a :identity :arg1 "arg1"]]
                             b])]
    (is (= (:args result) {:arg1 "arg1"}))))

(deftest transform-let-binding-in-arg
  (let [result (query-user '[:let [arg-val "arg1"
                                   a [:realize [:select :*]]
                                   b [:transform a :identity :arg1 arg-val]]
                             b])]
    (is (= (:args result) {:arg1 "arg1"}))))

(deftest paginate
  (let [result (query-user '[:let [a [:realize [:select :*]]
                                   b [:paginate a {:offset 0 :limit 1}]]
                             b])]
    (is (= (count result) 1))))

(deftest let-data
  (let [[a b c] (query-user '[:let [a [1 2 3]
                                   b {:a 1
                                      :b 2
                                      :c 3}
                                   c "X"]
                             [a b c]])]
    (is (= a [1 2 3]))
    (is (= b {:a 1 :b 2 :c 3}))
    (is (= c "X"))))

(deftest mix-test
  (is (= '(:b :b :b :a :a :a) (q/mix-sampler [[0.1 [:a :a :a]] [0.5 [:b :b :b]]])))
  (is (= '(:b :b :b :a :a :a) (q/mix-sampler [[0.1 [:a :a :a]] [0.5 [:b :b :b]] [99 []]]))))

(deftest sort-test
  (testing "An example of using sort."
    (let [res (query-user (--> [:select :*]
                               [:realize]
                               [:sort {:path [:user/age]
                                       :direction :descending}]
                               [:hydrate [:user/age]]))
          ages (map :user/age res)]
         (is (apply >= ages)))))

(deftest weighted-sort-test
  (testing "An example of using weighted-sort on a single attribute."
    (->> (query-user (--> [:select :*]
                          [:realize]
                          [:weighted-sort [{:path [:user/age]
                                            :weight 1.0
                                            :direction :descending}]]
                          [:hydrate [:user/age]]))
         (map :user/age)
         (apply >=)
         (is))))

(deftest filter-gt-test
  (testing "An example of using filter on a single attribute."
    (->> (query-user (--> [:select :*]
                          [:realize]
                          [:filter [[:user/age] :> 25]]
                          [:hydrate [:user/age]]))
         (map :user/age)
         (every? #(> % 25))
         (is))))

(deftest filter-equal-test
  (testing "An example of using filter on a single attribute."
    (->> (query-user (--> [:select :*]
                          [:realize]
                          [:filter [[:user/first-name] := "Bob"]]
                          [:hydrate [:user/first-name]]))
         (map :user/first-name)
         (every? #(= % "Bob"))
         (is))))

(deftest filter-not-equal-test
  (testing "An example of using filter on a single attribute."
    (->> (query-user (--> [:select :*]
                          [:realize]
                          [:filter [[:user/first-name] :not= "Bob"]]
                          [:hydrate [:user/first-name]]))
         (map :user/first-name)
         (every? #(not= % "Bob"))
         (is))))

(deftest filter-contains-test
  (testing "An example of using filter on a single attribute."
    (->> (query-user (--> [:select :*]
                          [:realize]
                          [:filter [[:user/first-name] :contains "Bo"]]
                          [:hydrate [:user/first-name]]))
         (map :user/first-name)
        (every? #(= % "Bob"))
        (is))))

(deftest filter-set-contains-test
  (testing "An example of using filter on a single attribute."
    (->> (query-user (--> [:select :*]
                          [:realize]
                          [:filter [[:user/friends] :contains "Alice"]]
                          [:hydrate [:user/first-name]]))
         (map :user/first-name)
        (every? #(= % "Bob"))
        (is))))

(deftest filter-set-contains-mismatch-test
  (testing "An example of using filter on a single attribute."
    (->> (query-user (--> [:select :*]
                          [:realize]
                          [:filter [[:user/friends] :contains "Bob"]]
                          [:hydrate [:user/first-name]]))
         (empty?)
         (is))))

(deftest filter-contains-or-test
  (testing "An example of using filter on a single attribute."
    (->> (query-user (--> [:select :*]
                          [:realize]
                          [:filter [:or [[:user/first-name] :contains "Bo"]
                                    [[:user/first-name] :contains "Alic"]]]
                          [:hydrate [:user/first-name]]))
         (map :user/first-name)
         (every? #(or (= % "Bob") (= % "Alice")))
         (is)
         )))

(deftest select-non-indexed
  (testing "An example of using select on a non-indexed attribute."
    (->> (query-user (--> [:select {:user/last-name "Foo"}]
                          [:realize]
                          [:hydrate [:user/first-name]]))
      (map :user/first-name)
      (every? #(or (= % "Bob") (= % "Alice")))
      (is))
    (->> (query-user (--> [:select {:user/last-name "Foo"}]
                          [:realize]
                          [:hydrate [:user/first-name]])
                     :use-datomic true)
      (map :user/first-name)
      (every? #(or (= % "Bob") (= % "Alice")))
      (is))))

(deftest select-non-indexed-and-indexed
    (testing "An example of using select on a non-indexed _and_ an indexed attribute."
      (->> (query-user (--> [:select {:user/last-name "Foo"
                                      :user/first-name "Bob"}]
                            [:realize]
                            [:hydrate [:user/first-name]]))
        (map :user/first-name)
        (first)
        (= "Bob")
        (is))
      (->> (query-user (--> [:select {:user/last-name "Foo"
                                      :user/first-name "Bob"}]
                            [:realize]
                            [:hydrate [:user/first-name]])
                       :use-datomic true)
        (map :user/first-name)
        (first)
        (= "Bob")
        (is))))

(deftest arrow-both-ways
  (let [q [[:select :*]
           [:realize]
           [:filter [:or [[:user/first-name] :contains "Bo"]
                     [[:user/first-name] :contains "Al"]]]
           [:hydrate [:user/first-name]]]
        threaded (reduce --> q)
        dethreaded (vec (<-- threaded))]
    (is (= q dethreaded))))


(deftest query-api
  (testing "Calling arbitrary API functions."
    (let [user-page (query-user (--> [:query [{:users [:user/first-name :user/email]}]]
                               [:get :users]
                               [:paginate {:limit 10}]))
          ;filtered (query-user (--> '[:query [(:users {:user/email "bob@foo.com"
          ;                                             :user/first-name "Bob"})]]))
          ]
      (println user-page)
      (is (= 10 (count user-page)))
      ;(println "filtered: " filtered)
      ;(is (= "Bob" (:user/first-name (first (:users filtered)))))
      )))

