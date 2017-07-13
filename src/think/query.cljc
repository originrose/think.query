(ns ^{:author "ThinkTopic, LLC."
      :doc "The query system is a general purpose system for querying in-memory indexes
in addition to the datomic backing style.  The intent is to reduce the number of endpoints by providing a
general purpose language that can specify the shape of filtered, ordered sets of results.  It is composed
of two possible operations, either a query operator or a compute operator which are currently
implemented as multimethods.  The source data for the query system is a set of indexes defined near
the top of the file as well some ability to translate queries to the datomic backing store.
There are a couple base assumptions the system makes about the underlying data, the first being
that a query maps to a single resource type (currently either :resource.type/visual-variant
or :resource.type/brand) and that these types have valid resource id's"}
    think.query
  (:require
   [clojure.string :as s]
   [clojure.set :as set]
   [clojure.walk :as walk]
   [clojure.test.check.random :as rand]
   #?(:clj  [think.query.datomic :as datomic])))


(defn- insert-at
  [coll item n]
  (let [[a b] (split-at n coll)]
    (into [] (concat a [item] b))))

(defn -->
  "A query thread operator that works just like ->, except for data in vectors.

  (--> :a [:b] [:c] [:d {:foo :bar}]) ; =>  [:d [:c [:b :a]] {:foo :bar}]
  "
  [init & ops]
  (reduce (fn [q op]
            (insert-at op q 1))
          init
          ops))


(defn <--
  "Inverse of -->"
  [q]
  (loop [q q
         p []]
    (let [op (first q)
          data (second q)
          last? (not (and (vector? data) (pos? (count data)) (keyword? (first data))))
          pipeline (conj p
                         (if last?
                           q
                           (into [] (concat [op] (drop 2 q)))))]
      (if-not last?
        (recur data pipeline)
        (reverse pipeline)))))


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

(defn- filter-by-selection
  "Linear scan over data for a given selection"
  [{:keys [:default-index :primary-index] :as indexes} selection data]
  (if (= default-index :datomic)
    (do
      #?(:clj (datomic/datomic-query (:db indexes) selection data)))
    (let [[index-key selection-match] (first selection)
          path (if (sequential? index-key) index-key [index-key])]
      (->> data
           (map primary-index)                              ;; realize
           (filter #(= selection-match (get-in % path)))    ;; linear scan / filter
           (map :resource/id)
           (into #{})))))

(defn apply-filter-logic
  "Recursively walk through a set of operands building up sets based on a few operators.  The primary index
  here is used solely for generating the entire set of uuid's when we want the set of all items *not* in
  a given query."
  ([indexes selection]
   (apply-filter-logic indexes selection #{}))
  ([indexes selection data]
   (let [[index-key selection-match] (first selection)
         sub-index (get indexes index-key)]
     (if (and (sequential? selection-match)
              (pos? (count selection-match))
              (some #{(first selection-match)} [:and :or :not]))     ;; selection match has boolean logic
       (let [;;Sub-query result is a sequence of sets
             sub-query-result (->> (rest selection-match)
                                   (map #(apply-filter-logic indexes {index-key %} data)))]
         (case (first selection-match)
           :and (apply set/intersection sub-query-result)
           :or  (apply set/union sub-query-result)
           :not (apply set/difference data sub-query-result)))
       (if sub-index
         (sub-index selection-match)
         (filter-by-selection indexes selection data))))))

(defn- index-query
  "Run a query against the in-memory indexes.
selection: the query
pred-keys: keys that do not map to in memory indexes
indexes: in memory indexes for this resource type
indexed-keys: keys that map to some subset of those indexes.

Does an intersection across all indexed values followed by a linear filter using predicate
values.  Returns a lazy sequence of resource ids."
  [selection pred-keys indexes indexed-keys]
  (->> (concat (rest indexed-keys) pred-keys)
       (reduce (fn [eax item]
                 (let [new-items (apply-filter-logic indexes (select-keys selection [item]) eax)]
                   (if (some #{item} indexed-keys)
                     (set/intersection eax new-items)
                     new-items)))
               (if (empty? indexed-keys)
                 (set (keys (:primary-index indexes)))
                 (apply-filter-logic indexes (select-keys selection [(first indexed-keys)]))))))


(defn do-selection
  "Given a the set of indexes for a given type and a selection return a set of uuids.
indexes: a map of keyword to index for a given resource type.
selection: map of key-value pairs where value can either be a specific value or
it could possibly be a nested set of things like 'and' and 'or' operators with
potentially more criteria."
  [indexes selection]
  (let [fast-keys    (set (keys indexes))
        indexed-keys (filter fast-keys (keys selection))
        pred-keys    (remove fast-keys (keys selection))]
    (index-query selection pred-keys indexes indexed-keys)))

(defmulti query-operator (fn [indexes q] (first q)))

(defn- maybe-query
  "Test if the multi-method implementation is there, otherwise just pass the data through."
  [indexes q]
  (if (and (sequential? q)
           (keyword? (first q))
           (get-method query-operator ((.dispatchFn ^clojure.lang.MultiFn query-operator) indexes q)))
    (query-operator indexes q)
    q))

(defmethod query-operator :select
  [indexes [_ selection]]
  (if (= selection :*)
    (set (keys (:primary-index indexes)))
    (set (do-selection indexes selection))))

(defmethod query-operator :realize
  [indexes q]
  (let [id-set (query-operator indexes (second q))]
    (map (:primary-index indexes) id-set)))


; Sort the input sequence
;  [:sort q {:path [:product/price] :direction :ascending}]
(defmethod query-operator :sort
  [indexes q]
  (let [{:keys [path direction] :or {direction :ascending} :as options} (last q)
        data (query-operator indexes (second q))
        sorted (sort-by #(get-in % path) data)]
    (if (= direction :ascending)
      sorted
      (reverse sorted))))

(defn- item->score
  [params item]
  (apply + (map (fn [{:keys [path direction weight normalizer]}]
                  (* weight normalizer (get-in item path) (if (= :descending direction) -1 1)))
                params)))

(defmethod query-operator :weighted-sort
  [indexes [_ subquery params]]
  (let [data (query-operator indexes subquery)
        params (for [{:keys [path] :as p} params]
                 (assoc p :normalizer (/ 1.0 (+ 1e-10 (apply max (map #(get-in % path) data))))))]
    (sort-by (partial item->score params) data)))

;; Hydration
; [:hydrate q [:product/name :product/id {:product/variants [:product/sku]}]]
(defn do-hydrate
  [data k]
  (if (sequential? data)
    (map #(do-hydrate % k) data)
    (let [v (cond
              (= '* k) (vec data)
              (keyword? k) [[k (get data k)]]
              (string? k) [[k (get data k)]]
              (vector? k) (into {} (mapcat #(do-hydrate data %1) k))
              (map? k) [k (into {} (for [[child-key child-hydration] k]
                                     [child-key (do-hydrate (get data child-key) child-hydration)]))])]
      v)))

(defn hydrate
  [data hydration]
  (into {} (mapcat #(do-hydrate data %1) hydration)))

(defmethod query-operator :hydrate
  [indexes q]
  (let [data (query-operator indexes (second q))
        hydration (last q)]
    (map #(hydrate % hydration) data)))

(defn- reweight-mixes
  "Takes a list of [weight elems] and normalizes the weights for a roulette. For example, given:
  [[2 X] [5 Y] [3 Z]]
  this returns:
  [[0.2 X] [0.7 Y] [1.0 Z]]
  The reason for this is that we want to be able to generate a random number
  between 0 and 1 and quickly walk through the seq to find the relevant seq of
  elements.

  Removes pairs with 0 weight or empty elems."
  [mixes]
  (let [mixes (remove (fn [[weight elems]] (or (zero? weight)
                                               (empty? elems))) mixes)]
    (if (not-empty mixes)
      (let [total-weight (reduce + (map first mixes))]
        (first (reduce (fn [[mem total] [weight q]]
                         (let [v (+ (/ weight total-weight) total)]
                           [(conj mem [v q]) v]))
                       [[] 0.0]
                       mixes))))))

(defn mix-sampler*
  "Given a random number generator and a seq of tuples [weight elems], where
  the weights have been normalized to cumulatively reach 1.0 (as per
  reweight-mixes), returns a seq composed of the items of all elems seqs drawn
  randomly from the tuples.

  Each element is only drawn once; order is preserved between elements of the
  same tuple. this is a bit like a randomized clojure.core/interleave, except
  that it will exhaust all seqs rather than stop at the first empty one."
  [^clojure.test.check.random.JavaUtilSplittableRandom rgen mixes]
  (if (not-empty mixes)
    (let [v (rand/rand-double rgen)
          mix (first (filter #(> (first %) v) mixes))
          [weight data] mix
          [next-item & data] (seq data)
          new-mix [weight data]
          new-mixes (if (seq data)
                      (replace {mix new-mix} mixes)
                      (reweight-mixes (remove #(= mix %) mixes)))]
      (lazy-seq (cons next-item
                      (mix-sampler* rgen new-mixes))))))

(defn mix-sampler
  "Calls mix-sampler* with fixed random seed so results are deterministic."
  [mixes]
  (mix-sampler* (rand/make-random 0) (reweight-mixes mixes)))

;; Weighted mixture operator
;; [:mix [[0.2 <query>]
;;        [0.5 <query>]]]
(defmethod query-operator :mix
  [indexes [_ mixes]]
  (->> mixes
       (reweight-mixes)
       (reduce (fn [mem [weight q]]
                 (conj mem [weight (query-operator indexes q)]))
               [])
       (mix-sampler)))

;; The query operator generally acts as the root operator, and provides offset and limit
;; functionality.
; [:query <sub-query>
;  {:offset <int>
;   :limit <int>}]
(defmethod query-operator :query
  [indexes q]
  (let [{:keys [offset limit] :or {offset 0} :as options} (last q)
        data (query-operator indexes (second q))
        data (if offset (drop offset data) data)
        data (if limit (take limit data) data)]
    data))

; ; Compute operator (to compute values used for sorting later)
; [:compute <sub-query> [:brand-dna-ranking {:query ...}]]
(defmulti compute-operator
  "Compute a value given a record.  The result will be assoced on to the record
  by the compute query operator."
  (fn [& args] (first args)))

;;[:compute sub-query c-type args?]
(defmethod query-operator :compute
  [indexes q]
  (let [c-type (nth q 2)
        args (->> q
                  (split-at 3)
                  last)]
    (for [result (query-operator indexes (second q))]
      (assoc result c-type (apply compute-operator c-type result args)))))

(defmulti transform-operator
  (fn [op data & args]
    op))

;; Given an offset and a limit, drop the offset and take the limit from the sequence of results.
(defmethod transform-operator :paginate
  [_ data & {:keys [offset limit]}]
  (as->
    (if offset (drop offset data) data) data
    (if limit (take limit data) data)))

(defmethod query-operator :transform
  [indexes [_ q op & args]]
  (let [q-result (query-operator indexes q)]
    (apply transform-operator op q-result (map (partial maybe-query indexes) args))))

(defn predicate->fn
  "e.g. [:and [[:retail-value] :> 10]
              [[:retail-value] :< 20]]
   or [[:color-distance] :<= 0.1]"
  [predicate]
  (case (first predicate)
    :and (apply every-pred (map predicate->fn (rest predicate)))
    :or (apply some-fn (map predicate->fn (rest predicate)))
    (let [[path operator v] predicate]
      (case operator
        :<  #(< (get-in % path) v)
        :>  #(> (get-in % path) v)
        :=  #(= (get-in % path) v)
        :not=  #(not= (get-in % path) v)
        :>= #(>= (get-in % path) v)
        :<= #(<= (get-in % path) v)
        :contains #(let [haystack (get-in % path)]
                     (if (string? haystack)
                       (not= (.indexOf haystack v) -1)
                       ((set haystack) v)))))))

(defmethod query-operator :filter
  [indexes [_ sub-query predicate]]
  (filter (predicate->fn predicate)
          (query-operator indexes sub-query)))


(defmethod query-operator :let-binding
  [indexes [_ bound-val*]]
  @bound-val*)

(defn build-let-result
  [bind-vals result]
  (cond
    (map? result)
    (reduce-kv (fn [m k bind-name]
                 (assoc m k (build-let-result bind-vals bind-name)))
               {} result)

    (vector? result)
    (mapv (fn [x] (build-let-result bind-vals x)) result)

    :default
    (if (contains? bind-vals result)
      @(bind-vals result)
      result)))

(defmethod query-operator :let
  [indexes [_ bindings result]]
  (let [bindings (partition 2 bindings)
        bind-vals (reduce (fn [bind-vals [b-name b-query]]
                            (let [new-query (walk/postwalk (fn [x] (if (contains? bind-vals x)
                                                                     [:let-binding (bind-vals x)]
                                                                     x))
                                                           b-query)]
                              (assoc bind-vals b-name (delay (maybe-query indexes new-query)))))
                          {}
                          bindings)]
    (build-let-result bind-vals result)))

(defn query
    "Run a generalized query against either the in-memory indexes or against datomic.

resource-type: The type of resource to query against; this defines the set of indexes that
  would potentially be used.  Specifically either :resource.type/visual-variant or
  :resource.type/brand

q: The query to run.



The query is a set of nested vectors of the form:
  [:hydrate
     [:weighted-sort [:compute
                        [:realize
                          [:select :*]]
                        :retail-value]
                     [{:direction :descending, :path [:retail-value], :weight 1.0}]]
     [:product/name :retail-value]]

There is a utility function defined that allows the same query above to be defined in the form of:
(q/--> [:select :*]
       [:realize]
       [:compute :retail-value]
       [:weighted-sort [{:path [:retail-value]
                         :weight 1.0
                         :direction :descending}]]
       [:hydrate [:product/name
                  :retail-value]])

In other words, you can define the query in an imperative style and the operator will do the required nesting.


Each query operator is in the form of:
  [operator-keyword & args]

where args may in fact be sub-queries or arguments specific to the query.  The examples below assum the above
nesting function (-->) has been used to form the query.

Current operators are:
:select - select a set of uuid's from a set of indexes.  The args are a map of index keywords to query values
  where query values can be either a scalar value or a filter value such as [:and a b] where a b can be sub-filters
  or scalar values.  Note that the keys themselves may be complex in which case they will find nested items in
  the selected set of objects.

  For resource.type/visual-variant the current set of indexes are:
    Sku->visual-variant resource id
    :product/sku
    product->visual variant
    :product/id
    tag uuid -> visual variant uuid
    :product-intelligence/weighted-tags
    brand canonical name->visual variant
    [:product/brand :brand/canonical-name]
    resource id
    resource/id

  For resource.type/brand there is only the primary index and a resource id index.


  [:select {:product-intelligence/weighted-tags
                             [:and
                              [:not \"ultra tight\"]
                              [:or \"leia buns\" \"chewbacca costume\"]]}]

  [:select {:product/quarantined? true
            [:product/brand :brand/canonical-name] \"correlian outfitters\"
            :product-intelligence/weighted-tags \"chewbacca costume\"}]

:realize - realize the set of uuid; this expands a uuid into a full item where item is a map of key value pairs.  This
  operator takes no arguments.

:sort - sort the results.  sort takes a map of two parameters for the path to find the sort key which must be ordered
  comparable and the direction.
  [:sort {:path [:retail-value]
          :direction :descending}]

:weighted-sort - take a set of sort criteria (path,direction,weight) and sort the dataset based on a sum of
  of the numeric values taken from the paths.  The values are normalized such that the min of each path is 0 and
  the max of each path is 1 so the values can be compared on an equal basis.
  [:weighted-sort [{:path [:retail-value]
                  :weight 0.95
                  :direction :ascending}
                 {:path [:sort-tags]
                  :weight 0.05
                  :direction :descending}]]

:hydrate - trim the realized data into the desired shape.  This has the opposite meaning as the datomic
  pull in that in this case it is doing a trimming operation and is intended to be applied after a realize
  call.  It will ensure the data has precisely the shape defined in the hydration and it's syntax a simplified
  datomic pull syntax.
  [:hydrate [:product/name
             :retail-value
             :product-intelligence/weighted-tags]]

:mix - Given two selected streams of data mix them such that your overall result
  is a percentage of each result.  Thus if you have two streams and your mix weights
  are 0.2 and 0.8 you will get 20% of random data from the first stream and then
  80% of the random data from the second stream.
  [:mix [[0.5 (--> [:select
                    {:product-intelligence/weighted-tags
                     [:and
                      #uuid \"7dd9aa68-4654-4d21-a5e9-5ab5d9141d15\"
                      #uuid \"6cc9aa68-4654-4d21-a5e9-5ab5d9141d14\"]}]
                   [:realize]
                   [:compute :color-distance [155 43 43]]
                   [:sort {:path [:color-distance] :direction :ascending}]
                   [:hydrate [:resource/id]])]
         [0.3 (--> [:select
                    {:product-intelligence/weighted-tags
                     [:and
                      #uuid \"9ee9aa68-4654-4d21-a5e9-5ab5d9141e16\"
                      #uuid \"2bb9aa68-4654-4d21-a5e9-5ab5d9141e45\"]}]
                   [:realize]
                   [:compute :sort-tags {\"shoes\" 0.91}]
                   [:sort {:path [:sort-tags]:direction :descending}]
                   [:hydrate [:resource/id]])]]]

:query - The query operator generally acts as the root operator, and provides offset and limit functionality.
  [:query <sub-query>
    {:offset <int>
     :limit <int>}]

:compute - compute an additional value and store it each object in the stream under a key of
  the name of the compute operator.  Operators are:
  :retail-value - The sum of the price multiplied by the number of items in stock for each variant.
  :sort-tags - The sum of the chosen tag weights multiplied by the associated values.
    [:compute :sort-tags [[\"leia buns\" 1.0]]]
  :color-distance - The distance from the selected color of the product in cie-lab space:
    [:compute :color-distance [155 43 43]]


Examples:

;; Sort the entire set of all products by retail value ascending and hydrate name and retail value.
(query :resource.type/visual-variant
 (-->   [:select :*]
        [:realize]
        [:compute :retail-value]
        [:weighted-sort [{:path [:retail-value]
                          :weight 1.0
                          :direction :ascending}]]
        [:hydrate [:product/name
                   :retail-value]]))

;; Find all products from the \"correlian outfitters\" and sort them by how much
;; chewbacca costume they have.
(query :resource.type/visual-variant
  [:hydrate
    [:sort
     [:compute
      [:realize [:select {[:product/brand :brand/canonical-name] \"correlian outfitters\"}]]
      :sort-tags {\"chewbacca costume\" 1.0}]
     {:path [:sort-tags]
      :direction :descending}]
  [:product/name
   :product/visual-id
   {:product/variants [:product/sku]}]]


;; The all product names and resource ids that have either the
;; \"leia buns\" tag or the \"chewbacca constume\" tag but not
;; the \"ultra tight\" way.

(query :resource.type/visual-variant
  (--> [:select {:product-intelligence/weighted-tags
                 [:and
                   [:not \"ultra tight\"]
                   [:or \"leia buns\" \"chewbacca costume\"]]}]
       [:realize]
       [:hydrate [:product/name :resource/id]])
"
  [resource-type indexes q]
  (query-operator indexes q))
