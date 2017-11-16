(ns ^{:author "ThinkTopic"
      :doc "A general purpose system for querying stores of data. Users of the
query system can select data from the store as desired and specify the level of
hydration they need. The system is extensible through multimethods that can
augment or even arbitrarily transform data items at query time. Each query is
performed in some context `ctx` that specifies how data is retrieved from the
underlying store. There are a couple base assumptions the system makes about the
underlying data, namely that each data item has `:resource/id` and
`:resource/type`."} think.query
  (:require
   [clojure.string :as s]
   [clojure.set :as set]
   [clojure.walk :as walk]
   [clojure.test.check.random :as rand]))


(defn- insert-at
  [coll item n]
  (let [[a b] (split-at n coll)]
    (into [] (concat a [item] b))))


(defn -->
  "A query thread operator that works just like ->, except for data in vectors.
    ex: (--> :a [:b] [:c] [:d {:foo :bar}]) ; =>  [:d [:c [:b :a]] {:foo :bar}]"
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


(defn- filter-by-selection
  "Linear scan over data for a given selection"
  [{:keys [:primary-index] :as indexes} selection data]
  (let [[index-key selection-match] (first selection)
        path (if (sequential? index-key) index-key [index-key])]
    (->> data
         (map primary-index)                             ;; realize
         (filter #(= selection-match (get-in % path)))   ;; linear scan / filter
         (map :resource/id)
         (into #{}))))


(defn apply-filter-logic
  "Recursively walk through a set of operands building up sets based on a few operators.  The primary index
  here is used solely for generating the entire set of UUIDs when we want the set of all items *not* in
  a given query."
  [indexes selection data]
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
          :not (apply set/difference (set data) sub-query-result)))
      (if sub-index
        (sub-index selection-match)
        (filter-by-selection indexes selection data)))))


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
               (let [resource-id-set (keys (:primary-index indexes))]
                 (if (empty? indexed-keys)
                   resource-id-set
                   (apply-filter-logic indexes
                                       (select-keys selection [(first indexed-keys)])
                                       resource-id-set))))))


(defn do-selection
  "Given a the set of indexes for a given type and a selection return a set of uuids.
    indexes: a map of keyword to index for a given resource type.
    selection: map of key-value pairs where value can either be a specific value
      or it could possibly be a nested set of things like 'and' and 'or' operators
      with potentially more criteria."
  [indexes selection]
  (let [fast-keys    (set (keys indexes))
        indexed-keys (filter fast-keys (keys selection))
        pred-keys    (remove fast-keys (keys selection))]
    (index-query selection pred-keys indexes indexed-keys)))


(defmulti query-operator (fn [ctx q] (first q)))


(defn- maybe-query
  "Test if the multi-method implementation is there, otherwise just pass the data through."
  [{:keys [indexes] :as ctx} q]
  (if (and (sequential? q)
           (keyword? (first q))
           (get-method query-operator
                       ((.dispatchFn ^clojure.lang.MultiFn query-operator) indexes q)))
    (query-operator ctx q)
    q))


;; Select
;; Get a set of resource ids from a data store
;; A selector is a function that takes a map like `:select` and returns a set of `:resource/id`s
(defmethod query-operator :select
  [{:keys [indexes selector] :as ctx} [_ selection]]
  (if selector
    (selector selection)
    (if (= selection :*)
      (set (keys (:primary-index indexes)))
      (set (do-selection indexes selection)))))


;; Realize
;; Turns a set of resource ids into actual resources
;; A realizer is a function that takes a set of `:resource/id`s and an optional datomic-like hydration and returns a sequence of maps
;; TODO: Optional hydration
(defmethod query-operator :realize
  [{:keys [indexes realizer] :as ctx} q]
  (let [id-set (query-operator ctx (second q))]
    (if realizer
      (realizer id-set)
      (map (:primary-index indexes) id-set))))


;; Sort the input sequence
;;  [:sort q {:path [:product/price] :direction :ascending}]
(defmethod query-operator :sort
  [ctx q]
  (let [{:keys [path direction]
         :or {direction :ascending}} (last q)
        data (query-operator ctx (second q))
        sorted (sort-by #(get-in % path) data)]
    (if (= direction :ascending)
      sorted
      (reverse sorted))))


(defn- item->score
  [params item]
  (apply + (map (fn [{:keys [path direction weight normalizer]}]
                  (* weight normalizer (get-in item path) (if (= :descending direction) -1 1)))
                params)))

;; Weighted Sort
;; Allows sorting by multiple attributes according to specified weights
(defmethod query-operator :weighted-sort
  [ctx [_ subquery params]]
  (let [data (query-operator ctx subquery)
        params (for [{:keys [path] :as p} params]
                 (assoc p :normalizer (/ 1.0 (+ 1e-10 (apply max (map #(get-in % path) data))))))]
    (sort-by (partial item->score params) data)))


(defn- do-hydrate
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


;; Hydration
;; Specify hydration using the datomic pull syntax.
;; [:hydrate q [:product/name :product/id {:product/variants [:product/sku]}]]
(defmethod query-operator :hydrate
  [ctx q]
  (let [data (query-operator ctx (second q))
        hydration (last q)]
      (map #(hydrate % hydration) data)))


;; Get
;; calls `clojure.core/get` on the result of the data with the specified key
(defmethod query-operator :get
  [ctx [_ sub-query k]]
  (let [data (query-operator ctx sub-query)
        res (get data k)]
    (get data k)))


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


(defn- mix-sampler*
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
  [ctx [_ mixes]]
  (->> mixes
       (reweight-mixes)
       (reduce (fn [mem [weight q]]
                 (conj mem [weight (query-operator ctx q)]))
               [])
       (mix-sampler)))


;; Pagination with an offset and limit.
;;   [:paginate <sub-query>
;;    {:offset <int>
;;     :limit <int>}]
(defmethod query-operator :paginate
  [ctx q]
  (let [{:keys [offset limit] :or {offset 0} :as options} (last q)
        data (query-operator ctx (second q))
        data (if offset (drop offset data) data)
        data (if limit (take limit data) data)]
    data))


(defmulti compute-operator
  "Compute a value given a record. The result will be assoced on to the record
  by the compute query operator."
  (fn [& args] (first args)))


;; Compute
;; Given a data item, compute a derived value and assoc it on. Extensible as the
;; multi-method `compute-operator` and associates the same-named key into the
;; item
(defmethod query-operator :compute
  [ctx q]
  (let [c-type (nth q 2)
        args (->> q
                  (split-at 3)
                  last)]
    (for [result (query-operator ctx (second q))]
      (assoc result c-type (apply compute-operator c-type result args)))))


(defmulti transform-operator
  (fn [op data & args]
    op))


;; Transform
;; Arbitrary transformations, extensible as the multi-method `transform-operator`
(defmethod query-operator :transform
  [ctx [_ q op & args]]
  (let [q-result (query-operator ctx q)]
    (apply transform-operator op q-result (map (partial maybe-query ctx) args))))


(defn predicate->fn
  "Examples:
     [:and [[:retail-value] :> 10]
            [[:retail-value] :< 20]]

     [[:color-distance] :<= 0.1]"
  [predicate]
  (case (first predicate)
    :and (apply every-pred (map predicate->fn (rest predicate)))
    :or (apply some-fn (map predicate->fn (rest predicate)))
    :not #(not ((predicate->fn (second predicate)) %))
    (let [[path operator v] predicate]
      (case operator
        :<  #?(:clj (if (inst? v)
                      #(< (.getTime (get-in % path)) (.getTime v))
                      #(< (get-in % path) v))
               :cljs #(< (get-in % path) v))
        :>  #?(:clj (if (inst? v)
                        #(> (.getTime (get-in % path)) (.getTime v))
                        #(> (get-in % path) v))
               :cljs #(> (get-in % path) v))
        :>= #?(:clj  (if (inst? v)
                       #(>= (.getTime (get-in % path)) (.getTime v))
                       #(>= (get-in % path) v))
               :cljs #(>= (get-in % path) v))
        :<= #?(:clj  (if (inst? v)
                       #(<= (.getTime (get-in % path)) (.getTime v))
                       #(<= (get-in % path) v))
               :cljs #(<= (get-in % path) v))
        :=  #(= (get-in % path) v)
        :not=  #(not= (get-in % path) v)
        :contains #(let [haystack (get-in % path)]
                     (if (and (string? haystack) (string? v))
                       (not= (.indexOf haystack v) -1)
                       ((set haystack) v)))))))


(defmethod query-operator :filter
  [ctx [_ sub-query predicate]]
  (filter (predicate->fn predicate)
          (query-operator ctx sub-query)))


(defmethod query-operator :let-binding
  [ctx [_ bound-val*]]
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
  [ctx [_ bindings result]]
  (let [bindings (partition 2 bindings)
        bind-vals (reduce (fn [bind-vals [b-name b-query]]
                            (let [new-query (walk/postwalk
                                              (fn [x] (if (contains? bind-vals x)
                                                        [:let-binding (bind-vals x)]
                                                        x))
                                              b-query)]
                              (assoc bind-vals b-name
                                     (delay (maybe-query ctx new-query)))))
                          {}
                          bindings)]
    (build-let-result bind-vals result)))


(defn query
  "Run a generalized query against a data store. `ctx` is a context in which to perform the query; a map with one of three shapes:
    1) Keys are `:primary-index` and then attributes with reverse indexes.
      - This is the original v1 in-memory shape, it is still supported.
    2) Key is `:indexes` and value is original v1 in-memory shape as (1).
    3) Keys are `:selector` and `:realizer`.
      - values are functions as described near :select and :realize above."
  [ctx q]
  (assert (and (map? ctx)
               (or (:primary-index ctx)
                   (:indexes ctx)
                   (and (:selector ctx)
                        (:realizer ctx))))
          "Bad query ctx shape. See docstring.")
  (if (:primary-index ctx)
    (query-operator {:indexes ctx} q)
    (query-operator ctx q)))
