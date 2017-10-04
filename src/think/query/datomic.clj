(ns think.query.datomic
  (:require [clojure.set :as set]))

(defn datomic-primary-index
  "Provides the default datomic index based on resource type"
  [db resource-type]
  (require 'datomic.api)
  (->> resource-type
       ((resolve 'datomic.api/q) '[:find ?resource-id (pull ?e [*])
              :in $ ?resource-type
              :where
              [?e :resource/type ?resource-type]
              [?e :resource/id ?resource-id]]
            db)
       (into {})))

;; maybe not use pull api?
(defn datomic-query
  "Use the selection criteria to query datomic directly.  Used when none of the selection
keys map to in-memory indexes."
  [db selection data]
  (require 'datomic.api)
  (->> (map (fn [[k v]] ['?e k v]) selection)
       (concat [:find '[(pull ?e [:resource/id]) ...] :where])
       (vec)
       ((fn [x] ((resolve 'datomic.api/q) x db)))
       (map :resource/id)
       (into #{})
       (set/intersection data)))
