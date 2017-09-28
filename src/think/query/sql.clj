(ns think.query.sql
  (:require [clojure.java.jdbc :as jdbc]
            [honeysql.core :as sql]
            [honeysql.helpers :refer [select from]]))

(defn- resource-type->table-name
  "One potential strategy for doing this. You may need your own (especially if
  you're not in control of the sql table names)."
  [resource-type]
  (-> resource-type name keyword))

(defn sql-primary-index
  "Given a jdbc sql-db map, query out all the items of the resource-type and
  construct an in-memory index from them."
  [sql-db resource-type sql-map->item]
  (->> (sql/build :select :*
                  :from (resource-type->table-name resource-type))
       (sql/format)
       (jdbc/query sql-db)
       (map sql-map->item)
       (map (fn [{:keys [resource/id] :as item}]
              [id item]))
       (into {})))

(defn- selection->where
  "Given a selection (the type of map passed to `:select` in a query) return a
  honeysql where clause."
  [keyword->column-name selection]
  (if (= selection :*)
    true
    (into [:and]
          (mapv (fn [[k v]]
                  [:= (keyword->column-name k) v])
                selection))))

(defn sql-selector
  "Create a query selector for dynamically querying items of the given resource
  type (that is, to query them on the fly without pre-loading them all into
  memory)."
  [sql-db resource-type keyword->column-name selection]
  (->> (sql/build :select :id
                  :from (resource-type->table-name resource-type)
                  :where (selection->where keyword->column-name selection))
       (sql/format)
       (jdbc/query sql-db)
       (map :id)
       (set)))

(defn sql-realizer
  "Create a query realizer for dynamically realizing items of the given resource
  type."
  [sql-db resource-type sql-map->item resource-id-set]
  (->> (sql/build :select :*
                  :from (resource-type->table-name resource-type)
                  :where (into [:or]
                               (mapv (fn [rid]
                                       [:= :id rid])
                                     resource-id-set)))
       (sql/format)
       (jdbc/query sql-db)
       (map sql-map->item)))
