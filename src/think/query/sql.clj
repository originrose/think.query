(ns think.query.sql
  (:require [clojure.java.jdbc :as jdbc]
            [honeysql.core :as sql]
            [honeysql.helpers :refer [select from]]))

(defn- resource-type->table-name
  [resource-type]
  (-> resource-type name keyword))

(defn sql-primary-index
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
  [keyword->column-name selection]
  (if (= selection :*)
    true
    (into [:and]
          (mapv (fn [[k v]]
                  [:= (keyword->column-name k) v])
                selection))))

(defn sql-selector
  [sql-db resource-type keyword->column-name selection]
  (->> (sql/build :select :id
                  :from (resource-type->table-name resource-type)
                  :where (selection->where keyword->column-name selection))
       (sql/format)
       (jdbc/query sql-db)
       (map :id)
       (set)))

(defn sql-realizer
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
