(ns think.query.sql
  (:require [clojure.java.jdbc :as jdbc]
            [honeysql.core :as sql]
            [honeysql.helpers :refer [select from]]))

(defn sql-primary-index
  [sql-db resource-type sql-map->item]
  (->> (sql/build :select :*
                  :from (-> resource-type name keyword))
       (sql/format)
       (jdbc/query sql-db)
       (map sql-map->item)
       (map (fn [{:keys [resource/id] :as item}]
              [id item]))
       (into {})))
