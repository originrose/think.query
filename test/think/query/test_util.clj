(ns think.query.test-util
  (:require [clojure.string :as str]
            [datomic.api :as d]
            [clojure.java.jdbc :as jdbc]
            [honeysql.core :as sql]
            [honeysql.helpers :refer [select from]]
            [think.query.test-data :as test-data])
  (:import [java.util UUID]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Datomic
(def ^:dynamic *datomic-uri* nil)

(defn conn
  []
  (d/connect *datomic-uri*))

(defn db
  []
  (d/db (conn)))

(def resource-schema
  [{:db/id #db/id[:db.part/db]
    :db/ident :resource/id
    :db/valueType :db.type/uuid
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :resource/type
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}])

(def user-schema
  [{:db/id #db/id[:db.part/db]
    :db/ident :resource.type/user}

   {:db/id #db/id[:db.part/db]
    :db/ident :user/first-name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :user/last-name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :user/sex
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :user/friends
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/many
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :user/email
    :db/unique :db.unique/identity
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :user/created
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}

   {:db/id #db/id[:db.part/db]
    :db/ident :user/age
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}])

(defn- setup-datomic
  []
  @(d/transact (conn) (concat resource-schema user-schema))
  @(d/transact (conn) test-data/test-users))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; SQL
(def ^:dynamic *sql-db* nil)

(defn sql-db
  []
  *sql-db*)

(defn- user->sql-map
  "Convert clojure types to H2 SQL friendly ones."
  [{:keys [resource/id resource/type
           user/first-name user/last-name
           user/sex user/friends
           user/email user/created user/age]}]
  {"\"resource/id\"" id
   "\"resource/type\"" (str type)
   "\"first-name\"" first-name
   "\"last-name\"" last-name
   :sex (str sex)
   :friends (pr-str friends)
   :email email
   :created created
   :age age})

(defn sql-map->user
  "Convert sql query results back to rich clojure maps."
  [{:keys [age created email first-name last-name friends sex resource/id resource/type]}]
  {:resource/id id
   :resource/type (clojure.edn/read-string type)
   :user/first-name first-name
   :user/last-name last-name
   :user/sex (clojure.edn/read-string sex)
   :user/email email
   :user/age age
   :user/friends (clojure.edn/read-string friends)
   :user/created created})

(def sql-schema (format "CREATE TABLE user (%s);"
                        (->> ["\"resource/id\" uuid"
                              "\"resource/type\" varchar"
                              "\"first-name\" varchar"
                              "\"last-name\" varchar"
                              "sex varchar"
                              "friends varchar"
                              "email varchar"
                              "created timestamp"
                              "age int"]
                             (str/join ","))))

(defn- setup-sql
  []
  (jdbc/db-do-commands *sql-db* sql-schema)
  (jdbc/insert-multi! *sql-db* :user (map user->sql-map test-data/test-users)))

(defn db-fixture
  [test-fn]
  (let [datomic-uri (str "datomic:mem://think-query.test.db." (d/squuid))
        sql-db {:classname   "org.h2.Driver"
                :subprotocol "h2:mem"
                :subname     (str (UUID/randomUUID) ";DB_CLOSE_DELAY=-1")}]
    (d/create-database datomic-uri)
    (with-bindings {#'*datomic-uri* datomic-uri
                    #'*sql-db* sql-db}
      (try
        (setup-datomic)
        (setup-sql)
        (test-fn)))))
