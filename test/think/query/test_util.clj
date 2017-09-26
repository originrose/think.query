(ns think.query.test-util
  (:require [datomic.api :as d]
            [think.query.test-schema :as test-schema]
            [think.query.test-data :as test-data]))

(def ^:dynamic *datomic-uri* nil)

(defn- import-test-data
  [datomic-uri]
  @(d/transact (d/connect datomic-uri) test-data/test-users))

(defn- test-system
  [test-fn setup-fn]
  (let [datomic-uri (str "datomic:mem://think-query.test.db." (d/squuid))]
    (d/create-database datomic-uri)
    (with-bindings {#'*datomic-uri* datomic-uri}
      (try
        @(d/transact (d/connect datomic-uri) (concat test-schema/resource-schema test-schema/user-schema))
        (setup-fn datomic-uri)
        (test-fn)))))

(defn with-db
  [test-fn]
  (test-system test-fn import-test-data))

(defn with-clean-db
  [test-fn]
  (test-system test-fn identity))

(defn db-conn
  "Returns a datomic connection to the (current) test database fixture."
  []
  (d/connect *datomic-uri*))

(defn db
  "Returns a datomic database."
  []
  (d/db (d/connect *datomic-uri*)))

(defn user->resource-id
  [user]
  (:resource/id (d/pull (db) [:resource/id] [:user/email (:user/email user)])))
