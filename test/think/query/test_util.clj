(ns think.query.test-util
  (:require [datomic.api :as d]
            [think.query.test-data :as test-data]))

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

(defn db-fixture
  [test-fn]
  (let [datomic-uri (str "datomic:mem://think-query.test.db." (d/squuid))]
    (d/create-database datomic-uri)
    (with-bindings {#'*datomic-uri* datomic-uri}
      (try
        (setup-datomic)
        (test-fn)))))
