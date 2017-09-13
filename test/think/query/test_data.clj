(ns think.query.test-data)

(def alice
  {:user/email "alice@foo.com"
   :user/created #inst "2017-09-12T18:24:09.556-00:00"
   :user/age 23
   :user/first-name "Alice"
   :user/last-name "Foo"})

(def bob
  {:user/email "bob@foo.com"
   :user/age 27
   :user/created #inst "2017-09-12T18:24:10.556-00:00"
   :user/first-name "Bob"
   :user/friends ["Alice"]
   :user/last-name "Foo"})

(def test-users [alice bob])
