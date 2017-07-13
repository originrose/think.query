(ns think.query.test-data)

(def alice
  {:user/email "alice@foo.com"
   :user/age 23
   :user/first-name "Alice"
   :user/last-name "Foo"})

(def bob
  {:user/email "bob@foo.com"
   :user/age 27
   :user/first-name "Bob"
   :user/friends ["Alice"]
   :user/last-name "Foo"})

(def test-users [alice bob])
