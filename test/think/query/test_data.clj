(ns think.query.test-data
  (:require [clojure.string :as str]))

(def NAMES
  [{:name "Aleisha Alverson", :sex :female}
   {:name "Jaymie Jobin", :sex :female}
   {:name "Randal Richert", :sex :female}
   {:name "Shizuko Sharper", :sex :female}
   {:name "Lin Linck", :sex :female}
   {:name "Hana Hayashida", :sex :female}
   {:name "Deloris Duenes", :sex :female}
   {:name "Genny Groman", :sex :female}
   {:name "Erin Enger", :sex :female}
   {:name "Yolande Yen", :sex :female}
   {:name "Lloyd Landwehr", :sex :female}
   {:name "Juanita Joyal", :sex :female}
   {:name "Aura Abel", :sex :female}
   {:name "Thomas Tempel", :sex :female}
   {:name "Dirk Dunavant", :sex :female}
   {:name "Cristie Cork", :sex :female}
   {:name "Hildred Harstad", :sex :female}
   {:name "Dave Deming", :sex :female}
   {:name "Lael Low", :sex :female}
   {:name "Julissa Johansson", :sex :female}
   {:name "Oleta Orlandi", :sex :female}
   {:name "Soon Schuster", :sex :female}
   {:name "Monique Montesinos", :sex :female}
   {:name "Lawrence Lugo", :sex :female}
   {:name "Diedre Doughtie", :sex :female}
   {:name "Silvia Schieber", :sex :female}
   {:name "Laurena Lipari", :sex :female}
   {:name "Latosha Laurin", :sex :female}
   {:name "Erica Eley", :sex :female}
   {:name "Garnett Gabor", :sex :female}
   {:name "Thuy Tyer", :sex :female}
   {:name "Isaura Irion", :sex :female}
   {:name "Migdalia Milani", :sex :female}
   {:name "Katherine Kerbs", :sex :female}
   {:name "Lauren Lanza", :sex :female}
   {:name "Tomas Tieman", :sex :female}
   {:name "Ramona Rockhill", :sex :female}
   {:name "Blossom Bulkley", :sex :female}
   {:name "Delila Daniell", :sex :female}
   {:name "Troy Tuch", :sex :female}
   {:name "Bee Bragg", :sex :female}
   {:name "Sherman Sarinana", :sex :female}
   {:name "Maryam Mcmonagle", :sex :female}
   {:name "Reed Rolon", :sex :female}
   {:name "Myron Malley", :sex :female}
   {:name "Crystle Cagney", :sex :female}
   {:name "Kathern Klingensmith", :sex :female}
   {:name "Chong Chouinard", :sex :female}
   {:name "Marlon Matis", :sex :female}
   {:name "Nerissa Nevius", :sex :female}])

(def EMAILS
  ["gmail.com" "yahoo.com" "techtrix.net"
   "hackerz.ru" "spammer.guv" "foo.org" "outlook.com"])

(defn make-users
  []
  (mapv
    (fn [{:keys [name sex] :as person}]
      (let [[f-name l-name] (str/split name #" ")
            email (.toLowerCase
                    (format "%s.%s@%s" f-name l-name (rand-nth EMAILS)))]
        {:user/email email
         :user/age (+ 18 (rand-int 50))
         :user/first-name f-name
         :user/last-name l-name}))
    NAMES))

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

(def test-users (concat [alice bob] (make-users)))
