(ns think.query.test-data
  (:require [clojure.string :as str]))

(defn uuid [] (str (java.util.UUID/randomUUID)))

(def NAMES
  [{:name "Aleisha Alverson", :user/sex :female}
   {:name "Jaymie Jobin", :user/sex :female}
   {:name "Randal Richert", :user/sex :male}
   {:name "Shizuko Sharper", :user/sex :female}
   {:name "Lin Linck", :user/sex :female}
   {:name "Hana Hayashida", :user/sex :female}
   {:name "Deloris Duenes", :user/sex :female}
   {:name "Genny Groman", :user/sex :female}
   {:name "Erin Enger", :user/sex :female}
   {:name "Yolande Yen", :user/sex :female}
   {:name "Lloyd Landwehr", :user/sex :female}
   {:name "Juanita Joyal", :user/sex :female}
   {:name "Aura Abel", :user/sex :female}
   {:name "Thomas Tempel", :user/sex :male}
   {:name "Dirk Dunavant", :user/sex :male}
   {:name "Cristie Cork", :user/sex :female}
   {:name "Hildred Harstad", :user/sex :female}
   {:name "Dave Deming", :user/sex :male}
   {:name "Lael Low", :user/sex :male}
   {:name "Julissa Johansson", :user/sex :female}
   {:name "Oleta Orlandi", :user/sex :female}
   {:name "Soon Schuster", :user/sex :male}
   {:name "Monique Montesinos", :user/sex :female}
   {:name "Lawrence Lugo", :user/sex :male}
   {:name "Diedre Doughtie", :user/sex :female}
   {:name "Silvia Schieber", :user/sex :female}
   {:name "Laurena Lipari", :user/sex :female}
   {:name "Latosha Laurin", :user/sex :female}
   {:name "Erica Eley", :user/sex :female}
   {:name "Garnett Gabor", :user/sex :male}
   {:name "Thuy Tyer", :user/sex :female}
   {:name "Isaura Irion", :user/sex :female}
   {:name "Migdalia Milani", :user/sex :female}
   {:name "Katherine Kerbs", :user/sex :female}
   {:name "Lauren Lanza", :user/sex :female}
   {:name "Tomas Tieman", :user/sex :male}
   {:name "Ramona Rockhill", :user/sex :female}
   {:name "Blossom Bulkley", :user/sex :female}
   {:name "Delila Daniell", :user/sex :female}
   {:name "Troy Tuch", :user/sex :male}
   {:name "Bee Bragg", :user/sex :female}
   {:name "Sherman Sarinana", :user/sex :male}
   {:name "Maryam Mcmonagle", :user/sex :female}
   {:name "Reed Rolon", :user/sex :male}
   {:name "Myron Malley", :user/sex :male}
   {:name "Crystle Cagney", :user/sex :female}
   {:name "Kathern Klingensmith", :user/sex :female}
   {:name "Chong Chouinard", :user/sex :male}
   {:name "Marlon Matis", :user/sex :male}
   {:name "Nerissa Nevius", :user/sex :female}])

(def EMAILS
  ["gmail.com" "yahoo.com" "techtrix.net"
   "hackerz.ru" "spammer.guv" "foo.org" "outlook.com"])

(defn make-users
  []
  (into []
        (map-indexed
          (fn [i {:keys [name user/sex] :as person}]
            (let [[f-name l-name] (str/split name #" ")
                  email (.toLowerCase
                          (format "%s.%s@%s" f-name l-name (rand-nth EMAILS)))]
              {:resource/id (uuid)
               :user/sex sex
               :user/email email
               :user/age (+ 18 i)
               :user/first-name f-name
               :user/last-name l-name}))
          NAMES)))

(def alice
  {:resource/id (uuid)
   :user/sex :female
   :user/email "alice@foo.com"
   :user/age 23
   :user/first-name "Alice"
   :user/last-name "Foo"})

(def bob
  {:resource/id (uuid)
   :user/sex :male
   :user/email "bob@foo.com"
   :user/age 27
   :user/first-name "Bob"
   :user/friends ["Alice"]
   :user/last-name "Foo"})

(def test-users (concat [alice bob] (make-users)))

