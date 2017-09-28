(ns think.query.test-data
  (:require [clojure.string :as s]))

(defn uuid [] (java.util.UUID/randomUUID))

(def NAMES
  [{:name "Aleisha Alverson"     :user/sex :female}
   {:name "Aura Abel"            :user/sex :female}
   {:name "Bee Bragg"            :user/sex :female}
   {:name "Blossom Bulkley"      :user/sex :female}
   {:name "Chong Chouinard"      :user/sex :male}
   {:name "Cristie Cork"         :user/sex :female}
   {:name "Crystle Cagney"       :user/sex :female}
   {:name "Dave Deming"          :user/sex :male}
   {:name "Delila Daniell"       :user/sex :female}
   {:name "Deloris Duenes"       :user/sex :female}
   {:name "Diedre Doughtie"      :user/sex :female}
   {:name "Dirk Dunavant"        :user/sex :male}
   {:name "Erica Eley"           :user/sex :female}
   {:name "Erin Enger"           :user/sex :female}
   {:name "Garnett Gabor"        :user/sex :male}
   {:name "Genny Groman"         :user/sex :female}
   {:name "Hana Hayashida"       :user/sex :female}
   {:name "Hildred Harstad"      :user/sex :female}
   {:name "Isaura Irion"         :user/sex :female}
   {:name "Jaymie Jobin"         :user/sex :female}
   {:name "Juanita Joyal"        :user/sex :female}
   {:name "Julissa Johansson"    :user/sex :female}
   {:name "Katherine Kerbs"      :user/sex :female}
   {:name "Kathern Klingensmith" :user/sex :female}
   {:name "Lael Low"             :user/sex :male}
   {:name "Latosha Laurin"       :user/sex :female}
   {:name "Lauren Lanza"         :user/sex :female}
   {:name "Laurena Lipari"       :user/sex :female}
   {:name "Lawrence Lugo"        :user/sex :male}
   {:name "Lin Linck"            :user/sex :female}
   {:name "Lloyd Landwehr"       :user/sex :female}
   {:name "Marlon Matis"         :user/sex :male}
   {:name "Maryam Mcmonagle"     :user/sex :female}
   {:name "Migdalia Milani"      :user/sex :female}
   {:name "Monique Montesinos"   :user/sex :female}
   {:name "Myron Malley"         :user/sex :male}
   {:name "Nerissa Nevius"       :user/sex :female}
   {:name "Oleta Orlandi"        :user/sex :female}
   {:name "Ramona Rockhill"      :user/sex :female}
   {:name "Randal Richert"       :user/sex :male}
   {:name "Reed Rolon"           :user/sex :male}
   {:name "Sherman Sarinana"     :user/sex :male}
   {:name "Shizuko Sharper"      :user/sex :female}
   {:name "Silvia Schieber"      :user/sex :female}
   {:name "Soon Schuster"        :user/sex :male}
   {:name "Thomas Tempel"        :user/sex :male}
   {:name "Thuy Tyer"            :user/sex :female}
   {:name "Tomas Tieman"         :user/sex :male}
   {:name "Troy Tuch"            :user/sex :male}
   {:name "Yolande Yen"          :user/sex :female}])

(def EMAILS
  ["gmail.com" "yahoo.com" "techtrix.net"
   "hackerz.ru" "spammer.guv" "foo.org" "outlook.com"])

(defn make-users
  []
  (->> NAMES
       (map-indexed
        (fn [i {:keys [name user/sex]}]
          (let [[f-name l-name] (s/split name #" ")
                email (.toLowerCase
                       (format "%s.%s@%s" f-name l-name (rand-nth EMAILS)))]
            {:resource/type :resource.type/user
             :resource/id (uuid)
             :user/first-name f-name
             :user/last-name l-name
             :user/sex sex
             :user/email email
             :user/age (+ 18 i)
             :user/created (java.util.Date.)})))
       (into [])))

(def alice
  {:resource/type :resource.type/user
   :resource/id (uuid)
   :user/first-name "Alice"
   :user/last-name "Foo"
   :user/sex :female
   :user/email "alice@foo.com"
   :user/age 23
   :user/created #inst "2017-09-12T18:24:09.556-00:00"})

(def bob
  {:resource/type :resource.type/user
   :resource/id (uuid)
   :user/first-name "Bob"
   :user/last-name "Foo"
   :user/sex :male
   :user/email "bob@foo.com"
   :user/age 27
   :user/friends ["Alice"]
   :user/created #inst "2017-09-12T18:24:10.556-00:00"})

(def test-users (concat [alice bob] (make-users)))
