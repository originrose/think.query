(defproject thinktopic/think.query "0.2.2-SNAPSHOT"
  :description "A query language for clojure."
  :url "http://github.com/thinktopic/think.query"
  :dependencies [[org.clojure/clojure "1.9.0-alpha14"]
                 [org.clojure/test.check "0.10.0-alpha2"]]

  :plugins [[s3-wagon-private "1.3.0"]]

  :profiles {:dev {:dependencies [[com.datomic/datomic-pro "0.9.5530"]
                                  [org.clojure/java.jdbc "0.7.1"]
                                  [com.h2database/h2 "1.4.196"]
                                  [honeysql "0.9.1"]]
                   :repositories {"my.datomic.com" {:url      "https://my.datomic.com/repo"
                                                    :username :env/DATOMIC_USERNAME
                                                    :password :env/DATOMIC_PASSWORD}}}
             :tools {:dependencies [[thinktopic/tools-db "0.1.13"]]}}

  :aliases {"db" ["with-profile" "tools" "run" "-m" "think.tools-db.main"]}

  :repositories {"snapshots" {:url "s3p://thinktopic.jars/snapshots/"
                              :no-auth true
                              :releases false}
                 "releases"  {:url "s3p://thinktopic.jars/releases/"
                              :no-auth true
                              :snapshots false
                              :sign-releases false}}

  :release-tasks [["vcs" "assert-committed"]
                  ["change" "version" "leiningen.release/bump-version" "release"]
                  ["vcs" "commit"]
                  ["vcs" "tag" "" "--no-sign"] ; disable signing
                  ["deploy"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["vcs" "commit"]
                  ["vcs" "push"]])
