(defproject thinktopic/think.query "0.1.1-SNAPSHOT"
  :description "A query language for clojure."
  :url "http://github.com/thinktopic/think.query"
  :dependencies [[org.clojure/clojure "1.8.0"]]

  :profiles {:release {:plugins [[s3-wagon-private "1.3.0"]]
                       :repositories {"snapshots"  {:url "s3p://thinktopic.jars/snapshots/"
                                                    :no-auth true
                                                    :releases false}
                                      "releases"  {:url "s3p://thinktopic.jars/releases/"
                                                   :no-auth true
                                                   :snapshots false
                                                   :sign-releases false}}}
             :test {:dependencies [[com.datomic/datomic-pro "0.9.5530"]]
                    :repositories  {"my.datomic.com" {:url      "https://my.datomic.com/repo"
                                                      :username :env/DATOMIC_USERNAME
                                                      :password :env/DATOMIC_PASSWORD}}}}

  :release-tasks [["vcs" "assert-committed"]
                  ["change" "version" "leiningen.release/bump-version" "release"]
                  ["vcs" "commit"]
                  ["vcs" "tag" "" "--no-sign"] ; disable signing
                  ["deploy"]
                  ["change" "version" "leiningen.release/bump-version"]
                  ["vcs" "commit"]
                  ["vcs" "push"]])
