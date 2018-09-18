(defproject lambdawerks "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
				[org.clojure/data.xml "0.0.8"]
				[org.postgresql/postgresql "42.2.5.jre7"]
				[korma "0.4.3"]
				[clj-time "0.14.4"]
				[org.clojure/java.jdbc "0.7.8"]
				[org.clojure/core.async "0.4.474"]]
  :main ^:skip-aot lambdawerks.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
