(defproject metric-service "0.1.0-SNAPSHOT"
  :url "http://monitorat.com"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [ring/ring-core "1.2.0"]
                 [ring/ring-jetty-adapter "1.2.0"]
                 [compojure "1.1.5"]
                 [cheshire "5.2.0"]
                 [com.taoensso/timbre "2.6.1"]
                 [clj-chunk-buffer "0.1.0"]
                 [org.apache.hadoop/hadoop-core "0.20.2"]
                 [org.clojure/tools.cli "0.2.4"]
                 [org.clojure/tools.nrepl "0.2.3"]
                 ]
  :main monitorat.metric-service)
