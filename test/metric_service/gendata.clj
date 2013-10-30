(ns metric-service.gendata
  (:require [clj-http.client :as client]
            [cheshire.core :as json]
            [clj-time.core :as time]
            [clj-time.format :as format]
            [clj-time.periodic :as periodic])
  (:use [clojure.tools.cli :only [cli]])
  )

(def DEFAULT-TZ (time/time-zone-for-id "Asia/Shanghai"))

(defn- format-iso [t]
  (format/unparse
   (format/formatter "yyyy-MM-dd'T'HH:mm:ssZ" DEFAULT-TZ) t
   ))

(defn send-tsd [tsd]
  (= 200 (:status (client/post "http://127.0.0.1:9999/tsds"
                               {:body (json/generate-string [tsd]) 
                                }))))


(defn -main [& args]
  (let [[opts _ _] (cli args
                ["-s" "--start"]
                ["-e" "--end"]
                ["--num" :parse-fn #(Integer. %)])
        fmt (:date-hour-minute format/formatters)]
    (loop [minutes-seq (periodic/periodic-seq (format/parse fmt (:start opts)) (time/minutes 1))]
      (if-let [m (first minutes-seq)]
        (when (time/before? m (format/parse fmt (:end opts)))
          (dotimes [_ (:num opts)]
            (send-tsd {:metric-name "sys.cpu.usage"
                       :value (rand-int 100)
                       :timestamp (format-iso m)}))
          (recur (next minutes-seq))
          )))))
