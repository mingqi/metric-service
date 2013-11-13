(ns monitorat.metric
  (:refer-clojure :exclude [sort find])
  (:require [monger.collection :as mgc]
            [monger.core :as mg]
            [monger.joda-time]
            [clj-time.core :as time]
            [cheshire.core :as json]
            [monitorat.restful :as restful])
  (:use [compojure.core :only [let-request]]
        [clj-time.coerce :only [from-long to-long]]
        [clj-time.core :only [date-time]]
        [monger.query :only [with-collection find sort]]))

(defn set-mongodb!
  ([] (set-mongodb! "127.0.0.1" 27017))
  ([host port]
     (mg/connect! {:host host, :port port})
     (mg/set-db! (mg/get-db "monitorat"))
     ))


(def test-r {:params {:name "sys.cpu.usage"
                      :interval "5m"
                      :aggregator "sum"
                      :start 1383264000
                      :end   1383267600
                      }
             :user-id "user001"})

(defn succ-pair-seq [time-seq interval default-value]
  (loop [result []
         expect (ffirst time-seq)
         s time-seq]
    (if-not (seq s)
      result
      (cond
       (= expect (ffirst s))
       (recur (conj result (first s)) (+ expect interval) (rest s))

       (< expect (ffirst s))
       (recur (conj result [expect default-value]) (+ expect interval) s)

       (> expect (ffirst s))
       (recur result (+ expect interval) (rest s))
       ))))

(defn handler [ r ]
  (let [user-id (:user-id r)]
    (let-request [[name start end aggregator interval & dimensions] r]
      (if-not (and name start end aggregator interval)
        (restful/bad-request "illegal-parameters" "necessary parameters is necessary")
        (let [start (from-long (* 1000 (Long. start)))
              end (from-long (* 1000 (Long. end)))
              dimensions (merge (sorted-map) dimensions)]
          (json/generate-string
           (map (fn [x] [(long (/ (to-long (:timepoint x)) 1000)) (:value x)])
                (with-collection "tsd_5m"
                  (find {:user-id user-id
                         :metric-name name
                         :interval interval
                         :aggregator aggregator
                         :timepoint {"$gte" start, "$lt" end}
                         :dimensions dimensions
                         })
                  (sort (array-map :timepoint 1))
                  ))))))))
