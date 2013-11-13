(ns monitorat.tsd
  (:require [compojure.route :as route]
            [compojure.handler :as handler]
            [cheshire.core :as json]
            [clojure.java.io :as io]
            [ring.util.response :as ring-resp]
            [taoensso.timbre :as log]
            [monitorat.hdfs :as hdfs]
            [monitorat.restful :as restful]
            [clojure.string :as str]
            [clj-chunk-buffer.core :as chunk-buffer]
            [clj-time.core :as time]
            [clj-time.format :as time-format]
            )

  (:use [compojure.core]
        [monitorat.config :only [configs]])
  (:import [java.util.concurrent BlockingQueue LinkedBlockingQueue]
           [java.io ByteArrayOutputStream]
           [java.security MessageDigest]
           [java.util.concurrent Executors TimeUnit Callable]
           [com.fasterxml.jackson.core JsonParseException]
           [org.joda.time DateTime DateTimeZone]
           [org.joda.time.format DateTimeFormat ISODateTimeFormat]
           [java.util UUID]
           )
  )

(def DEFAULT-TZ (time/time-zone-for-id "Asia/Shanghai"))

(defn- parse-size [size-str]
  (cond
   (or (.endsWith size-str "M") (.endsWith size-str "m")) (* 1024 1024 (Integer. (.substring size-str 0 (dec (count size-str)))))
   (or (.endsWith size-str "K") (.endsWith size-str "k")) (* 1024 (Integer. (.substring size-str 0 (dec (count size-str)))))
   :else (Integer. size-str)
   ))

;;; buffer output ;;;
(extend-type String
  chunk-buffer/ChunkData
  (size [this] (count this)))

(defn- chunk-unique-id [chunk]
  (let [uuid (.toString (UUID/randomUUID))]
    (.substring uuid (- (count uuid) 5) (count uuid))))

(defn write-chunk-to-fs
  "write chunk to local file system"
  [chunk, root]
  (log/debug "writing chunk to file system for key" (:key chunk) ", chunk size is" (:size chunk))
  (let [path (str root "/" (str/replace (:key chunk) "-" "/")) 
        file-name (str (str/replace (:key chunk) "-" "") "-part-" (chunk-unique-id chunk))]
    (when (not (.exists (io/file path)))
      (.mkdirs (io/file path)))
    (with-open [o (io/output-stream (io/file path file-name))]
      (doseq [tsd-str, (:data-seq chunk)]
        (.write o (.getBytes (str tsd-str "\n"))))
      (.flush o)
      )))

(defn write-chunk-to-hdfs
  "write chunk to hadoop file system"
  [chunk, hdfs, root-dir]
  (log/debug "writing chunk to hdfs for key" (:key chunk) ", chunk size is" (:size chunk))
  (let [hdfs-file (str
                   root-dir
                   "/"
                   (str/replace (:key chunk) "-" "/")
                   "/"
                   (str/replace (:key chunk) "-" "")
                   "-part-" (chunk-unique-id chunk))
        ]
    (hdfs/write
     hdfs hdfs-file
     (fn [out]
       (doseq [tsd-str (:data-seq chunk)]
         (.write out (.getBytes (str tsd-str "\n")))))
     )))


(defn- parse-iso [t]
  (try
    (time-format/parse (:date-time-parser time-format/formatters) t)
    (catch Exception e
      )))

(defn- format-iso [t]
  (time-format/unparse
   (time-format/formatter "yyyy-MM-dd'T'HH:mm:ssZ" DEFAULT-TZ) t
   ))

(defn- format-minute [t]
  (time-format/unparse (time-format/formatter "yyyy-MM-dd-HH-mm"
                                              (time/time-zone-for-id "Asia/Shanghai"))
                       (parse-iso t)))


(defn- now-with-isoformat []
  (time-format/unparse
   (time-format/formatter "yyyy-MM-dd'T'HH:mm:ssZ" DEFAULT-TZ)
   (time/now)
   ))

(defn- parse-hdfs-option [hdfs]
  (let [[host port] (str/split hdfs #":")]
    [host (Integer. port)]))

(defn- validate-tsd [user-id tsd]
  (when (and (:metric-name tsd)
             (:value tsd)
             (number? (:value tsd))
             (or (not (:timestamp tsd)) (parse-iso (:timestamp tsd))))
    (-> (select-keys tsd [:metric-name, :timestamp, :value, :dimensions])
        (update-in [:timestamp] #(format-iso (if % (parse-iso %) (time/now))))
        (assoc :user-id  user-id)
        )))

(defn- receive-tsds [buffer user-id tsd-seq]
  (letfn [(reduce-fn [{:keys [succ, failures] :as result} tsd]
            (try
              (let [validated-tsd (validate-tsd user-id tsd)]
                (cond

                 ;;; validate tsd
                 (not validated-tsd)
                 (do
                   (log/warn "discard TSD because illegal format:" tsd)
                   (update-in result [:failure, :illegal-format] #(if % (inc %) 1)))

                 ;;; push to buffer
                 (not (chunk-buffer/write buffer
                                          (format-minute (:timestamp tsd))
                                          (json/generate-string validated-tsd)))
                  (do
                    (log/warn "discard TSD because failed push to buffer")
                    (update-in result [:failure, :service-internal-error] #(if % (inc %) 1)))

                  :else
                  (update-in result [:success] inc)
                 ))
              (catch Exception e
                (do
                  (log/warn e)
                  (update-in result [:failure, :service-internal-error] #(if % (inc %) 1))))
              ))]

    (reduce reduce-fn {:success 0, :failure {}} tsd-seq)))


(def get-buffer
  (memoize
   (fn []
     (chunk-buffer/mk-chunk-buffer
      (assoc {:worker-num (Integer. (:worker-num @configs))
              :chunk-size (parse-size (:chunk-size @configs))
              :chunk-age (Integer. (:chunk-age @configs))
              :queue-limit (Integer. (:queue-limit @configs))
              }
        :worker-fn
        #(if-let [hdfs (:hdfs @configs)]
           (write-chunk-to-hdfs %
                                (zipmap [:host, :port] (parse-hdfs-option hdfs))
                                (:root @configs))
           (write-chunk-to-fs % (:root @configs))))
      ))))


(defn handler[ request ]
  (let [{:keys [user-id body]} request
        buffer (get-buffer)]
           (try
             (if-let [tsd-seq (json/parse-stream (io/reader body) true)]
               (restful/json-response (receive-tsds buffer user-id tsd-seq))
               (restful/internal-error))
             (catch JsonParseException e (restful/bad-request "bad-json-format" "content maybe is not illegal json format")))
           ))
