(ns monitorat.metric-service
  (:gen-class)
  (:require [compojure.route :as route]
            [compojure.handler :as handler]
            [cheshire.core :as json]
            [clojure.java.io :as io]
            [ring.util.response :as ring-resp]
            [taoensso.timbre :as log]
            [ring.adapter.jetty :as jetty]
            [monitorat.hdfs :as hdfs]
            [monitorat.restful :as restful]
            [clojure.tools.nrepl.server :as nrepl]
            [clojure.string :as str]
            [clj-chunk-buffer.core :as chunk-buffer]
            )
  (:use [compojure.core]
        [clojure.tools.cli :only [cli]])
  (:import [java.util.concurrent BlockingQueue LinkedBlockingQueue]
           [java.io ByteArrayOutputStream]
           [java.security MessageDigest]
           [java.util.concurrent Executors TimeUnit Callable]
           [com.fasterxml.jackson.core JsonParseException]
           [org.joda.time DateTime DateTimeZone]
           [org.joda.time.format DateTimeFormat ISODateTimeFormat]
           [java.util UUID]
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
  (let [path (str root "/" ) 
        file-name (str (:key chunk) "-part-" (chunk-unique-id chunk))]
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

;;; REST API ;;;
(defn- validate-tsd [tsd]
  true)

(defn- format-minute [tsd]
  (let [datetime (->
                  (ISODateTimeFormat/dateTimeParser)
                  (.parseDateTime  (:timestamp tsd))
                  (.withZone (DateTimeZone/forID "Asia/Shanghai"))
                  )]
    (.print (DateTimeFormat/forPattern "yyyy-MM-dd-HH-mm") datetime)))

(defn- receive-tsds [buffer tsd-seq]
  (doseq [tsd tsd-seq]
    (try
      (if (validate-tsd tsd)
        (when-not (chunk-buffer/write buffer
                                    (format-minute tsd)
                                    (json/generate-string tsd))
          (log/warn "discart TSD item"))
        (log/warn "discart illegal TSD:" tsd))
      (catch Exception e (log/error e))))
  true)

(defn- mk-app [buffer]
  (let [app-routes (routes
          (POST "/spy/tsds" {body :body}
                (try
                  (if-let [tsd-seq (json/parse-stream (io/reader body) true)]
                    (do (receive-tsds buffer tsd-seq)
                        (restful/success))
                    (restful/internal-error))
                  (catch JsonParseException e (restful/bad-request "bad-json-format" "content maybe is not illegal json format"))))
          )]
    (->
     (handler/api app-routes)
     (restful/wrap-gzip-request)
     )))


;;; main ;;;
(defn- parse-size [size-str]
  (cond
   (or (.endsWith size-str "M") (.endsWith size-str "m")) (* 1024 1024 (Integer. (.substring size-str 0 (dec (count size-str)))))
   (or (.endsWith size-str "K") (.endsWith size-str "k")) (* 1024 (Integer. (.substring size-str 0 (dec (count size-str)))))
   :else (Integer. size-str)
   ))

(defn- parse-hdfs-option [hdfs]
  (let [[host port] (str/split hdfs #":")]
    [host (Integer. port)]))

(defn -main [& args]
  (let [[opts _ help]
        (cli args
             ["-p " "--port" "Listen on this port" :parse-fn #(Integer. %) :default 9999]
             ["--thread-num" "Worker thread number" :parse-fn #(Integer. %) :default 5]
             ["--chunk-size" "buffer's chunk size limit" :parse-fn parse-size :default 10485760]
             ["--chunk-age" "buffer's chunk age limit" :parse-fn #(Integer. %) :default 60]
             ["--queue-limit" "buffer's queue size limit" :parse-fn #(Integer. %) :default 5]
             ["--root" "the root directory of file system"]
             ["--hdfs" "the hdfs url <host:port>" :parse-fn parse-hdfs-option]
             )]
    
    (when-not (:root opts)
      (log/error "root option is necessary")
      (System/exit 1))

    (let [buffer (chunk-buffer/mk-chunk-buffer {:worker-num (:thread-num opts)
                                                :chunk-size (:chunk-size opts)
                                                :chunk-age (:chunk-age opts)
                                                :queue-limit (:queue-limit opts)
                                                :worker-fn #(if (:hdfs opts)
                                                              (write-chunk-to-hdfs % (zipmap [:host, :port] (:hdfs opts)) (:root opts))
                                                              (write-chunk-to-fs % (:root opts))
                                                              )})]
      (nrepl/start-server :port 5555)
      (jetty/run-jetty (mk-app buffer) {:port (:port opts)} )
      )))
