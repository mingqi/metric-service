(ns monitorat.hdfs
  (:import [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.fs FSDataOutputStream FileSystem Path])
  )

(defrecord HDFS [host port])

(defn write [hdfs, path, write-fn]
  (let [fs (FileSystem/get
            (doto (Configuration.)
              (.set "fs.default.name" (format "hdfs://%s:%d/" (:host hdfs) (:port hdfs)))
              ))]
    (with-open [data-out (.create fs (Path. path))]
      (write-fn data-out)
      )))
