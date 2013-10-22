(ns monitorat.restful
  (:require [compojure.handler]
            [cheshire.core :as json]
            [ring.util.response :as ring-resp]
            [clojure.java.io :as io])
  (:use [compojure.core])
  (:import [java.util.zip GZIPInputStream]
           [java.io ByteArrayOutputStream ByteArrayInputStream])
  )

(def CONTENT-TYPE "Content-Type")
(def JSON-HEADER "application/json") 
(def OK 200)

(defn wrap-gzip-request [handler]
  (fn [req]
    (let [content-encoding (get-in req [:headers "content-encoding"])]
      (if (and content-encoding (= content-encoding "gzip"))
        (handler (update-in req [:body] #(GZIPInputStream. %)))
        (handler req)
        ))))

(defn bad-request [code, description]
  (-> 
   (ring-resp/response (json/generate-string {:error-code code, :error-message description }))
   (ring-resp/status 400)
   (ring-resp/header CONTENT-TYPE JSON-HEADER)
   ))

(defn internal-error []
  (->
   (ring-resp/response (json/generate-string {:error-code "internal-error" :error-message "there are some internal occuried in service side"}))
   (ring-resp/status 500)
   (ring-resp/header CONTENT-TYPE JSON-HEADER)))

(defn success []
  (->
   (ring-resp/response "")
   (ring-resp/status 204)))

(defn json-response
  ([json] (json-response json OK))
  ([json, status]
     (-> (ring-resp/response (json/generate-string json))
         (ring-resp/status (Integer. status))
         (ring-resp/header CONTENT-TYPE JSON-HEADER))))
  

(defn json-api
    "a middleware of json API."
    [handler]
    (fn [request]
      (if-let [content-type (get-in request [:headers "content-type"])]
        (when (= content-type "application/json")
          (let [response (handler (assoc-in request :json-body (json/parse-stream (io/reader (:body request)) true)))]
            (if (map? response)
              nil nil)
            ))
        )))
