(ns monitorat.metric-service
  (:gen-class)
  (:require [monitorat.tsd :as tsd]
            [clojure-ini.core :as ini]
            [monitorat.restful :as restful]
            [compojure.handler :as handler]
            [taoensso.timbre :as log]
            [clojure.tools.nrepl.server :as nrepl]
            [ring.adapter.jetty :as jetty]
            )
  (:use [compojure.core]
        [clojure.tools.cli :only [cli]]
        [monitorat.config :only [configs]])
  )

;;; REST API ;;;
(defn auth-middleware [handler]
  (fn [req]
    (handler (assoc req :user-id "user001"))
    ))


(defroutes app-routes
  (POST "/tsds" [:as r] tsd/receive)
  )

(defn app []
  (->
   (handler/api app-routes)
   (restful/wrap-gzip-request)
   (auth-middleware)))

;;; main ;;;
(defn -main [& args]
  (let [[opts _ help]
        (cli args
             ["-c " "--config" "the config file"]
             )]
    
    (when-not (:config opts)
      (log/error "config option is necessary")
      (System/exit 1))

    (swap! configs merge (ini/read-ini (:config opts) :keywordize? true :comment-char \#))

    (nrepl/start-server :port 5555)
    (jetty/run-jetty ( app) {:port (Integer. (:port @configs))} )))
