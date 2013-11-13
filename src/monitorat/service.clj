(ns monitorat.service
  (:gen-class)
  (:require [monitorat
             [tsd :as tsd]
             [restful :as restful]
             [metric :as metric]]
            [clojure-ini.core :as ini]
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
  (POST "/tsds" [:as r] (tsd/handler r))
  (GET "/metric" [:as r] (metric/handler r))
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
             ["-c" "--config" "the config file"]
             )]
    
    (when-not (:config opts)
      (log/error "config option is necessary")
      (System/exit 1))

    (swap! configs merge (ini/read-ini (:config opts) :keywordize? true :comment-char \#))

    (metric/set-mongodb!)
    (nrepl/start-server :port 5555)
    (jetty/run-jetty ( app) {:port (Integer. (:port @configs))} )))
