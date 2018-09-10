(ns lambdawerks.core
	(:require [lambdawerks.xml-handling :refer [read-xml]]
			 [lambdawerks.db-handling :refer [example-select example-select-2]]
			 [korma.core :as kcore]
			 [lambdawerks.load-statistics :refer [memory-usage cpu-usage]])
  (:gen-class))

  
  
(defn -main [& args]
  )
