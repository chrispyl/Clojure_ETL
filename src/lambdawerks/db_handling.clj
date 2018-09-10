(ns lambdawerks.db-handling
	(:require [korma.db :as kdb]
			 [korma.core :as kcore]))
	
(def db-spec {:dbtype "postgresql"
			:subprotocol "postgresql"
			:dbname "lambdawerks"
			:host "localhost"
            :user "postgres"
            :password (slurp "C:/Users/Christos/Desktop/password.txt")
            :ssl false
            :sslfactory "org.postgresql.ssl.NonValidatingFactory"})	

(kdb/defdb korma-db db-spec)			

(kcore/defentity person)

(defn example-select []
	(kcore/select person
		(kcore/limit 5)))

	