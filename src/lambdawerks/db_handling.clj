(ns lambdawerks.db-handling
	(:require [korma.db :as kdb]
			 [korma.core :as kcore]
			 [clj-time.coerce :as c]))
	
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

(defn str-to-date [s]
	(c/to-sql-date s))

(defn example-select []
	(kcore/select person
		(kcore/limit 5)))

(defn example-select-2 []		
	(kcore/select person
		(kcore/where (and {:fname "TA'KYA"}
						{:lname "RIVERA COTTO"}
						{:dob (str-to-date "1912-05-04")}))))

;(kcore/insert person
;	(kcore/values [{:fname "chris" :lname "pylianidis" :dob (str-to-date "1990-01-01") :phone "6940686949"}
;				 {:fname "theo" :lname "pylianidis" :dob (str-to-date "1990-01-01") :phone "69508090"}]))

;(kcore/delete person
;  (kcore/where 
;		(or {:fname [= "chris"]}
;			{:fname [= "theo"]}))) 
				 
;(kcore/update person
;  (kcore/set-fields {:phone "69508091"})
;  (kcore/where {:fname [= "theo"]}))	