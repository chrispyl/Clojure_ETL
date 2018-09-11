(ns lambdawerks.db-handling
	(:require [korma.db :as kdb]
			 [korma.core :as kcore]
			 [clj-time.coerce :as c]
			 [clojure.java.jdbc :as j]))
	
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

(defn multi-insert [records]
	(kcore/insert person
		(kcore/values (mapv #(assoc % :dob (str-to-date (% :dob))) records))))	

(defn multi-update [records]
	(let [sql-string "UPDATE person SET phone = ? WHERE fname = ? AND lname = ? AND dob = ?"
		 vectors (map #(vector (% :phone) (% :fname) (% :lname) (% :dob)) records)
		 final-vector (reduce #(conj % %2) [sql-string] vectors)]
		(j/db-do-prepared db-spec final-vector {:multi? true})))	
	
(defn multi-select [amount offset]
	(kcore/select person
		(kcore/limit amount)
		(kcore/offset offset)))

;example result for 400000 (1 400001 800001 1200001 1600001 2000001 2400001 2800001 3200001 3600001 4000001 4400001 4800001 5200001 5600001 6000001 6400001 6800001 7200001 7600001 8000001 8400001 8800001 9200001 9600001 10000001)		
(defn offsets-for-select [records-in-db select-size]
	(range 1 records-in-db select-size))		
		
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