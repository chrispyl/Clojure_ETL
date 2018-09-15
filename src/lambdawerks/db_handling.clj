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
	
(defn multi-insert 
	([records]
		(println "inserting to db")
		(doseq [record-partition (partition-all 5000 records)]
			(kcore/insert person
				(kcore/values (mapv #(assoc % :dob (str-to-date (% :dob))) record-partition)))))
	([records entity]
		(doseq [record-partition (partition-all 5000 records)]
			(kcore/insert entity
				(kcore/values (mapv #(assoc % :dob (str-to-date (% :dob))) record-partition))))))	

(defn multi-update [records]
	(println "updating db")
	(j/execute! db-spec
		(j/create-table-ddl :temp_person
							 [[:fname "character varying"]
							  [:lname "character varying"]
							  [:dob "date"]
							  [:phone "character(10)"]]))
	
	(kcore/defentity temp_person)
	
	(multi-insert records temp_person)
			
	(kcore/exec-raw ["UPDATE person
					SET phone = temp_person.phone
					FROM temp_person
					WHERE person.fname = temp_person.fname
						AND person.lname = temp_person.lname
						AND person.dob = temp_person.dob;"])
	
	(j/execute! db-spec
		(j/drop-table-ddl :temp_person)))			
			
	
(defn multi-select [offset amount]
	(kcore/select person
		(kcore/limit amount)
		(kcore/offset offset)))

;example result for 400000 (1 400001 800001 1200001 1600001 2000001 2400001 2800001 3200001 3600001 4000001 4400001 4800001 5200001 5600001 6000001 6400001 6800001 7200001 7600001 8000001 8400001 8800001 9200001 9600001 10000001)		
(defn offsets-for-select [records-in-db select-size]
	(range 1 records-in-db select-size))		
			