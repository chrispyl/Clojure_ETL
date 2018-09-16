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
            :ssl false})	

(kdb/defdb korma-db db-spec)			
(kcore/defentity person)

(defn str-to-date 
	"Takes a string representing a date YYYY-MM-DD. Returns a java.sql.Date instance."
	[s]
	(c/to-sql-date s))

(defn offsets-for-select 
	"Takes two long numbers representing the number of records in the persons table
	and how many records we are going to select each time. Returns a lazy sequence 
	containing the offsets from which we will select records from the person table."
	[records-in-db select-size]
	(range 0 records-in-db select-size))	

(defn add-person-ids 
	"Adds an auto-incrementing id collumn to the person table as a primary key."
	[]
	(kcore/exec-raw ["ALTER TABLE person ADD COLUMN id SERIAL PRIMARY KEY;"]))

(defn drop-person-ids []
	"Drops the id collumn of the person table."
	(kcore/exec-raw ["ALTER TABLE person DROP COLUMN id;"]))	
	
(defn multi-select 
	"Takes two long numbers representing an offset from which a select will retrieve records
	and the number of records. Performs a select query and returns the result as a sequence."
	[offset amount]
	(kcore/select person
		(kcore/fields :fname :lname :dob :phone)
		(kcore/order :id)
		(kcore/limit amount)
		(kcore/offset offset)))	
	
(defn multi-insert
	"Takes a collection of update file records and maybe a Korma entity representing a table.
	If no entity is given, it inserts the records to the person table. Otherwise, it inserts
	them to the table represented by the entity. The insertion is happening in batches of 5000
	because the connection to the db was being lost for higher amounts."
	([records]
		(let [insert-limit 5000]
			(println "inserting to person table")
			(doseq [record-partition (partition-all insert-limit records)]
				(kcore/insert person
					(kcore/values (mapv #(assoc % :dob (str-to-date (:dob %))) record-partition))))))
	([records entity]
		(let [insert-limit 5000]
			(doseq [record-partition (partition-all insert-limit records)]
				(kcore/insert entity
					(kcore/values (mapv #(assoc % :dob (str-to-date (:dob %))) record-partition)))))))	
			
					
(defn multi-update 
	"Takes a collection of update file records. It updates the records of the person table according
	to the records of the update file."
	[records]
	(println "updating person table")
	(kcore/exec-raw ["CREATE TABLE temp_person
					(
					  fname character varying,
					  lname character varying,
					  dob date,
					  phone character(10)
					);"])
	
	(kcore/defentity temp_person)
	(multi-insert records temp_person)		
	(kcore/exec-raw ["UPDATE person
					SET phone = temp_person.phone
					FROM temp_person
					WHERE person.fname = temp_person.fname
						AND person.lname = temp_person.lname
						AND person.dob = temp_person.dob;"])
	(kcore/exec-raw ["DROP TABLE temp_person;"]))			

		
			