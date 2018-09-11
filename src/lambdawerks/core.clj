(ns lambdawerks.core
	(:require [lambdawerks.xml-handling :refer [read-xml xml-record-batch-traversal]]
			 [lambdawerks.db-handling :refer [multi-select offsets-for-select]]
			 [lambdawerks.utilities :refer [get-cores]]
			 [lambdawerks.load-statistics :refer [memory-usage cpu-usage]])
  (:gen-class))


(def insert-repo (atom []))
(def update-repo (atom []))


(defn crosscheck-records [db-records xml-records]
	(let []
		))

(defn extract-xml-records [xml-batch-size]
	)
		
(defn distribute-work [db-records xml-batch-size]
	(doall
		(map #(future (crosscheck-records % %2)) 
			(repeat db-records) 
			(partition-all 4 (read-xml (* xml-batch-size (get-cores)))))))	

(defn count-db-traversals [records-in-xml xml-batch-size]
	(Math/ceil (->> xml-batch-size
				(* (get-cores))
				(/ records-in-xml))))		
		
(defn update-db []
	(let [records-in-db 10000000
		 records-in-xml 1500000
		 select-size 400000
		 xml-batch-size 25000
		 db-traversals (count-db-traversals records-in-xml xml-batch-size)
		 db-offsets (offsets-for-select records-in-db select-size)]
		
		(dotimes [iteration db-traversals]
			(doseq [db-offset db-offsets]
				(let [db-records (multi-select select-size db-offset)
					 futures (distribute-work db-records xml-batch-size)]
					 (doall
						(map deref futures)))))))




(defn f [n member-records]
	(loop [rs member-records elems []]
		(if (= (count elems) n)
			elems
			(recur (drop 1) (first member-records)))))

  
  
  
  
  
  
  
(defn -main [& args]
  )
