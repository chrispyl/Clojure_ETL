(ns lambdawerks.core
	(:require [lambdawerks.xml-handling :refer [read-xml-chunk xml-record-traversal]]
			 [lambdawerks.db-handling :refer [multi-select multi-insert multi-update offsets-for-select]]
			 [lambdawerks.utilities :refer [get-cores]]
			 [lambdawerks.load-statistics :refer [memory-usage cpu-usage]])
  (:gen-class))


(defn crosscheck-records [db-records xml-records-partition update-repo]
	(doseq [xml-record xml-records-partition
		   db-record db-records]
		(let [map-record (xml-record-traversal xml-record)]
			(when
				(every? true? (filter #(= (map-record %) (db-record %)) [:fname :lname :dob]))
				(when-not (= (map-record :phone) (db-record :phone)) (swap! update-repo (conj @update-repo map-record)))
					;but how i find when it must go to insert repository					
			))))
		
(defn distribute-work [db-records xml-records update-repo]
	(doall
		(map #(future (crosscheck-records % %2 %3)) 
			(repeat db-records) 
			xml-records
			update-repo)))	

(defn count-db-traversals [records-in-xml xml-batch-size]
	(Math/ceil (->> xml-batch-size
				(* (get-cores))
				(/ records-in-xml))))		

(defn check-repos [insert-repo update-repo repo-limit]
	(mapv #(when (>= @% repo-limit) (do (%2 @%) (reset! % []))) 
		 [insert-repo multi-insert]
		 [update-repo multi-update]))				
				
(defn update-db []
	(let [records-in-db 10000000
		 records-in-xml 1500000
		 select-size 400000
		 xml-batch-size 25000
		 repo-limit 1000
		 db-traversals (count-db-traversals records-in-xml xml-batch-size)
		 db-offsets (offsets-for-select records-in-db select-size)
		 insert-repo (atom [])
		 update-repo (atom [])]
		(dotimes [iteration db-traversals]
			(let [xml-records (read-xml-chunk 
								(* xml-batch-size (get-cores)) 
								(* iteration (* xml-batch-size (get-cores))))]
				(check-repos insert-repo update-repo repo-limit)				
				(doseq [db-offset db-offsets]
					(let [db-records (multi-select select-size db-offset)
						 futures (distribute-work db-records xml-records update-repo)]
						 (dorun
							(map deref futures))))))))
  
  
(defn -main [& args]
  )
