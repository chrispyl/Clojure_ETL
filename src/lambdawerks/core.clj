(ns lambdawerks.core
	(:require [lambdawerks.xml-handling :refer [read-xml-chunk xml-record-traversal]]
			 [lambdawerks.db-handling :refer [multi-select multi-insert multi-update offsets-for-select str-to-date]]
			 [lambdawerks.load-statistics :refer [memory-usage cpu-usage]]
			 [clj-time.coerce :as c])
  (:gen-class))

;maps is a set containing maps
(defn maps-transformation [maps] 
	(zipmap (map #(dissoc % :phone) maps) (map #(% :phone) maps)))  		
		
(defn crosscheck-records [db-records xml-record-holder update-repo]

	(println "intersection " (count (clojure.set/intersection @xml-record-holder db-records)))
	(println "differencies " (count (clojure.set/difference @xml-record-holder db-records)))
	
	(let [xml-maps (maps-transformation @xml-record-holder)
		 db-maps (maps-transformation db-records)
		 wrong-phones (filter #(if-let [db-phone (db-maps (first %))] (not= db-phone (second %)) false) xml-maps)] ;wrong phones, so they go to update

		(dorun 
			(map #(swap! update-repo conj (assoc (first %) :phone (second %))) wrong-phones))		 
		(apply swap! xml-record-holder disj (apply conj 
											(mapv #(assoc (first %) :phone (second %)) wrong-phones) 
											(clojure.set/intersection @xml-record-holder db-records)))))	

(defn count-db-traversals [records-in-xml xml-batch-size]
	(Math/ceil (/ records-in-xml xml-batch-size)))		

(defn format-dates [db-records]
	(mapv #(update % :dob (fn [old-val] (if old-val (.toString old-val) nil))) db-records))				

(defn extract-xml [iteration xml-batch-size]
	 (->>(read-xml-chunk 
			xml-batch-size
			(* iteration xml-batch-size))
		(mapv #(xml-record-traversal %))))
				
(defn check-repos [insert-repo update-repo repo-limit]
	(dorun
		(map #(when (>= (count @%) repo-limit) 
					(%2 @%) 
					(reset! % [])) 
			 [insert-repo update-repo]
			 [multi-insert multi-update])))				

(defn empty-xml-record-holder [xml-record-holder]
	(reset! xml-record-holder #{}))			 
			 
(defn fill-xml-record-holder [xml-records xml-record-holder]
	(apply swap! xml-record-holder conj xml-records))			 
			 
(defn archive-missing-records [xml-record-holder insert-repo]
	(apply swap! insert-repo conj @xml-record-holder))
			 
(defn update-db []
	(let [records-in-db 10000000
		 records-in-xml 1500000
		 select-size 400000
		 xml-batch-size 100000
		 repo-limit 1000
		 db-traversals (count-db-traversals records-in-xml xml-batch-size)
		 db-offsets (offsets-for-select records-in-db select-size)
		 insert-repo (atom [])
		 update-repo (atom [])
		 xml-record-holder (atom #{})]
		(dotimes [iteration db-traversals]			
				(archive-missing-records xml-record-holder insert-repo) ;insert stuff after a db complete traversal				
				(println "insert-repo: " (count @insert-repo))
				(empty-xml-record-holder xml-record-holder) ;from the previous full db traversal				
				(-> iteration
					(extract-xml xml-batch-size)
					(fill-xml-record-holder xml-record-holder)) 				
				(check-repos insert-repo update-repo repo-limit) ;check if repos have >1000 and empty them if true				
				(doseq [db-offset db-offsets]
					(-> db-offset
					  (multi-select select-size)
					  (format-dates)
					  (set)
					  (crosscheck-records xml-record-holder update-repo))
					(println "offset: " db-offset ", update-repo: " (count @update-repo) ", xml-holder: " (count @xml-record-holder))))
		(archive-missing-records xml-record-holder insert-repo) ;after the last db traversal we have to insert the missing records in the repo
		(multi-insert @insert-repo)
		(multi-update @update-repo)))
  
  
(defn -main [& args]
  )
